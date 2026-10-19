/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_transfer.h"
#include "vine_protocol.h"
#include "vine_worker.h"

#include "debug.h"
#include "host_disk_info.h"
#include "link.h"
#include "macros.h"
#include "path.h"
#include "stringtools.h"
#include "trash.h"
#include "unlink_recursive.h"
#include "url_encode.h"
#include "xxmalloc.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Temporary hacks to access global values. */

__attribute__((format(printf, 2, 3))) void send_message(struct link *l, const char *fmt, ...);

/*
This module implements the streaming directory transfer,
making it efficient to move large directory trees without
multiple round trips needed for remote procedure calls.

Each file, directory, or symlink is represented by a single
header line giving the name, length, and mode of the entry.
Files and symlinks are followed by the raw contents of the file
or link, respectively, while directories are followed by more
lines containing the contents of the directory, until an "end"
is received.

For example, the following directory tree:

- mydir
-- 1.txt
-- 2.txt
-- mysubdir
--- a.txt
--- b.txt
-- z.jpb

Is represented as follows:

dir mydir
file 1.txt 35291 0600
  (35291 bytes of 1.txt)
file 2.txt 502 0666
  (502 bytes of 2.txt)
dir mysubdir
file a.txt 321 0600
  (321 bytes of a.txt)
file b.txt 456 0600
  (456 bytes of a.txt)
end
file z.jpg 40001 0644
  (40001 bytes of z.jpg)
end

*/

/* A directory being sent, or one that contains the requested item, identified by device and inode, with the directory
 * that contains it. A link that leads to one of them is sent as a link instead of being followed, which would send the
 * tree again without end. */
struct sent_directory {
	dev_t device;
	ino_t inode;
	const struct sent_directory *parent;
};

/*
Send one item. Links are followed and sent as what they lead to, except a link to a directory being sent or to one
that contains the requested item, which is sent as a link. Inside a directory, a FIFO, a socket, or a device has no
data to send and is skipped. Any other item that cannot be read is sent as "error", which fails the transfer.
ancestors holds the directories around the item, the innermost first, and nested is zero for the requested item.
*/

static int vine_transfer_put_internal(struct link *lnk, const char *full_name, const char *relative_name, vine_transfer_mode_t xfer_mode, time_t stoptime, const struct sent_directory *ancestors, int nested)
{
	struct stat info;
	int64_t actual, length;
	int mode;

	/* URL encode filename to handle spaces and unprintable characters. */
	char relative_name_encoded[VINE_LINE_MAX];
	url_encode(relative_name, relative_name_encoded, sizeof(relative_name_encoded));

	/* Links are followed and sent as what they point to. A link that cannot be followed, such as one whose target
	 * does not exist, is sent as a link. */
	if (stat(full_name, &info) != 0 && lstat(full_name, &info) != 0) {
		goto access_failure;
	}

	if (S_ISDIR(info.st_mode)) {
		for (const struct sent_directory *a = ancestors; a; a = a->parent) {
			if (a->device == info.st_dev && a->inode == info.st_ino) {
				/* A link to a directory being sent or containing the item. A loop without a link, such as a bind
				 * mount, cannot be sent. */
				if (lstat(full_name, &info) != 0) {
					goto access_failure;
				}
				if (!S_ISLNK(info.st_mode)) {
					errno = ELOOP;
					goto access_failure;
				}
				break;
			}
		}
	}

	/* Send only the normal mode bits. */
	mode = info.st_mode & 0777;

	if (S_ISREG(info.st_mode)) {
		int fd = open(full_name, O_RDONLY, 0);
		if (fd >= 0) {
			length = info.st_size;
			send_message(lnk, "file %s %" PRId64 " 0%o %lld\n", relative_name_encoded, length, mode, (long long)info.st_mtime);
			actual = link_stream_from_fd(lnk, fd, length, stoptime);
			close(fd);
			if (actual != length)
				goto send_failure;
		} else {
			goto access_failure;
		}
	} else if (xfer_mode == VINE_TRANSFER_MODE_FILE_ONLY) {
		/*
		The caller only wants a file, but full_name is something else.
		Choose a suitable error number to return in the error message.
		*/
		if (S_ISDIR(info.st_mode)) {
			errno = EISDIR;
		} else {
			errno = EINVAL;
		}
		goto access_failure;
	} else if (S_ISDIR(info.st_mode)) {
		DIR *dir = opendir(full_name);
		if (!dir)
			goto access_failure;

		send_message(lnk, "dir %s %o %lld\n", relative_name_encoded, mode, (long long)info.st_mtime);

		struct sent_directory sent = {info.st_dev, info.st_ino, ancestors};
		struct dirent *dent;
		while ((dent = readdir(dir))) {
			if (!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, ".."))
				continue;
			char *sub_full_name = string_format("%s/%s", full_name, dent->d_name);
			int sub_result = vine_transfer_put_internal(lnk, sub_full_name, dent->d_name, xfer_mode, stoptime, &sent, 1);
			free(sub_full_name);

			// Bail out of transfer if we cannot send any more
			if (!sub_result)
				break;
		}
		closedir(dir);
		send_message(lnk, "end\n");

	} else if (S_ISLNK(info.st_mode)) {
		char link_target[VINE_LINE_MAX];
		int result = readlink(full_name, link_target, sizeof(link_target));
		if (result > 0) {
			send_message(lnk, "symlink %s %d\n", relative_name_encoded, result);
			link_write(lnk, link_target, result, stoptime);
		} else {
			goto access_failure;
		}
	} else if (nested) {
		debug(D_VINE, "not sending %s: a FIFO, socket, or device holds no data", full_name);
	} else {
		errno = EINVAL;
		goto access_failure;
	}

	return 1;

access_failure:
	// An error here is not a failure from our perspective, keep going.
	send_message(lnk, "error %s %d\n", relative_name_encoded, errno);
	return 1;

send_failure:
	debug(D_VINE, "Sending back output file - %s failed: bytes to send = %" PRId64 " and bytes actually sent = %" PRId64 ".", full_name, length, actual);
	return 0;
}

/*
Send a cached object of any type down the wire.
*/

int vine_transfer_put_any(struct link *lnk, struct vine_cache *cache, const char *filename, vine_transfer_mode_t xfer_mode, time_t stoptime)
{
	char *cached_path = vine_cache_data_path(cache, filename);
	int r;

	if (path_within_dir(cached_path, workspace->cache_dir)) {
		/* The directories that contain the item, from the root down. */
		struct sent_directory *above = NULL;
		char *prefix = xxstrdup(cached_path);
		for (char *slash = strchr(prefix, '/'); slash; slash = strchr(slash + 1, '/')) {
			char kept = slash[1];
			slash[1] = 0;
			struct stat info;
			if (stat(prefix, &info) == 0) {
				struct sent_directory *directory = xxmalloc(sizeof(*directory));
				*directory = (struct sent_directory){info.st_dev, info.st_ino, above};
				above = directory;
			}
			slash[1] = kept;
		}
		free(prefix);
		r = vine_transfer_put_internal(lnk, cached_path, path_basename(filename), xfer_mode, stoptime, above, 0);
		while (above) {
			struct sent_directory *parent = (struct sent_directory *)above->parent;
			free(above);
			above = parent;
		}
	} else {
		/* Same reply as an unreadable file in vine_transfer_put_internal. */
		debug(D_VINE, "refusing to send %s: not within %s", cached_path, workspace->cache_dir);
		char filename_encoded[VINE_LINE_MAX];
		url_encode(path_basename(filename), filename_encoded, sizeof(filename_encoded));
		send_message(lnk, "error %s %d\n", filename_encoded, EACCES);
		r = 1;
	}

	free(cached_path);
	return r;
}

int vine_transfer_request_any(struct link *lnk, const char *request_path, const char *dirname, int64_t *totalsize, int *mode, int *mtime, int timeout, char **error_message)
{
	send_message(lnk, "get %s\n", request_path);
	return vine_transfer_get_any(lnk, dirname, totalsize, mode, mtime, timeout, error_message);
}
