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
#include "path.h"
#include "stringtools.h"
#include "trash.h"
#include "unlink_recursive.h"
#include "url_encode.h"

#include <dirent.h>
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Temporary hacks to access global values. */

int recv_message(struct link *l, char *line, int length, time_t stoptime);

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

/* Bytes of headers that one call of put_walk_produce() queues at most. */
#define PUT_WALK_CHUNK (64 * 1024)

struct put_frame {
	DIR *dir;		  /* The open directory. */
	char *path;		  /* Its full path. */
	struct put_frame *parent; /* The directory that contains it, or NULL for the top of the walk. */
};

/* The walk of an item being sent, which queues its next entries each time it reaches the head of the queue. */
struct put_walk {
	char *full_name;	   /* Full path of the item. */
	char *relative_name;	   /* Name the item is sent under. */
	vine_transfer_mode_t mode; /* What the receiver accepts. */
	int started;		   /* The item itself was queued. */
	struct put_frame *top;	   /* The innermost directory being walked, or NULL. */
	size_t queued;		   /* Bytes of headers queued by the current call. */
};

/* Queue a header line and log it. */
__attribute__((format(printf, 3, 4))) static int put_line(struct link_queue *lq, struct put_walk *w, const char *fmt, ...)
{
	char line[2 * VINE_LINE_MAX];
	va_list va;
	va_start(va, fmt);
	int length = vsnprintf(line, sizeof(line), fmt, va);
	va_end(va);
	if (length < 0 || (size_t)length >= sizeof(line)) {
		return 0;
	}
	debug(D_VINE, "tx: %s", line);
	w->queued += length;
	return link_queue_push(lq, line, length);
}

/* Queue one item of a walk. A directory's entries follow in later calls. Return 2 for a file, 1 for any other item, or
 * zero if the queue refused it. */
static int put_item(struct link_queue *lq, struct put_walk *w, const char *full_name, const char *relative_name)
{
	struct stat info;

	/* URL encode filename to handle spaces and unprintable characters. */
	char relative_name_encoded[VINE_LINE_MAX];
	url_encode(relative_name, relative_name_encoded, sizeof(relative_name_encoded));

	if (stat(full_name, &info) != 0) {
		goto access_failure;
	}

	/* Send only the normal mode bits. */
	int mode = info.st_mode & 0777;

	if (S_ISREG(info.st_mode)) {
		int fd = open(full_name, O_RDONLY, 0);
		if (fd < 0) {
			goto access_failure;
		}
		int64_t length = info.st_size;
		if (!put_line(lq, w, "file %s %" PRId64 " 0%o %lld\n", relative_name_encoded, length, mode, (long long)info.st_mtime)) {
			close(fd);
			return 0;
		}
		return link_queue_push_fd(lq, fd, 0, length) ? 2 : 0;
	} else if (w->mode == VINE_TRANSFER_MODE_FILE_ONLY) {
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
		if (!dir) {
			goto access_failure;
		}
		struct put_frame *f = malloc(sizeof(*f));
		char *path = strdup(full_name);
		if (!f || !path) {
			free(f);
			free(path);
			closedir(dir);
			return 0;
		}
		f->dir = dir;
		f->path = path;
		f->parent = w->top;
		w->top = f;
		return put_line(lq, w, "dir %s %o %lld\n", relative_name_encoded, mode, (long long)info.st_mtime);
	} else if (S_ISLNK(info.st_mode)) {
		char link_target[VINE_LINE_MAX];
		int result = readlink(full_name, link_target, sizeof(link_target));
		if (result > 0) {
			return put_line(lq, w, "symlink %s %d\n", relative_name_encoded, result) && link_queue_push(lq, link_target, result);
		} else {
			goto access_failure;
		}
	}

access_failure:
	// An error here is not a failure from our perspective, keep going.
	return put_line(lq, w, "error %s %d\n", relative_name_encoded, errno);
}

static void put_walk_pop(struct put_walk *w)
{
	struct put_frame *f = w->top;
	w->top = f->parent;
	closedir(f->dir);
	free(f->path);
	free(f);
}

static void put_walk_delete(void *arg)
{
	struct put_walk *w = arg;
	while (w->top) {
		put_walk_pop(w);
	}
	free(w->full_name);
	free(w->relative_name);
	free(w);
}

/* Queue the next entries of a walk, up to a file or PUT_WALK_CHUNK bytes of headers. Return one while entries remain,
 * zero when the walk is done, or -1 if the queue refused an entry. */
static int put_walk_produce(struct link_queue *lq, void *arg)
{
	struct put_walk *w = arg;
	w->queued = 0;

	if (!w->started) {
		w->started = 1;
		if (!put_item(lq, w, w->full_name, w->relative_name)) {
			return -1;
		}
		return w->top ? 1 : 0;
	}

	while (w->top && w->queued < PUT_WALK_CHUNK) {
		struct dirent *dent = readdir(w->top->dir);
		if (!dent) {
			put_walk_pop(w);
			if (!put_line(lq, w, "end\n")) {
				return -1;
			}
			continue;
		}
		if (!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, "..")) {
			continue;
		}
		char *sub_full_name = string_format("%s/%s", w->top->path, dent->d_name);
		int result = put_item(lq, w, sub_full_name, dent->d_name);
		free(sub_full_name);
		if (!result) {
			return -1;
		}
		/* One open file at a time. */
		if (result == 2) {
			break;
		}
	}
	return w->top ? 1 : 0;
}

static int put_walk_start(struct link_queue *lq, const char *full_name, const char *relative_name, vine_transfer_mode_t mode)
{
	struct put_walk *w = calloc(1, sizeof(*w));
	if (!w) {
		return 0;
	}
	w->full_name = strdup(full_name);
	w->relative_name = strdup(relative_name);
	w->mode = mode;
	if (!w->full_name || !w->relative_name) {
		put_walk_delete(w);
		return 0;
	}
	return link_queue_push_producer(lq, put_walk_produce, put_walk_delete, w);
}

/*
Send a cached object of any type down the wire.
*/

int vine_transfer_put_any(struct link_queue *lq, struct vine_cache *cache, const char *filename, vine_transfer_mode_t xfer_mode)
{
	char *cached_path = vine_cache_data_path(cache, filename);
	int r;

	if (path_within_dir(cached_path, workspace->cache_dir)) {
		r = put_walk_start(lq, cached_path, path_basename(filename), xfer_mode);
	} else {
		/* Same reply as an unreadable file in put_item. */
		debug(D_VINE, "refusing to send %s: not within %s", cached_path, workspace->cache_dir);
		char filename_encoded[VINE_LINE_MAX];
		url_encode(path_basename(filename), filename_encoded, sizeof(filename_encoded));
		char *line = string_format("error %s %d\n", filename_encoded, EACCES);
		debug(D_VINE, "tx: %s", line);
		r = link_queue_push(lq, line, strlen(line));
		free(line);
	}

	free(cached_path);
	return r;
}

/*
Handle an incoming symbolic link inside the recursive protocol.
The filename of the symlink was already given in the message,
and the target of the symlink is given as the "body" which
must be read off of the wire.  The symlink target does not
need to be url_decoded because it is sent in the body.
*/

static int vine_transfer_get_symlink_internal(struct link *lnk, char *filename, int length, time_t stoptime)
{
	char *target = malloc(length);

	int actual = link_read(lnk, target, length, stoptime);
	if (actual != length) {
		free(target);
		return 0;
	}

	int result = symlink(target, filename);
	if (result < 0) {
		debug(D_VINE, "could not create symlink %s: %s", filename, strerror(errno));
		free(target);
		return 0;
	}

	free(target);

	return 1;
}

/*
Handle an incoming file inside the recursive protocol.
Notice that we trust the caller to have created
the necessary parent directories and checked the
name for validity.
*/

static int vine_transfer_get_file_internal(struct link *lnk, const char *filename, int64_t length, int mode, int mtime, time_t stoptime)
{
	if (!check_disk_space_for_filesize(".", length, 0)) {
		debug(D_VINE, "Could not put file %s, not enough disk space (%" PRId64 " bytes needed)\n", filename, length);
		return 0;
	}

	int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0700);
	if (fd < 0) {
		debug(D_VINE, "Could not open %s for writing. (%s)\n", filename, strerror(errno));
		return 0;
	}

	int64_t actual = link_stream_to_fd(lnk, fd, length, stoptime);

	if (actual != length) {
		debug(D_VINE, "Failed to put file - %s (%s)\n", filename, strerror(errno));
		close(fd);
		return 0;
	}

	if (close(fd) < 0) {
		debug(D_VINE, "Failed to close file - %s (%s)\n", filename, strerror(errno));
		return 0;
	}

	chmod(filename, mode);

	return 1;
}

static int vine_transfer_get_dir_internal(struct link *lnk, const char *dirname, int64_t *totalsize, int mode, int mtime, time_t stoptime, char **error_message);

/*
Receive a single item of unknown type into the directory "dirname".
Returns 0 on failure to transfer.
Returns 1 on successful transfer of one item.
Returns 2 on successful receipt of "end" of list.
*/

int vine_transfer_get_any(struct link *lnk, const char *dirname, int64_t *totalsize, int *mode, int *mtime, time_t stoptime, char **error_message)
{
	char line[VINE_LINE_MAX];
	char name_encoded[VINE_LINE_MAX];
	char name[VINE_LINE_MAX];
	int64_t size;
	int errornum;

	if (!recv_message(lnk, line, sizeof(line), stoptime)) {
		/* network error before getting any message type */
		if (error_message && *error_message == NULL) {
			*error_message = string_format("Failed to receive message from worker: %s", strerror(errno));
		}
		return 0;
	}

	int r = 0;

	if (sscanf(line, "file %s %" SCNd64 " %o %d", name_encoded, &size, mode, mtime) == 4) {

		url_decode(name_encoded, name, sizeof(name));

		/* Only use the normal mode bits. */
		*mode &= 0777;

		char *subname = string_format("%s/%s", dirname, name);
		r = vine_transfer_get_file_internal(lnk, subname, size, *mode, *mtime, stoptime);
		if (!r && error_message && *error_message == NULL) {
			*error_message = string_format("Failed processing file '%s': %s", name, strerror(errno));
		}
		free(subname);

		*totalsize += size;

	} else if (sscanf(line, "symlink %s %" SCNd64, name_encoded, &size) == 2) {

		url_decode(name_encoded, name, sizeof(name));

		char *subname = string_format("%s/%s", dirname, name);
		r = vine_transfer_get_symlink_internal(lnk, subname, size, stoptime);
		if (!r && error_message && *error_message == NULL) {
			*error_message = string_format("Failed processing symlink '%s': %s", name, strerror(errno));
		}
		free(subname);

		// The symlink doesn't really have an inherent mtime or mode
		*totalsize += size;
		*mtime = 0;
		*mode = 0777;

	} else if (sscanf(line, "dir %s %o %d", name_encoded, mode, mtime) == 3) {

		url_decode(name_encoded, name, sizeof(name));

		/* Only use the normal mode bits. */
		*mode &= 0777;

		char *subname = string_format("%s/%s", dirname, name);
		r = vine_transfer_get_dir_internal(lnk, subname, totalsize, *mode, *mtime, stoptime, error_message);
		if (!r && error_message && *error_message == NULL) {
			*error_message = string_format("Failed processing directory '%s': %s", name, strerror(errno));
		}
		free(subname);

	} else if (sscanf(line, "error %s %d", name_encoded, &errornum) == 2) {

		url_decode(name_encoded, name, sizeof(name));

		const char *err_str = strerror(errornum);
		debug(D_VINE, "Received error from peer for '%s': %s", name, err_str);

		if (error_message && *error_message == NULL) {
			*error_message = string_format("Remote worker reported error for '%s': %s", name, err_str);
		}
		r = 0;

	} else if (!strcmp(line, "end")) {
		r = 2;
	} else {
		char line_preview[101];
		snprintf(line_preview, sizeof(line_preview), "%.100s", line);

		debug(D_VINE, "Received invalid line from peer: %s", line_preview);
		if (error_message && *error_message == NULL) {
			*error_message = string_format("Received invalid line from peer: %s", line_preview);
		}
		r = 0;
	}

	return r;
}

/*
Handle an incoming directory inside the recursive dir protocol.
Notice that we have already checked the dirname for validity,
and now we process "file" and "dir" commands within the list
until "end" is reached.
*/

static int vine_transfer_get_dir_internal(struct link *lnk, const char *dirname, int64_t *totalsize, int mode, int mtime, time_t stoptime, char **error_message)
{
	/* Only use the normal mode bits. */
	mode &= 0777;

	int result = mkdir(dirname, mode);
	if (result < 0) {
		debug(D_VINE, "unable to create %s: %s", dirname, strerror(errno));
		if (error_message && *error_message == NULL) {
			*error_message = string_format("Unable to create directory '%s': %s", dirname, strerror(errno));
		}
		return 0;
	}

	while (1) {
		/* dummy values that won't get used. */
		int temp_mode, temp_mtime;

		int r = vine_transfer_get_any(lnk, dirname, totalsize, &temp_mode, &temp_mtime, stoptime, error_message);
		if (r == 1) {
			// Successfully received one item.
			continue;
		} else if (r == 2) {
			// Sucessfully got end of sequence.
			return 1;
		} else {
			// Failed to receive item.
			return 0;
		}
	}

	return 0;
}

int vine_transfer_request_any(struct link *lnk, const char *request_path, const char *dirname, int64_t *totalsize, int *mode, int *mtime, time_t stoptime, char **error_message)
{
	debug(D_VINE, "tx: get %s", request_path);
	if (link_printf(lnk, stoptime, "get %s\n", request_path) < 0) {
		return 0;
	}
	return vine_transfer_get_any(lnk, dirname, totalsize, mode, mtime, stoptime, error_message);
}
