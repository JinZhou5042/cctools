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

/*
Queue a cached object of any type to be sent. Return zero if the writer refused it.
*/

int vine_transfer_put_any(struct link_writer *lw, struct vine_cache *cache, const char *filename, vine_transfer_mode_t xfer_mode)
{
	char *cached_path = vine_cache_data_path(cache, filename);
	int result;

	if (path_within_dir(cached_path, workspace->cache_dir)) {
		result = vine_transfer_queue_item(lw, cached_path, path_basename(filename), xfer_mode);
	} else {
		debug(D_VINE, "refusing to send %s: not within %s", cached_path, workspace->cache_dir);
		result = EACCES;
	}
	if (result > 0) {
		result = vine_transfer_queue_error(lw, path_basename(filename), result) ? 0 : -1;
	}

	free(cached_path);
	return result == 0;
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
	struct link_writer *lw = link_writer_create(lnk);
	if (!lw) {
		return 0;
	}
	char *request = string_format("get %s\n", request_path);
	int sent = link_writer_push(lw, request, strlen(request)) && link_writer_send_sync(lw, timestamp_monotonic() + (stoptime > time(0) ? (timestamp_t)(stoptime - time(0)) * 1000000 : 0), 0);
	free(request);
	link_writer_delete(lw);
	if (!sent) {
		return 0;
	}
	return vine_transfer_get_any(lnk, dirname, totalsize, mode, mtime, stoptime, error_message);
}
