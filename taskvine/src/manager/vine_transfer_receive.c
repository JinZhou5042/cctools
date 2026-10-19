/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* Receive files, directories, and symlinks with the streaming transfer protocol described in vine_transfer_receive.h.
 * Workers and the Manager data service share this code. */

#include "vine_transfer_receive.h"
#include "vine_protocol.h"

#include "debug.h"
#include "host_disk_info.h"
#include "macros.h"
#include "stringtools.h"
#include "url_encode.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Receive one header line. Only the header of the requested item is logged, like every message the Worker receives:
 * entries counts the items received inside a directory, which is NULL for the requested item itself, so a directory of
 * many entries logs one line when it starts and one when it ends instead of a line per entry. */
static int recv_line(struct link *lnk, char *line, int length, time_t stoptime, int64_t *entries)
{
	int result = link_readline(lnk, line, length, stoptime);
	if (result && !entries) {
		debug(D_VINE, "rx: %s", line);
	}
	return result;
}

/*
Handle an incoming symbolic link inside the recursive protocol.
The filename of the symlink was already given in the message,
and the target of the symlink is given as the "body" which
must be read off of the wire.  The symlink target does not
need to be url_decoded because it is sent in the body.
*/

static int vine_transfer_get_symlink_internal(struct link *lnk, char *filename, int length, int timeout)
{
	/* The body holds the target without a terminating null. */
	char *target = malloc(length + 1);
	if (!target) {
		return 0;
	}

	int actual = link_read(lnk, target, length, time(0) + timeout);
	if (actual != length) {
		free(target);
		return 0;
	}
	target[length] = 0;

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

static int vine_transfer_get_file_internal(struct link *lnk, const char *filename, int64_t length, int mode, int mtime, int timeout)
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

	/* Each piece gets a new deadline, so a transfer fails only when no data arrives for timeout seconds, and a large
	 * file that keeps arriving is never cut off. */
	int64_t actual = 0;
	while (actual < length) {
		int64_t piece = link_stream_to_fd(lnk, fd, MIN(length - actual, 1 << 16), time(0) + timeout);
		if (piece <= 0) {
			break;
		}
		actual += piece;
	}

	if (actual != length) {
		debug(D_VINE, "Failed to put file - %s (%s)\n", filename, strerror(errno));
		close(fd);
		return 0;
	}

	/* The open descriptor sets the mode without resolving the path again. */
	fchmod(fd, mode);

	if (close(fd) < 0) {
		debug(D_VINE, "Failed to close file - %s (%s)\n", filename, strerror(errno));
		return 0;
	}

	return 1;
}

static int vine_transfer_get_dir_internal(struct link *lnk, const char *dirname, int64_t *totalsize, int mode, int mtime, int timeout, char **error_message, int64_t *entries);

/*
Receive a single item of unknown type into the directory "dirname".
Returns 0 on failure to transfer.
Returns -1 when the sender reported an item it cannot send, inside the requested item or as it.
Returns 1 on successful transfer of one item.
Returns 2 on successful receipt of "end" of list.
entries is NULL for the requested item, and counts the items received inside it otherwise.
*/

static int vine_transfer_get_item(struct link *lnk, const char *dirname, int64_t *totalsize, int *mode, int *mtime, int timeout, char **error_message, int64_t *entries)
{
	char line[VINE_LINE_MAX];
	char name_encoded[VINE_LINE_MAX];
	char name[VINE_LINE_MAX];
	int64_t size;
	int errornum;

	if (!recv_line(lnk, line, sizeof(line), time(0) + timeout, entries)) {
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
		r = vine_transfer_get_file_internal(lnk, subname, size, *mode, *mtime, timeout);
		if (!r && error_message && *error_message == NULL) {
			*error_message = string_format("Failed processing file '%s': %s", name, strerror(errno));
		}
		free(subname);

		*totalsize += size;

	} else if (sscanf(line, "symlink %s %" SCNd64, name_encoded, &size) == 2) {

		url_decode(name_encoded, name, sizeof(name));

		char *subname = string_format("%s/%s", dirname, name);
		r = vine_transfer_get_symlink_internal(lnk, subname, size, timeout);
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
		int64_t count = 0;
		int64_t before = *totalsize;
		r = vine_transfer_get_dir_internal(lnk, subname, totalsize, *mode, *mtime, timeout, error_message, entries ? entries : &count);
		if (!r && error_message && *error_message == NULL) {
			*error_message = string_format("Failed processing directory '%s': %s", name, strerror(errno));
		}
		if (!entries) {
			debug(D_VINE, "rx: %s directory %s: %" PRId64 " entries, %" PRId64 " bytes", r > 0 ? "received" : "failed to receive", name, count, *totalsize - before);
		}
		free(subname);

	} else if (sscanf(line, "error %s %d", name_encoded, &errornum) == 2) {

		url_decode(name_encoded, name, sizeof(name));

		const char *err_str = strerror(errornum);
		debug(D_VINE, "Received error from peer for '%s': %s", name, err_str);

		if (error_message && *error_message == NULL) {
			*error_message = string_format("Remote worker reported error for '%s': %s", name, err_str);
		}
		/* The sender answered and cannot send this item, so the transfer cannot succeed from it. */
		r = -1;

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

int vine_transfer_get_any(struct link *lnk, const char *dirname, int64_t *totalsize, int *mode, int *mtime, int timeout, char **error_message)
{
	return vine_transfer_get_item(lnk, dirname, totalsize, mode, mtime, timeout, error_message, NULL);
}

/*
Handle an incoming directory inside the recursive dir protocol.
Notice that we have already checked the dirname for validity,
and now we process "file" and "dir" commands within the list
until "end" is reached.
*/

static int vine_transfer_get_dir_internal(struct link *lnk, const char *dirname, int64_t *totalsize, int mode, int mtime, int timeout, char **error_message, int64_t *entries)
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

		int r = vine_transfer_get_item(lnk, dirname, totalsize, &temp_mode, &temp_mtime, timeout, error_message, entries);
		if (r == 1) {
			// Successfully received one item.
			(*entries)++;
			continue;
		} else if (r == 2) {
			// Sucessfully got end of sequence.
			return 1;
		} else {
			// Failed to receive item, or the sender cannot send it (-1).
			return r < 0 ? -1 : 0;
		}
	}

	return 0;
}
