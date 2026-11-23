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
#include "full_io.h"
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

	/* Each piece of 64 KB gets a new deadline, so a large file that keeps arriving is never cut off. A piece that
	 * ends short ended at its deadline, at the end of the stream, or on an error, so the transfer stops there: a source
	 * that stops in the middle of a piece costs one timeout, not that piece's and the next one's. */
	int64_t actual = 0;
	while (actual < length) {
		int64_t wanted = MIN(length - actual, 1 << 16);
		int64_t piece = link_stream_to_fd(lnk, fd, wanted, time(0) + timeout);
		if (piece > 0) {
			actual += piece;
		}
		if (piece < wanted) {
			break;
		}
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

/*
The receive in pieces (vine_transfer_receiver_continue()). It walks the same protocol as vine_transfer_get_item(), but
keeps its position between calls instead of on the stack: the directory that receives the next header and how many
directories are open, and the file or symlink whose contents are arriving with the bytes still missing.
*/

struct vine_transfer_receiver {
	char *dir;        /* The directory that receives the next header: dirname, then each directory open inside it. */
	int depth;        /* The directories open inside dirname: dir is dirname when zero. */
	int started;      /* Non-zero once the header of the requested item was read, which alone is logged. */
	char *name;       /* The name of the requested item when it is a directory, for the line logged when it ends. */
	int fd;           /* The file whose contents are arriving, or -1. */
	char *target;     /* The target of the symlink whose body is arriving, or NULL. */
	char *path;       /* The path of that file or symlink. */
	int item_mode;    /* The mode of that file. */
	int64_t length;   /* The bytes of its contents or target. */
	int64_t received; /* The bytes of them received so far. */
	int64_t entries;  /* The items received inside the requested directory. */
	int64_t size;     /* The bytes of the files and symlinks received. */
	int mode;         /* The mode of the requested item. */
	int mtime;        /* The mtime of the requested item. */
};

struct vine_transfer_receiver *vine_transfer_receiver_create(const char *dirname)
{
	struct vine_transfer_receiver *r = calloc(1, sizeof(*r));
	if (!r) {
		return NULL;
	}
	r->dir = strdup(dirname);
	if (!r->dir) {
		free(r);
		return NULL;
	}
	r->fd = -1;
	return r;
}

void vine_transfer_receiver_delete(struct vine_transfer_receiver *r)
{
	if (!r) {
		return;
	}
	if (r->fd >= 0) {
		close(r->fd);
	}
	free(r->target);
	free(r->path);
	free(r->name);
	free(r->dir);
	free(r);
}

int64_t vine_transfer_receiver_size(const struct vine_transfer_receiver *r)
{
	return r->size;
}

int vine_transfer_receiver_mode(const struct vine_transfer_receiver *r)
{
	return r->mode;
}

int vine_transfer_receiver_mtime(const struct vine_transfer_receiver *r)
{
	return r->mtime;
}

/* An item ended: the requested one, or one more inside it. Returns non-zero when it was the requested item. */
static int receiver_item_done(struct vine_transfer_receiver *r)
{
	if (r->depth == 0) {
		return 1;
	}
	r->entries++;
	return 0;
}

/* Close the file or create the symlink whose contents have all arrived. Returns zero on failure. */
static int receiver_finish_contents(struct vine_transfer_receiver *r)
{
	int ok = 1;
	if (r->fd >= 0) {
		/* The open descriptor sets the mode without resolving the path again. */
		fchmod(r->fd, r->item_mode);
		if (close(r->fd) < 0) {
			debug(D_VINE, "Failed to close file - %s (%s)\n", r->path, strerror(errno));
			ok = 0;
		}
		r->fd = -1;
	} else {
		r->target[r->length] = 0;
		if (symlink(r->target, r->path) < 0) {
			debug(D_VINE, "could not create symlink %s: %s", r->path, strerror(errno));
			ok = 0;
		}
		free(r->target);
		r->target = NULL;
	}
	free(r->path);
	r->path = NULL;
	return ok;
}

/* Read one piece of at most 64 KB of the contents of the current file or symlink without waiting. Returns PENDING when
 * nothing has arrived, FAILED on failure, and COMPLETE after a piece. */
static vine_transfer_receive_status_t receiver_read_contents(struct vine_transfer_receiver *r, struct link *lnk)
{
	char buffer[1 << 16];
	char *data = r->target ? r->target + r->received : buffer;
	size_t wanted = (size_t)MIN(r->length - r->received, (int64_t)sizeof(buffer));

	ssize_t n = link_read_avail(lnk, data, wanted, LINK_NOWAIT);
	if (n < 0 && (errno == ETIMEDOUT || errno_is_temporary(errno))) {
		return VINE_TRANSFER_RECEIVE_PENDING;
	}
	if (n <= 0) {
		debug(D_VINE, "Failed to put file - %s (%s)\n", r->path, n == 0 ? "end of stream" : strerror(errno));
		return VINE_TRANSFER_RECEIVE_FAILED;
	}
	if (!r->target && full_write(r->fd, buffer, n) != n) {
		debug(D_VINE, "Failed to put file - %s (%s)\n", r->path, strerror(errno));
		return VINE_TRANSFER_RECEIVE_FAILED;
	}
	r->received += n;
	return VINE_TRANSFER_RECEIVE_COMPLETE;
}

/* Handle one whole header line. Returns COMPLETE when the requested item ended, PENDING when more follows, and FAILED
 * or REFUSED otherwise. */
static vine_transfer_receive_status_t receiver_header(struct vine_transfer_receiver *r, const char *line)
{
	char name_encoded[VINE_LINE_MAX];
	char name[VINE_LINE_MAX];
	int64_t size;
	int mode, mtime, errornum;

	if (sscanf(line, "file %s %" SCNd64 " %o %d", name_encoded, &size, &mode, &mtime) == 4 && size >= 0) {
		url_decode(name_encoded, name, sizeof(name));
		/* Only use the normal mode bits. */
		mode &= 0777;
		if (r->depth == 0) {
			r->mode = mode;
			r->mtime = mtime;
		}
		r->path = string_format("%s/%s", r->dir, name);
		if (!check_disk_space_for_filesize(".", size, 0)) {
			debug(D_VINE, "Could not put file %s, not enough disk space (%" PRId64 " bytes needed)\n", r->path, size);
			return VINE_TRANSFER_RECEIVE_FAILED;
		}
		r->fd = open(r->path, O_WRONLY | O_CREAT | O_TRUNC, 0700);
		if (r->fd < 0) {
			debug(D_VINE, "Could not open %s for writing. (%s)\n", r->path, strerror(errno));
			return VINE_TRANSFER_RECEIVE_FAILED;
		}
		r->item_mode = mode;
		r->length = size;
		r->received = 0;
		r->size += size;
		return VINE_TRANSFER_RECEIVE_PENDING;
	} else if (sscanf(line, "symlink %s %" SCNd64, name_encoded, &size) == 2 && size >= 0) {
		url_decode(name_encoded, name, sizeof(name));
		/* The symlink doesn't really have an inherent mtime or mode. */
		if (r->depth == 0) {
			r->mode = 0777;
			r->mtime = 0;
		}
		/* The body holds the target without a terminating null. */
		r->target = malloc(size + 1);
		if (!r->target) {
			return VINE_TRANSFER_RECEIVE_FAILED;
		}
		r->path = string_format("%s/%s", r->dir, name);
		r->length = size;
		r->received = 0;
		r->size += size;
		return VINE_TRANSFER_RECEIVE_PENDING;
	} else if (sscanf(line, "dir %s %o %d", name_encoded, &mode, &mtime) == 3) {
		url_decode(name_encoded, name, sizeof(name));
		/* Only use the normal mode bits. */
		mode &= 0777;
		char *subname = string_format("%s/%s", r->dir, name);
		if (mkdir(subname, mode) < 0) {
			debug(D_VINE, "unable to create %s: %s", subname, strerror(errno));
			free(subname);
			return VINE_TRANSFER_RECEIVE_FAILED;
		}
		if (r->depth == 0) {
			r->mode = mode;
			r->mtime = mtime;
			r->name = strdup(name);
		}
		free(r->dir);
		r->dir = subname;
		r->depth++;
		return VINE_TRANSFER_RECEIVE_PENDING;
	} else if (sscanf(line, "error %s %d", name_encoded, &errornum) == 2) {
		url_decode(name_encoded, name, sizeof(name));
		debug(D_VINE, "Received error from peer for '%s': %s", name, strerror(errornum));
		/* The sender answered and cannot send this item, so the transfer cannot succeed from it. */
		return VINE_TRANSFER_RECEIVE_REFUSED;
	} else if (!strcmp(line, "end") && r->depth > 0) {
		/* The open directory ends: the next header goes to its parent. */
		*strrchr(r->dir, '/') = 0;
		r->depth--;
		return receiver_item_done(r) ? VINE_TRANSFER_RECEIVE_COMPLETE : VINE_TRANSFER_RECEIVE_PENDING;
	} else {
		debug(D_VINE, "Received invalid line from peer: %.100s", line);
		return VINE_TRANSFER_RECEIVE_FAILED;
	}
}

vine_transfer_receive_status_t vine_transfer_receiver_continue(struct vine_transfer_receiver *r, struct link *lnk, const struct budget *b)
{
	vine_transfer_receive_status_t status = VINE_TRANSFER_RECEIVE_PENDING;
	int pieces = 0;

	while (pieces == 0 || !budget_expired(b)) {
		pieces++;
		if (r->fd < 0 && !r->target) {
			/* A header is read only once it is whole, so a sender that stops in the middle of one never holds the
			 * caller. */
			if (link_buffer_has_line(lnk) != 1) {
				break;
			}
			char line[VINE_LINE_MAX];
			if (!link_readline(lnk, line, sizeof(line), LINK_NOWAIT)) {
				debug(D_VINE, "Failed to receive a header: %s", strerror(errno));
				status = VINE_TRANSFER_RECEIVE_FAILED;
				break;
			}
			if (!r->started) {
				debug(D_VINE, "rx: %s", line);
				r->started = 1;
			}
			status = receiver_header(r, line);
			if (status != VINE_TRANSFER_RECEIVE_PENDING) {
				break;
			}
			if (r->fd < 0 && !r->target) {
				continue;
			}
		} else {
			status = receiver_read_contents(r, lnk);
			if (status != VINE_TRANSFER_RECEIVE_COMPLETE) {
				break;
			}
			status = VINE_TRANSFER_RECEIVE_PENDING;
		}

		/* Contents that are complete, also empty ones, are finished at once: nothing more arrives for them, so a
		 * caller that waits for input would wait for nothing. */
		if (r->received < r->length) {
			continue;
		}
		if (!receiver_finish_contents(r)) {
			status = VINE_TRANSFER_RECEIVE_FAILED;
			break;
		}
		if (receiver_item_done(r)) {
			status = VINE_TRANSFER_RECEIVE_COMPLETE;
			break;
		}
	}

	if (r->name && status != VINE_TRANSFER_RECEIVE_PENDING) {
		debug(D_VINE, "rx: %s directory %s: %" PRId64 " entries, %" PRId64 " bytes", status == VINE_TRANSFER_RECEIVE_COMPLETE ? "received" : "failed to receive", r->name, r->entries, r->size);
	}
	return status;
}
