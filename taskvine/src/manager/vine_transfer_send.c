/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_transfer_send.h"
#include "vine_protocol.h"

#include "debug.h"
#include "stringtools.h"
#include "url_encode.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Bytes of headers that one call of walk_produce() queues at most. */
#define WALK_CHUNK (64 * 1024)

/* A directory being sent, open while its entries are queued. */
struct walk_frame {
	DIR *dir;		   /* The open directory. */
	char *path;		   /* Its full path. */
	struct walk_frame *parent; /* The directory that contains it, or NULL at the top of the walk. */
};

/* The walk of a directory being sent, which queues its next entries each time it reaches the head of the writer. */
struct walk {
	struct walk_frame *top; /* The innermost directory being walked, or NULL when the walk is done. */
	size_t queued;		/* Bytes of headers queued by the current call. */
};

/* Queue a header line and log it, counting its bytes in queued if given. */
__attribute__((format(printf, 3, 4))) static int queue_line(struct link_writer *lw, size_t *queued, const char *fmt, ...)
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
	if (queued) {
		*queued += length;
	}
	return link_writer_push(lw, line, length);
}

static void walk_pop(struct walk *w)
{
	struct walk_frame *f = w->top;
	w->top = f->parent;
	closedir(f->dir);
	free(f->path);
	free(f);
}

static void walk_delete(void *arg)
{
	struct walk *w = arg;
	while (w->top) {
		walk_pop(w);
	}
	free(w);
}

/* Queue an open file or directory, taking its descriptor. A directory becomes the innermost frame of the walk, and its
 * entries follow in later calls. Return 2 for a file, whose contents fill the writer's next chunks, 1 for a directory,
 * zero if the writer refused it, or -1 if the directory cannot be read, with errno set and nothing queued. */
static int queue_open_item(struct link_writer *lw, struct walk *w, int fd, const struct stat *info, const char *path, const char *encoded)
{
	/* Send only the normal mode bits. */
	int mode = info->st_mode & 0777;

	if (S_ISREG(info->st_mode)) {
		int64_t length = info->st_size;
		if (!queue_line(lw, &w->queued, "file %s %" PRId64 " 0%o %lld\n", encoded, length, mode, (long long)info->st_mtime)) {
			close(fd);
			return 0;
		}
		return link_writer_push_fd(lw, fd, 0, length) ? 2 : 0;
	}

	DIR *dir = fdopendir(fd);
	if (!dir) {
		int saved = errno;
		close(fd);
		errno = saved;
		return -1;
	}
	struct walk_frame *f = malloc(sizeof(*f));
	char *copy = strdup(path);
	if (!f || !copy) {
		free(f);
		free(copy);
		closedir(dir);
		return 0;
	}
	*f = (struct walk_frame){dir, copy, w->top};
	w->top = f;
	return queue_line(lw, &w->queued, "dir %s %o %lld\n", encoded, mode, (long long)info->st_mtime);
}

/* Queue one entry of a directory. Any entry that cannot be sent is sent as "error", which fails the transfer. Return 2
 * for a file, 1 for any other entry, or zero if the writer refused it. */
static int queue_entry(struct link_writer *lw, struct walk *w, const char *path, const char *name)
{
	/* URL encode the name to handle spaces and unprintable characters. */
	char encoded[VINE_LINE_MAX];
	url_encode(name, encoded, sizeof(encoded));

	struct stat info;
	int fd = -1;
	if (stat(path, &info) != 0) {
		goto failure;
	}
	if (!S_ISREG(info.st_mode) && !S_ISDIR(info.st_mode)) {
		errno = EINVAL;
		goto failure;
	}
	fd = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &info) != 0) {
		goto failure;
	}
	int result = queue_open_item(lw, w, fd, &info, path, encoded);
	if (result >= 0) {
		return result;
	}
	fd = -1;

failure:
	if (fd >= 0) {
		int saved = errno;
		close(fd);
		errno = saved;
	}
	return queue_line(lw, &w->queued, "error %s %d\n", encoded, errno);
}

/* Queue the next entries of a walk, up to a file or WALK_CHUNK bytes of headers. Return one while entries remain, zero
 * when the walk is done, or -1 if the writer refused an entry. */
static int walk_produce(struct link_writer *lw, void *arg)
{
	struct walk *w = arg;
	w->queued = 0;

	while (w->top && w->queued < WALK_CHUNK) {
		struct dirent *dent = readdir(w->top->dir);
		if (!dent) {
			walk_pop(w);
			if (!queue_line(lw, &w->queued, "end\n")) {
				return -1;
			}
			continue;
		}
		if (!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, "..")) {
			continue;
		}
		char *sub_path = string_format("%s/%s", w->top->path, dent->d_name);
		int result = queue_entry(lw, w, sub_path, dent->d_name);
		free(sub_path);
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

int vine_transfer_queue_item(struct link_writer *lw, const char *path, const char *name, vine_transfer_mode_t mode)
{
	struct stat info;
	int fd = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &info) != 0) {
		goto failure;
	}
	if (!S_ISREG(info.st_mode) && (mode == VINE_TRANSFER_MODE_FILE_ONLY || !S_ISDIR(info.st_mode))) {
		/* Choose a suitable error number for what the receiver cannot take. */
		errno = S_ISDIR(info.st_mode) ? EISDIR : EINVAL;
		goto failure;
	}

	struct walk *w = calloc(1, sizeof(*w));
	if (!w) {
		close(fd);
		return -1;
	}

	char encoded[VINE_LINE_MAX];
	url_encode(name, encoded, sizeof(encoded));
	int result = queue_open_item(lw, w, fd, &info, path, encoded);
	if (result < 0) {
		int saved = errno;
		walk_delete(w);
		return saved;
	}
	if (!result) {
		walk_delete(w);
		return -1;
	}
	if (!w->top) {
		walk_delete(w);
		return 0;
	}
	return link_writer_push_producer(lw, walk_produce, walk_delete, w) ? 0 : -1;

failure:;
	int saved = errno;
	if (fd >= 0) {
		close(fd);
	}
	return saved;
}

int vine_transfer_queue_error(struct link_writer *lw, const char *name, int errnum)
{
	char encoded[VINE_LINE_MAX];
	url_encode(name, encoded, sizeof(encoded));
	return queue_line(lw, NULL, "error %s %d\n", encoded, errnum);
}
