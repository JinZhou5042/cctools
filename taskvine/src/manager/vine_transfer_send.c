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

/* A directory of a walk, identified by device and inode: one being sent, open while its entries are queued, or one that
 * contains the requested item, which is never opened. A link that leads to one of them is sent as a link instead of
 * being followed, which would send the tree again without end. */
struct walk_frame {
	DIR *dir;		   /* The open directory, or NULL for a directory that contains the requested item. */
	char *path;		   /* Its full path, or NULL for a directory that contains the requested item. */
	dev_t device;		   /* Device of the directory. */
	ino_t inode;		   /* Inode of the directory. */
	struct walk_frame *parent; /* The directory that contains it, or NULL at the top of its list. */
};

/* The walk of a directory being sent, which queues its next entries each time it reaches the head of the writer. */
struct walk {
	struct walk_frame *top;	  /* The innermost directory being walked, or NULL when the walk is done. */
	struct walk_frame *above; /* The directories that contain the requested item, the innermost first. */
	size_t queued;		  /* Bytes of headers queued by the current call. */
	char *name;		  /* Name of the requested directory, for its log line, or NULL before it is queued. */
	int64_t entries;	  /* Entries queued inside the requested directory. */
	int64_t bytes;		  /* Bytes of files and symlinks queued inside it. */
};

/* Queue a header line, counting its bytes in the walk. The log gets the header of the requested item and, for a
 * directory, one line when it ends, as a receive logs, not a line per entry. */
__attribute__((format(printf, 3, 4))) static int queue_line(struct link_writer *lw, struct walk *w, const char *fmt, ...)
{
	char line[2 * VINE_LINE_MAX];
	va_list va;
	va_start(va, fmt);
	int length = vsnprintf(line, sizeof(line), fmt, va);
	va_end(va);
	if (length < 0 || (size_t)length >= sizeof(line)) {
		return 0;
	}
	if (!w || !w->name) {
		debug(D_VINE, "tx: %s", line);
	}
	if (w) {
		w->queued += length;
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
	while (w->above) {
		struct walk_frame *f = w->above;
		w->above = f->parent;
		free(f);
	}
	free(w->name);
	free(w);
}

/* Check whether a directory is being sent or contains the requested item. */
static int walk_holds(struct walk *w, const struct stat *info)
{
	for (const struct walk_frame *f = w->top; f; f = f->parent) {
		if (f->device == info->st_dev && f->inode == info->st_ino) {
			return 1;
		}
	}
	for (const struct walk_frame *f = w->above; f; f = f->parent) {
		if (f->device == info->st_dev && f->inode == info->st_ino) {
			return 1;
		}
	}
	return 0;
}

/* Record the directories that contain the item at path, from the root down, so the innermost ends first in the list.
 * Return zero if memory is exhausted. */
static int walk_find_above(struct walk *w, const char *path)
{
	char *prefix = strdup(path);
	if (!prefix) {
		return 0;
	}
	for (char *slash = strchr(prefix, '/'); slash; slash = strchr(slash + 1, '/')) {
		char kept = slash[1];
		slash[1] = 0;
		struct stat info;
		int found = stat(prefix, &info) == 0;
		slash[1] = kept;
		if (found) {
			struct walk_frame *f = calloc(1, sizeof(*f));
			if (!f) {
				free(prefix);
				return 0;
			}
			f->device = info.st_dev;
			f->inode = info.st_ino;
			f->parent = w->above;
			w->above = f;
		}
	}
	free(prefix);
	return 1;
}

/* Queue a symbolic link as a link, with its target. Return zero if the writer refused it, or -1 if the link cannot be
 * read, with errno set and nothing queued. */
static int queue_link(struct link_writer *lw, struct walk *w, const char *path, const char *encoded)
{
	char target[VINE_LINE_MAX];
	int length = readlink(path, target, sizeof(target));
	if (length <= 0) {
		if (length == 0) {
			errno = EINVAL;
		}
		return -1;
	}
	w->bytes += length;
	return queue_line(lw, w, "symlink %s %d\n", encoded, length) && link_writer_push(lw, target, length);
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
		if (!queue_line(lw, w, "file %s %" PRId64 " 0%o %lld\n", encoded, length, mode, (long long)info->st_mtime)) {
			close(fd);
			return 0;
		}
		w->bytes += length;
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
	*f = (struct walk_frame){dir, copy, info->st_dev, info->st_ino, w->top};
	w->top = f;
	return queue_line(lw, w, "dir %s %o %lld\n", encoded, mode, (long long)info->st_mtime);
}

/*
Queue one entry of a directory. Links are followed and sent as what they lead to, except a link that cannot be followed,
such as one whose target does not exist, and a link to a directory being sent or to one that contains the requested
item, which are sent as links. A FIFO, a socket, or a device has no data to send and is skipped. Any other entry that
cannot be read is sent as "error", which fails the transfer: the receiver cannot hold the item as it is. Return 2 for a
file, 1 for any other entry, or zero if the writer refused it.
*/
static int queue_entry(struct link_writer *lw, struct walk *w, const char *path, const char *name)
{
	/* URL encode the name to handle spaces and unprintable characters. */
	char encoded[VINE_LINE_MAX];
	url_encode(name, encoded, sizeof(encoded));

	struct stat info;
	int fd = -1;
	int result;
	w->entries++;
	if (stat(path, &info) != 0 && lstat(path, &info) != 0) {
		goto failure;
	}
	if (S_ISDIR(info.st_mode) && walk_holds(w, &info)) {
		/* A link to a directory being sent or containing the item. A loop without a link, such as a bind mount, cannot
		 * be sent. */
		if (lstat(path, &info) != 0) {
			goto failure;
		}
		if (!S_ISLNK(info.st_mode)) {
			errno = ELOOP;
			goto failure;
		}
	}
	if (S_ISLNK(info.st_mode)) {
		result = queue_link(lw, w, path, encoded);
		if (result >= 0) {
			return result;
		}
		goto failure;
	}
	if (!S_ISREG(info.st_mode) && !S_ISDIR(info.st_mode)) {
		debug(D_VINE, "not sending %s: a FIFO, socket, or device holds no data", path);
		return 1;
	}
	fd = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &info) != 0) {
		goto failure;
	}
	result = queue_open_item(lw, w, fd, &info, path, encoded);
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
	int errnum = errno;
	debug(D_VINE, "cannot send %s: %s", path, strerror(errnum));
	return queue_line(lw, w, "error %s %d\n", encoded, errnum);
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
			if (!queue_line(lw, w, "end\n")) {
				return -1;
			}
			if (!w->top) {
				debug(D_VINE, "tx: sent directory %s: %" PRId64 " entries, %" PRId64 " bytes", w->name, w->entries, w->bytes);
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
	int result;
	if (!S_ISDIR(info.st_mode)) {
		result = queue_open_item(lw, w, fd, &info, path, encoded);
	} else if (!walk_find_above(w, path)) {
		close(fd);
		result = 0;
	} else if (walk_holds(w, &info)) {
		/* A link to a directory that contains the item is sent as a link. A loop without a link cannot be sent. */
		close(fd);
		struct stat link_info;
		if (lstat(path, &link_info) != 0) {
			result = -1;
		} else if (!S_ISLNK(link_info.st_mode)) {
			errno = ELOOP;
			result = -1;
		} else {
			result = queue_link(lw, w, path, encoded);
		}
	} else {
		result = queue_open_item(lw, w, fd, &info, path, encoded);
	}
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
	w->bytes = 0;
	w->name = strdup(name);
	if (!w->name) {
		walk_delete(w);
		return -1;
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
