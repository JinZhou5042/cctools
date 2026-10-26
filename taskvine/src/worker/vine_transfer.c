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
#include <stdarg.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

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

/* Bytes of headers that one call of put_walk_produce() queues at most, before it lets the queue write them. */
#define PUT_WALK_CHUNK (64 * 1024)

/* A directory of a walk, identified by device and inode: one being sent, open while its entries are queued, or one that
 * contains the requested item, which is never opened. A link that leads to one of them is sent as a link instead of
 * being followed, which would send the tree again without end. */
struct put_frame {
	DIR *dir;		  /* The open directory, or NULL for a directory that contains the requested item. */
	char *path;		  /* Its full path, or NULL for a directory that contains the requested item. */
	dev_t device;		  /* Device of the directory. */
	ino_t inode;		  /* Inode of the directory. */
	struct put_frame *parent; /* The directory that contains it, or NULL at the top of its list. */
};

/* The walk of an item being sent, which queues its next entries each time the queue reaches it. */
struct put_walk {
	char *full_name;	   /* Full path of the item. */
	char *relative_name;	   /* Name the item is sent under. */
	vine_transfer_mode_t mode; /* What the receiver accepts. */
	int started;		   /* The item itself was queued. */
	struct put_frame *top;	   /* The innermost directory being walked, or NULL. */
	struct put_frame *above;   /* The directories that contain the item, the innermost first. */
	size_t queued;		   /* Bytes of headers queued by the current call. */
};

/* Queue a header line, with a debug line as every message to a peer has. */
__attribute__((format(printf, 3, 4))) static int put_line(struct link_queue *queue, struct put_walk *w, const char *fmt, ...)
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
	return link_queue_push(queue, line, length);
}

/* Check whether a directory is being sent or contains the requested item. */
static int put_walk_holds(struct put_walk *w, const struct stat *info)
{
	for (const struct put_frame *f = w->top; f; f = f->parent) {
		if (f->device == info->st_dev && f->inode == info->st_ino) {
			return 1;
		}
	}
	for (const struct put_frame *f = w->above; f; f = f->parent) {
		if (f->device == info->st_dev && f->inode == info->st_ino) {
			return 1;
		}
	}
	return 0;
}

/*
Queue one item of a walk: the header and contents of a file or symlink, the header of a directory, whose entries follow
in later calls, or an error. Links are followed and sent as what they lead to, except a link that cannot be followed,
such as one whose target does not exist, and a link to a directory being sent or to one that contains the requested
item, which are sent as links. Inside a directory (nested), a FIFO, a socket, or a device has no data to send and is
skipped. Any other item that cannot be read is sent as "error", which fails the transfer: the receiver cannot hold the
item as it is. Return 2 for a file, whose contents fill the queue's next chunks, 1 for any other item, or zero if the
queue refused it.
*/
static int put_item(struct link_queue *queue, struct put_walk *w, const char *full_name, const char *relative_name, int nested)
{
	struct stat info;

	/* URL encode filename to handle spaces and unprintable characters. */
	char relative_name_encoded[VINE_LINE_MAX];
	url_encode(relative_name, relative_name_encoded, sizeof(relative_name_encoded));

	if (stat(full_name, &info) != 0 && lstat(full_name, &info) != 0) {
		goto access_failure;
	}

	if (S_ISDIR(info.st_mode) && put_walk_holds(w, &info)) {
		/* A link to a directory being sent or containing the item. A loop without a link, such as a bind mount, cannot
		 * be sent. */
		if (lstat(full_name, &info) != 0) {
			goto access_failure;
		}
		if (!S_ISLNK(info.st_mode)) {
			errno = ELOOP;
			goto access_failure;
		}
	}

	/* Send only the normal mode bits. */
	int mode = info.st_mode & 0777;

	if (S_ISREG(info.st_mode)) {
		int fd = open(full_name, O_RDONLY, 0);
		if (fd < 0) {
			goto access_failure;
		}
		int64_t length = info.st_size;
		if (!put_line(queue, w, "file %s %" PRId64 " 0%o %lld\n", relative_name_encoded, length, mode, (long long)info.st_mtime)) {
			close(fd);
			return 0;
		}
		/* The queue reads the file when it reaches it, and fails if the file is shorter by then. */
		return link_queue_push_fd(queue, fd, 0, length) ? 2 : 0;
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
		*f = (struct put_frame){dir, path, info.st_dev, info.st_ino, w->top};
		w->top = f;
		return put_line(queue, w, "dir %s %o %lld\n", relative_name_encoded, mode, (long long)info.st_mtime);
	} else if (S_ISLNK(info.st_mode)) {
		char link_target[VINE_LINE_MAX];
		int result = readlink(full_name, link_target, sizeof(link_target));
		if (result > 0) {
			return put_line(queue, w, "symlink %s %d\n", relative_name_encoded, result) && link_queue_push(queue, link_target, result);
		} else {
			goto access_failure;
		}
	} else if (nested) {
		debug(D_VINE, "not sending %s: a FIFO, socket, or device holds no data", full_name);
		return 1;
	} else {
		errno = EINVAL;
		goto access_failure;
	}

access_failure:
	// An error here is not a failure from our perspective, keep going.
	return put_line(queue, w, "error %s %d\n", relative_name_encoded, errno);
}

/* Close the innermost directory of a walk. */
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
	while (w->above) {
		struct put_frame *f = w->above;
		w->above = f->parent;
		free(f);
	}
	free(w->full_name);
	free(w->relative_name);
	free(w);
}

/* Queue the next entries of a walk, up to a file or PUT_WALK_CHUNK bytes of headers, so the queue holds little of a
 * large directory at a time. Return one while entries remain, zero when the walk is done, or -1 if the queue refused
 * an entry. */
static int put_walk_produce(struct link_queue *queue, void *arg)
{
	struct put_walk *w = arg;
	w->queued = 0;

	if (!w->started) {
		w->started = 1;
		if (!put_item(queue, w, w->full_name, w->relative_name, 0)) {
			return -1;
		}
		return w->top ? 1 : 0;
	}

	while (w->top && w->queued < PUT_WALK_CHUNK) {
		struct dirent *dent = readdir(w->top->dir);
		if (!dent) {
			put_walk_pop(w);
			if (!put_line(queue, w, "end\n")) {
				return -1;
			}
			continue;
		}
		if (!strcmp(dent->d_name, ".") || !strcmp(dent->d_name, "..")) {
			continue;
		}
		char *sub_full_name = string_format("%s/%s", w->top->path, dent->d_name);
		int result = put_item(queue, w, sub_full_name, dent->d_name, 1);
		free(sub_full_name);
		if (!result) {
			return -1;
		}
		/* A file's contents go out before the next entry is read, so the queue holds one open file at a time. */
		if (result == 2) {
			break;
		}
	}
	return w->top ? 1 : 0;
}

/* Queue the walk of an item to be sent. Return zero if the queue refused it. */
static int put_walk_start(struct link_queue *queue, const char *full_name, const char *relative_name, vine_transfer_mode_t mode)
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

	/* The directories that contain the item, from the root down, so the innermost ends first in the list. */
	char *prefix = w->full_name;
	for (char *slash = strchr(prefix, '/'); slash; slash = strchr(slash + 1, '/')) {
		char kept = slash[1];
		slash[1] = 0;
		struct stat info;
		int found = stat(prefix, &info) == 0;
		slash[1] = kept;
		if (found) {
			struct put_frame *f = calloc(1, sizeof(*f));
			if (!f) {
				put_walk_delete(w);
				return 0;
			}
			f->device = info.st_dev;
			f->inode = info.st_ino;
			f->parent = w->above;
			w->above = f;
		}
	}

	return link_queue_push_producer(queue, put_walk_produce, put_walk_delete, w);
}

/*
Queue a cached object of any type to be sent.
*/

int vine_transfer_put_any(struct link_queue *queue, struct vine_cache *cache, const char *filename, vine_transfer_mode_t xfer_mode)
{
	char *cached_path = vine_cache_data_path(cache, filename);
	int r;

	if (path_within_dir(cached_path, workspace->cache_dir)) {
		r = put_walk_start(queue, cached_path, path_basename(filename), xfer_mode);
	} else {
		/* Same reply as an unreadable file in put_item. */
		debug(D_VINE, "refusing to send %s: not within %s", cached_path, workspace->cache_dir);
		char filename_encoded[VINE_LINE_MAX];
		url_encode(path_basename(filename), filename_encoded, sizeof(filename_encoded));
		char *line = string_format("error %s %d\n", filename_encoded, EACCES);
		debug(D_VINE, "tx: %s", line);
		r = link_queue_push(queue, line, strlen(line));
		free(line);
	}

	free(cached_path);
	return r;
}

int vine_transfer_request_any(struct link *lnk, const char *request_path, const char *dirname, int64_t *totalsize, int *mode, int *mtime, int timeout, char **error_message)
{
	debug(D_VINE, "tx: get %s", request_path);
	if (link_printf(lnk, time(0) + timeout, "get %s\n", request_path) < 0) {
		return 0;
	}
	return vine_transfer_get_any(lnk, dirname, totalsize, mode, mtime, timeout, error_message);
}
