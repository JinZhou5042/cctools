/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "link_queue.h"
#include "debug.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Bytes of a file read per chunk, and the size up to which consecutive small pushes share one entry. */
#define LINK_QUEUE_CHUNK (64 * 1024)

/* One entry of the queue. A data entry holds its bytes. A file entry holds the range still to read, and its current
 * chunk in data while it is at the head. */
struct link_queue_entry {
	char *data;                     /* Bytes to write: all of a data entry, or the current chunk of a file entry. */
	size_t length;                  /* Number of bytes in data. */
	size_t capacity;                /* Bytes allocated for data. */
	size_t offset;                  /* Bytes of data already written. */
	char *path;                     /* File of a file entry, or NULL for a data entry. */
	int fd;                         /* Descriptor of the file while its entry is at the head, else -1. */
	int64_t file_offset;            /* Offset in the file of the next chunk to read. */
	int64_t file_left;              /* Bytes of the range not yet read into a chunk. */
	int started;                    /* A write was attempted, so data stays in place and takes no further pushes. */
	struct link_queue_entry *next;  /* The entry after this one, or NULL at the tail. */
};

/* The queue is a singly linked list of entries, written from the head. */
struct link_queue {
	struct link *link;              /* Link written to, owned by the caller. */
	struct link_queue_entry *head;  /* Next entry to write, or NULL when the queue is empty. */
	struct link_queue_entry *tail;  /* Last entry, where pushes go. */
	timestamp_t last_progress;      /* When the queue last wrote a byte, or became non-empty. */
	int failed;                     /* A write or file read failed, so the queue drops everything. */
	int sealed;                     /* The queue refuses new data, and writes what it holds. */
};

struct link_queue *link_queue_create(struct link *link)
{
	struct link_queue *q = calloc(1, sizeof(*q));
	if (!q) {
		return NULL;
	}
	q->link = link;
	return q;
}

static void entry_delete(struct link_queue_entry *e)
{
	if (e->fd >= 0) {
		close(e->fd);
	}
	free(e->data);
	free(e->path);
	free(e);
}

static void drop_entries(struct link_queue *q)
{
	struct link_queue_entry *e = q->head;
	while (e) {
		struct link_queue_entry *next = e->next;
		entry_delete(e);
		e = next;
	}
	q->head = q->tail = NULL;
}

void link_queue_delete(struct link_queue *q)
{
	if (!q) {
		return;
	}
	drop_entries(q);
	free(q);
}

static void fail(struct link_queue *q)
{
	q->failed = 1;
	drop_entries(q);
}

static struct link_queue_entry *entry_create(void)
{
	struct link_queue_entry *e = calloc(1, sizeof(*e));
	if (!e) {
		return NULL;
	}
	e->fd = -1;
	return e;
}

static void append(struct link_queue *q, struct link_queue_entry *e)
{
	if (!q->head) {
		q->head = e;
		q->last_progress = timestamp_get();
	} else {
		q->tail->next = e;
	}
	q->tail = e;
}

int link_queue_push(struct link_queue *q, const char *data, size_t length)
{
	if (q->failed || q->sealed) {
		return 0;
	}
	if (length == 0) {
		return 1;
	}

	/* Small consecutive pushes, such as the lines of a message, share an entry that no write has touched yet. */
	struct link_queue_entry *e = q->tail;
	if (!e || e->path || e->started || e->length + length > LINK_QUEUE_CHUNK) {
		e = entry_create();
		if (!e) {
			fail(q);
			return 0;
		}
		append(q, e);
	}

	if (e->length + length > e->capacity) {
		size_t capacity = e->capacity ? e->capacity : 256;
		while (capacity < e->length + length) {
			capacity *= 2;
		}
		char *data_grown = realloc(e->data, capacity);
		if (!data_grown) {
			fail(q);
			return 0;
		}
		e->data = data_grown;
		e->capacity = capacity;
	}
	memcpy(e->data + e->length, data, length);
	e->length += length;
	return 1;
}

int link_queue_push_file(struct link_queue *q, const char *path, int64_t offset, int64_t length)
{
	if (q->failed || q->sealed) {
		return 0;
	}
	if (length <= 0) {
		return 1;
	}

	struct link_queue_entry *e = entry_create();
	if (!e) {
		fail(q);
		return 0;
	}
	e->path = strdup(path);
	if (!e->path) {
		entry_delete(e);
		fail(q);
		return 0;
	}
	e->file_offset = offset;
	e->file_left = length;
	append(q, e);
	return 1;
}

/* Read the next chunk of a file entry into its data. Return zero if the file cannot be read or ends early. */
static int read_chunk(struct link_queue_entry *e)
{
	if (e->fd < 0) {
		e->fd = open(e->path, O_RDONLY);
		if (e->fd < 0) {
			debug(D_NOTICE, "cannot open %s to send it: %s", e->path, strerror(errno));
			return 0;
		}
	}
	if (!e->data) {
		e->data = malloc(LINK_QUEUE_CHUNK);
		if (!e->data) {
			return 0;
		}
		e->capacity = LINK_QUEUE_CHUNK;
	}

	size_t wanted = e->file_left < LINK_QUEUE_CHUNK ? (size_t)e->file_left : LINK_QUEUE_CHUNK;
	size_t got = 0;
	while (got < wanted) {
		ssize_t n = pread(e->fd, e->data + got, wanted - got, e->file_offset + got);
		if (n < 0 && errno == EINTR) {
			continue;
		}
		if (n <= 0) {
			debug(D_NOTICE, "cannot read %s to send it: %s", e->path, n < 0 ? strerror(errno) : "the file is shorter than announced");
			return 0;
		}
		got += n;
	}

	e->length = got;
	e->offset = 0;
	e->file_offset += got;
	e->file_left -= got;
	return 1;
}

static int entry_done(struct link_queue_entry *e)
{
	return e->offset == e->length && e->file_left == 0;
}

/* Write from the head entry once. Return the bytes written, zero if a write without waiting found no room, or -1 if
 * the queue failed. Set full when the link took less than it was offered. */
static ssize_t write_head(struct link_queue *q, time_t stoptime, int *full)
{
	struct link_queue_entry *e = q->head;
	if (e->offset == e->length && !read_chunk(e)) {
		fail(q);
		return -1;
	}

	e->started = 1;
	size_t offered = e->length - e->offset;
	ssize_t n = link_write(q->link, e->data + e->offset, offered, stoptime);
	if (n > 0) {
		*full = (size_t)n < offered;
		e->offset += n;
		q->last_progress = timestamp_get();
		if (entry_done(e)) {
			q->head = e->next;
			if (!q->head) {
				q->tail = NULL;
			}
			entry_delete(e);
		}
		return n;
	}

	/* A write that may not wait reports a full socket as a timeout. */
	if (stoptime == LINK_NOWAIT && (errno == ETIMEDOUT || errno_is_temporary(errno))) {
		*full = 1;
		return 0;
	}
	fail(q);
	return -1;
}

int link_queue_send(struct link_queue *q, timestamp_t deadline)
{
	while (!q->failed && q->head) {
		int full = 0;
		if (write_head(q, LINK_NOWAIT, &full) < 0 || full || timestamp_get() >= deadline) {
			break;
		}
	}
	return !q->failed;
}

int link_queue_flush(struct link_queue *q, time_t stoptime)
{
	while (!q->failed && q->head) {
		int full = 0;
		if (write_head(q, stoptime, &full) <= 0) {
			fail(q);
			return 0;
		}
	}
	return !q->failed;
}

int link_queue_empty(struct link_queue *q)
{
	return q->head == NULL;
}

void link_queue_seal(struct link_queue *q)
{
	q->sealed = 1;
}

int link_queue_failed(struct link_queue *q)
{
	return q->failed;
}

int link_queue_stalled(struct link_queue *q, int seconds)
{
	if (seconds <= 0 || !q->head) {
		return 0;
	}
	return timestamp_get() - q->last_progress >= (timestamp_t)seconds * 1000000;
}
