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

/* Size of a file chunk, of merged small pushes, and of a gathered write. */
#define LINK_QUEUE_CHUNK (64 * 1024)

typedef enum {
	ENTRY_DATA,	/* Bytes copied into the entry. */
	ENTRY_FILE,	/* A byte range of a file, given by path and opened at the head, or given open. */
	ENTRY_PRODUCER, /* A function that pushes the data ahead of the entry when it reaches the head. */
} entry_kind_t;

/* A data entry holds its bytes, a file entry its range and current chunk, and a producer entry its function. */
struct link_queue_entry {
	entry_kind_t kind;
	char *data;		       /* Bytes to write: all of a data entry, or the current chunk of a file. */
	size_t length;		       /* Number of bytes in data to write. */
	size_t capacity;	       /* Bytes allocated for data. */
	size_t offset;		       /* Bytes of data already written. */
	char *path;		       /* File of a file entry, or NULL. */
	int fd;			       /* Descriptor of a file entry: open from the push, or from when its path reaches the head. */
	int64_t file_offset;	       /* Offset in the file of the next chunk to read. */
	int64_t file_left;	       /* Bytes of the range not yet read into a chunk. */
	link_queue_producer_t produce; /* Function of a producer entry. */
	void (*release)(void *arg);    /* Frees the argument of a producer entry, or NULL. */
	void *arg;		       /* Argument of a producer entry. */
	int started;		       /* A write was attempted, so the bytes stay in place and take no further pushes. */
	struct link_queue_entry *next; /* The entry after this one, or NULL at the tail. */
};

/* A list of entries written from the head. A running producer's pushes go to the made list, then ahead of it. */
struct link_queue {
	struct link *link;		    /* Link written to, owned by the caller. */
	struct link_queue_entry *head;	    /* Next entry to write, or NULL when the queue is empty. */
	struct link_queue_entry *tail;	    /* Last entry, where pushes go. */
	struct link_queue_entry *made_head; /* First entry pushed by the running producer, or NULL. */
	struct link_queue_entry *made_tail; /* Last entry pushed by the running producer, or NULL. */
	int producing;			    /* A producer runs, so pushes go to the made list, even into a sealed queue. */
	timestamp_t last_progress;	    /* When the queue last wrote a byte, or became non-empty. */
	int failed;			    /* A write, file read, or producer failed, so the queue drops everything. */
	int sealed;			    /* The queue refuses new data, and writes what it holds. */
	char *spare;			    /* Bytes of the last data entry written, kept for the next one, or NULL. */
	size_t spare_capacity;		    /* Bytes allocated for spare. */
};

struct link_queue *link_queue_create(struct link *link)
{
	struct link_queue *lq = calloc(1, sizeof(*lq));
	if (!lq) {
		return NULL;
	}
	lq->link = link;
	return lq;
}

static void entry_delete(struct link_queue_entry *e)
{
	if (e->fd >= 0) {
		close(e->fd);
	}
	if (e->release) {
		e->release(e->arg);
	}
	free(e->data);
	free(e->path);
	free(e);
}

static void delete_list(struct link_queue_entry *e)
{
	while (e) {
		struct link_queue_entry *next = e->next;
		entry_delete(e);
		e = next;
	}
}

static void drop_entries(struct link_queue *lq)
{
	delete_list(lq->head);
	delete_list(lq->made_head);
	lq->head = lq->tail = NULL;
	lq->made_head = lq->made_tail = NULL;
}

void link_queue_delete(struct link_queue *lq)
{
	if (!lq) {
		return;
	}
	drop_entries(lq);
	free(lq->spare);
	free(lq);
}

static void fail(struct link_queue *lq)
{
	lq->failed = 1;
	drop_entries(lq);
}

static struct link_queue_entry *entry_create(entry_kind_t kind)
{
	struct link_queue_entry *e = calloc(1, sizeof(*e));
	if (!e) {
		return NULL;
	}
	e->kind = kind;
	e->fd = -1;
	return e;
}

static int accepts(struct link_queue *lq)
{
	return !lq->failed && (!lq->sealed || lq->producing);
}

static struct link_queue_entry *last_entry(struct link_queue *lq)
{
	return lq->producing ? lq->made_tail : lq->tail;
}

/* Delete an entry, keeping a small data buffer as the spare for the next one, which saves allocations per batch of
 * messages. */
static void entry_retire(struct link_queue *lq, struct link_queue_entry *e)
{
	if (!lq->spare && e->kind == ENTRY_DATA && e->data && e->capacity <= LINK_QUEUE_CHUNK) {
		lq->spare = e->data;
		lq->spare_capacity = e->capacity;
		e->data = NULL;
	}
	entry_delete(e);
}

static void take_spare(struct link_queue *lq, struct link_queue_entry *e)
{
	if (lq->spare && !e->data) {
		e->data = lq->spare;
		e->capacity = lq->spare_capacity;
		lq->spare = NULL;
		lq->spare_capacity = 0;
	}
}

static void append(struct link_queue *lq, struct link_queue_entry *e)
{
	if (lq->producing) {
		if (!lq->made_head) {
			lq->made_head = e;
		} else {
			lq->made_tail->next = e;
		}
		lq->made_tail = e;
		return;
	}
	if (!lq->head) {
		lq->head = e;
		lq->last_progress = timestamp_get();
	} else {
		lq->tail->next = e;
	}
	lq->tail = e;
}

int link_queue_push(struct link_queue *lq, const char *data, size_t length)
{
	if (!accepts(lq)) {
		return 0;
	}
	if (length == 0) {
		return 1;
	}

	/* Small consecutive pushes, such as the lines of a message, share an entry that no write has touched yet. */
	struct link_queue_entry *e = last_entry(lq);
	if (!e || e->kind != ENTRY_DATA || e->started || e->length + length > LINK_QUEUE_CHUNK) {
		e = entry_create(ENTRY_DATA);
		if (!e) {
			fail(lq);
			return 0;
		}
		append(lq, e);
	}

	take_spare(lq, e);
	if (e->length + length > e->capacity) {
		size_t capacity = e->capacity ? e->capacity : 256;
		while (capacity < e->length + length) {
			capacity *= 2;
		}
		char *data_grown = realloc(e->data, capacity);
		if (!data_grown) {
			fail(lq);
			return 0;
		}
		e->data = data_grown;
		e->capacity = capacity;
	}
	memcpy(e->data + e->length, data, length);
	e->length += length;
	return 1;
}

int link_queue_push_file(struct link_queue *lq, const char *path, int64_t offset, int64_t length)
{
	if (!accepts(lq)) {
		return 0;
	}
	if (length <= 0) {
		return 1;
	}

	struct link_queue_entry *e = entry_create(ENTRY_FILE);
	if (!e) {
		fail(lq);
		return 0;
	}
	e->path = strdup(path);
	if (!e->path) {
		entry_delete(e);
		fail(lq);
		return 0;
	}
	e->file_offset = offset;
	e->file_left = length;
	append(lq, e);
	return 1;
}

int link_queue_push_fd(struct link_queue *lq, int fd, int64_t offset, int64_t length)
{
	if (!accepts(lq) || length <= 0) {
		close(fd);
		return accepts(lq);
	}

	struct link_queue_entry *e = entry_create(ENTRY_FILE);
	if (!e) {
		close(fd);
		fail(lq);
		return 0;
	}
	e->fd = fd;
	e->file_offset = offset;
	e->file_left = length;
	append(lq, e);
	return 1;
}

int link_queue_push_producer(struct link_queue *lq, link_queue_producer_t produce, void (*release)(void *arg), void *arg)
{
	if (!accepts(lq)) {
		if (release) {
			release(arg);
		}
		return 0;
	}

	struct link_queue_entry *e = entry_create(ENTRY_PRODUCER);
	if (!e) {
		if (release) {
			release(arg);
		}
		fail(lq);
		return 0;
	}
	e->produce = produce;
	e->release = release;
	e->arg = arg;
	append(lq, e);
	return 1;
}

static const char *entry_name(struct link_queue_entry *e)
{
	return e->path ? e->path : "an open file";
}

/* Return zero if the file cannot be read or ends early. */
static int read_range(int fd, const char *name, char *data, size_t length, int64_t offset)
{
	size_t got = 0;
	while (got < length) {
		ssize_t n = pread(fd, data + got, length - got, offset + got);
		if (n < 0 && errno == EINTR) {
			continue;
		}
		if (n <= 0) {
			debug(D_NOTICE, "cannot read %s to send it: %s", name, n < 0 ? strerror(errno) : "the file is shorter than announced");
			return 0;
		}
		got += n;
	}
	return 1;
}

static int entry_open(struct link_queue_entry *e)
{
	if (e->fd >= 0) {
		return 1;
	}
	e->fd = open(e->path, O_RDONLY);
	if (e->fd < 0) {
		debug(D_NOTICE, "cannot open %s to send it: %s", e->path, strerror(errno));
		return 0;
	}
	return 1;
}

static int read_chunk(struct link_queue_entry *e)
{
	if (!entry_open(e)) {
		return 0;
	}
	if (!e->data) {
		e->data = malloc(LINK_QUEUE_CHUNK);
		if (!e->data) {
			return 0;
		}
		e->capacity = LINK_QUEUE_CHUNK;
	}

	size_t wanted = e->file_left < LINK_QUEUE_CHUNK ? (size_t)e->file_left : LINK_QUEUE_CHUNK;
	if (!read_range(e->fd, entry_name(e), e->data, wanted, e->file_offset)) {
		return 0;
	}

	e->length = wanted;
	e->offset = 0;
	e->file_offset += wanted;
	e->file_left -= wanted;
	return 1;
}

static size_t entry_size(struct link_queue_entry *e)
{
	return e->kind == ENTRY_DATA ? e->length : (size_t)e->file_left;
}

static int entry_copy(struct link_queue_entry *e, char *data)
{
	if (e->kind == ENTRY_DATA) {
		memcpy(data, e->data, e->length);
		return 1;
	}
	return entry_open(e) && read_range(e->fd, entry_name(e), data, e->file_left, e->file_offset);
}

/* Copy the untouched entries at the head that fit in one chunk into one data entry, so small files between message
 * lines leave in one write. A producer ends the run. */
static int gather_head(struct link_queue *lq)
{
	struct link_queue_entry *end = lq->head;
	size_t total = 0;
	int count = 0;
	while (end && !end->started && end->kind != ENTRY_PRODUCER && total + entry_size(end) <= LINK_QUEUE_CHUNK) {
		total += entry_size(end);
		count++;
		end = end->next;
	}
	if (count < 2) {
		return 1;
	}

	struct link_queue_entry *gathered = entry_create(ENTRY_DATA);
	if (!gathered) {
		return 0;
	}
	if (lq->spare && lq->spare_capacity >= total) {
		take_spare(lq, gathered);
	} else {
		gathered->data = malloc(total);
		if (!gathered->data) {
			entry_delete(gathered);
			return 0;
		}
		gathered->capacity = total;
	}
	while (lq->head != end) {
		struct link_queue_entry *e = lq->head;
		if (!entry_copy(e, gathered->data + gathered->length)) {
			entry_delete(gathered);
			return 0;
		}
		gathered->length += entry_size(e);
		lq->head = e->next;
		entry_retire(lq, e);
	}
	gathered->next = end;
	lq->head = gathered;
	if (!end) {
		lq->tail = gathered;
	}
	return 1;
}

static void pop_head(struct link_queue *lq)
{
	struct link_queue_entry *e = lq->head;
	lq->head = e->next;
	if (!lq->head) {
		lq->tail = NULL;
	}
	entry_retire(lq, e);
}

/* Run the producers at the head until the head holds data or the queue is empty. Return zero if a producer failed. */
static int produce_head(struct link_queue *lq)
{
	while (lq->head && lq->head->kind == ENTRY_PRODUCER) {
		struct link_queue_entry *p = lq->head;
		lq->producing = 1;
		int result = p->produce(lq, p->arg);
		lq->producing = 0;

		struct link_queue_entry *made = lq->made_head;
		struct link_queue_entry *made_tail = lq->made_tail;
		lq->made_head = lq->made_tail = NULL;
		if (lq->failed) {
			delete_list(made);
			return 0;
		}
		if (result < 0 || (result > 0 && !made)) {
			if (result > 0) {
				debug(D_NOTICE, "a producer of a link queue reported more data but pushed none");
			}
			delete_list(made);
			fail(lq);
			return 0;
		}
		if (result == 0) {
			pop_head(lq);
		}
		if (made) {
			made_tail->next = lq->head;
			if (!lq->head) {
				lq->tail = made_tail;
			}
			lq->head = made;
		}
	}
	return 1;
}

static int entry_done(struct link_queue_entry *e)
{
	return e->offset == e->length && e->file_left == 0;
}

/* Write from the head entry once. Return the bytes written, zero if there was no room or nothing to write, or -1 if
 * the queue failed. Set full when the link took less than offered. */
static ssize_t write_head(struct link_queue *lq, time_t stoptime, int *full)
{
	if (!produce_head(lq)) {
		return -1;
	}
	if (!lq->head) {
		return 0;
	}
	if (!lq->head->started && !gather_head(lq)) {
		fail(lq);
		return -1;
	}
	struct link_queue_entry *e = lq->head;
	if (e->offset == e->length && !read_chunk(e)) {
		fail(lq);
		return -1;
	}

	e->started = 1;
	size_t offered = e->length - e->offset;
	ssize_t n = link_write(lq->link, e->data + e->offset, offered, stoptime);
	if (n > 0) {
		*full = (size_t)n < offered;
		e->offset += n;
		lq->last_progress = timestamp_get();
		if (entry_done(e)) {
			pop_head(lq);
		}
		return n;
	}

	/* A write that may not wait reports a full socket as a timeout. */
	if (stoptime == LINK_NOWAIT && (errno == ETIMEDOUT || errno_is_temporary(errno))) {
		*full = 1;
		return 0;
	}
	fail(lq);
	return -1;
}

int link_queue_send(struct link_queue *lq, timestamp_t deadline)
{
	while (!lq->failed && lq->head) {
		int full = 0;
		if (write_head(lq, LINK_NOWAIT, &full) < 0 || full || timestamp_get() >= deadline) {
			break;
		}
	}
	return !lq->failed;
}

/* The stoptime of link_write for a deadline, rounded up so the deadline is never shortened. */
static time_t stoptime_of(timestamp_t deadline)
{
	return (time_t)((deadline + 999999) / 1000000);
}

int link_queue_flush(struct link_queue *lq, timestamp_t deadline)
{
	time_t stoptime = stoptime_of(deadline);
	while (!lq->failed && lq->head) {
		int full = 0;
		ssize_t n = write_head(lq, stoptime, &full);
		if (n < 0 || (n == 0 && lq->head)) {
			fail(lq);
			return 0;
		}
	}
	return !lq->failed;
}

int link_queue_finish(struct link_queue *lq, timestamp_t deadline)
{
	time_t stoptime = stoptime_of(deadline);
	struct link_queue_entry *e = lq->head;
	while (!lq->failed && e && lq->head == e && e->started) {
		int full = 0;
		if (write_head(lq, stoptime, &full) <= 0) {
			fail(lq);
			return 0;
		}
	}
	return !lq->failed;
}

int link_queue_empty(struct link_queue *lq)
{
	return lq->head == NULL;
}

void link_queue_seal(struct link_queue *lq)
{
	lq->sealed = 1;
}

int link_queue_failed(struct link_queue *lq)
{
	return lq->failed;
}

int link_queue_stalled(struct link_queue *lq, int seconds)
{
	if (seconds <= 0 || !lq->head) {
		return 0;
	}
	return timestamp_get() - lq->last_progress >= (timestamp_t)seconds * 1000000;
}
