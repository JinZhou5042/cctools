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

/* Bytes of a file read per chunk, the size up to which consecutive small pushes share one entry, and the size up to
 * which entries that reach the head together are copied into one write. */
#define LINK_QUEUE_CHUNK (64 * 1024)

/* The kinds of queue entries. */
typedef enum {
	ENTRY_DATA,	/* Bytes copied into the entry. */
	ENTRY_FILE,	/* A byte range of a file, opened when the entry reaches the head. */
	ENTRY_FD,	/* A byte range of a file that was open when the entry was queued. */
	ENTRY_PRODUCER, /* A function that pushes the data ahead of the entry when it reaches the head. */
} entry_kind_t;

/* One entry of the queue. A data entry holds its bytes. A file entry holds the range still to read, and its current
 * chunk in data while it is at the head. A producer entry holds its function and argument. */
struct link_queue_entry {
	entry_kind_t kind;
	char *data;		       /* Bytes to write: all of a data entry, or the current chunk of a file. */
	size_t length;		       /* Number of bytes in data to write. */
	size_t capacity;	       /* Bytes allocated for data. */
	size_t offset;		       /* Bytes of data already written. */
	char *path;		       /* File of a file entry, or NULL. */
	int fd;			       /* Descriptor of the file of a file entry while it is at the head, or of an fd entry. */
	int64_t file_offset;	       /* Offset in the file of the next chunk to read. */
	int64_t file_left;	       /* Bytes of the range not yet read into a chunk. */
	link_queue_producer_t produce; /* Function of a producer entry. */
	void (*release)(void *arg);    /* Frees the argument of a producer entry, or NULL. */
	void *arg;		       /* Argument of a producer entry. */
	int started;		       /* A write was attempted, so the bytes stay in place and take no further pushes. */
	struct link_queue_entry *next; /* The entry after this one, or NULL at the tail. */
};

/* The queue is a singly linked list of entries, written from the head. While a producer at the head runs, pushes go
 * to a list of their own, which then goes ahead of the producer. */
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

static void drop_entries(struct link_queue *q)
{
	delete_list(q->head);
	delete_list(q->made_head);
	q->head = q->tail = NULL;
	q->made_head = q->made_tail = NULL;
}

void link_queue_delete(struct link_queue *q)
{
	if (!q) {
		return;
	}
	drop_entries(q);
	free(q->spare);
	free(q);
}

static void fail(struct link_queue *q)
{
	q->failed = 1;
	drop_entries(q);
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

/* Check whether a queue takes a push now. */
static int accepts(struct link_queue *q)
{
	return !q->failed && (!q->sealed || q->producing);
}

/* Return the last entry that a push may join: the tail, or the last entry of the running producer. */
static struct link_queue_entry *last_entry(struct link_queue *q)
{
	return q->producing ? q->made_tail : q->tail;
}

/* Delete an entry that was written or copied, keeping the bytes of a data entry of at most LINK_QUEUE_CHUNK as the
 * queue's spare. The messages queued to a link between two writes usually fill one data entry, which then needs no
 * allocation: growing a new one from 256 bytes reallocated it about three times per batch of messages. */
static void entry_retire(struct link_queue *q, struct link_queue_entry *e)
{
	if (!q->spare && e->kind == ENTRY_DATA && e->data && e->capacity <= LINK_QUEUE_CHUNK) {
		q->spare = e->data;
		q->spare_capacity = e->capacity;
		e->data = NULL;
	}
	entry_delete(e);
}

/* Give an entry without bytes the queue's spare, if any. */
static void take_spare(struct link_queue *q, struct link_queue_entry *e)
{
	if (q->spare && !e->data) {
		e->data = q->spare;
		e->capacity = q->spare_capacity;
		q->spare = NULL;
		q->spare_capacity = 0;
	}
}

static void append(struct link_queue *q, struct link_queue_entry *e)
{
	if (q->producing) {
		if (!q->made_head) {
			q->made_head = e;
		} else {
			q->made_tail->next = e;
		}
		q->made_tail = e;
		return;
	}
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
	if (!accepts(q)) {
		return 0;
	}
	if (length == 0) {
		return 1;
	}

	/* Small consecutive pushes, such as the lines of a message, share an entry that no write has touched yet. */
	struct link_queue_entry *e = last_entry(q);
	if (!e || e->kind != ENTRY_DATA || e->started || e->length + length > LINK_QUEUE_CHUNK) {
		e = entry_create(ENTRY_DATA);
		if (!e) {
			fail(q);
			return 0;
		}
		append(q, e);
	}

	take_spare(q, e);
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
	if (!accepts(q)) {
		return 0;
	}
	if (length <= 0) {
		return 1;
	}

	struct link_queue_entry *e = entry_create(ENTRY_FILE);
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

int link_queue_push_fd(struct link_queue *q, int fd, int64_t offset, int64_t length)
{
	if (!accepts(q) || length <= 0) {
		close(fd);
		return accepts(q);
	}

	struct link_queue_entry *e = entry_create(ENTRY_FD);
	if (!e) {
		close(fd);
		fail(q);
		return 0;
	}
	e->fd = fd;
	e->file_offset = offset;
	e->file_left = length;
	append(q, e);
	return 1;
}

int link_queue_push_producer(struct link_queue *q, link_queue_producer_t produce, void (*release)(void *arg), void *arg)
{
	if (!accepts(q)) {
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
		fail(q);
		return 0;
	}
	e->produce = produce;
	e->release = release;
	e->arg = arg;
	append(q, e);
	return 1;
}

/* Name the file of a file or fd entry in messages. */
static const char *entry_name(struct link_queue_entry *e)
{
	return e->path ? e->path : "an open file";
}

/* Read length bytes at an offset of an open file into data. Return zero if the file cannot be read or ends early. */
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

/* Open the file of a file entry, unless it is open already. Return zero on failure. */
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

/* Read the next chunk of a file or fd entry into its data. Return zero if the file cannot be read or ends early. */
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

/* Return the number of bytes of an entry that no write has touched. */
static size_t entry_size(struct link_queue_entry *e)
{
	return e->kind == ENTRY_DATA ? e->length : (size_t)e->file_left;
}

/* Copy the bytes of an entry that no write has touched into data. Return zero if a file cannot be read in full. */
static int entry_copy(struct link_queue_entry *e, char *data)
{
	if (e->kind == ENTRY_DATA) {
		memcpy(data, e->data, e->length);
		return 1;
	}
	return entry_open(e) && read_range(e->fd, entry_name(e), data, e->file_left, e->file_offset);
}

/* Replace the untouched entries at the head that fit in one chunk by one data entry with a copy of their bytes, so that
 * entries that pushes could not merge, such as small files between the lines of messages, leave in one write instead
 * of one each. A producer ends the run, because its data does not exist yet. A single entry is left as it is, so its
 * bytes are not copied for nothing. The copy lives only while it is at the head, so a queue holds at most one. Return
 * zero if a file cannot be read in full. */
static int gather_head(struct link_queue *q)
{
	struct link_queue_entry *end = q->head;
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
	if (q->spare && q->spare_capacity >= total) {
		take_spare(q, gathered);
	} else {
		gathered->data = malloc(total);
		if (!gathered->data) {
			entry_delete(gathered);
			return 0;
		}
		gathered->capacity = total;
	}
	while (q->head != end) {
		struct link_queue_entry *e = q->head;
		if (!entry_copy(e, gathered->data + gathered->length)) {
			entry_delete(gathered);
			return 0;
		}
		gathered->length += entry_size(e);
		q->head = e->next;
		entry_retire(q, e);
	}
	gathered->next = end;
	q->head = gathered;
	if (!end) {
		q->tail = gathered;
	}
	return 1;
}

/* Remove the head entry. */
static void pop_head(struct link_queue *q)
{
	struct link_queue_entry *e = q->head;
	q->head = e->next;
	if (!q->head) {
		q->tail = NULL;
	}
	entry_retire(q, e);
}

/* Run the producers at the head until the head holds data or the queue is empty. Return zero if a producer failed. */
static int produce_head(struct link_queue *q)
{
	while (q->head && q->head->kind == ENTRY_PRODUCER) {
		struct link_queue_entry *p = q->head;
		q->producing = 1;
		int result = p->produce(q, p->arg);
		q->producing = 0;

		struct link_queue_entry *made = q->made_head;
		struct link_queue_entry *made_tail = q->made_tail;
		q->made_head = q->made_tail = NULL;
		if (q->failed) {
			delete_list(made);
			return 0;
		}
		if (result < 0 || (result > 0 && !made)) {
			if (result > 0) {
				debug(D_NOTICE, "a producer of a link queue reported more data but pushed none");
			}
			delete_list(made);
			fail(q);
			return 0;
		}
		if (result == 0) {
			pop_head(q);
		}
		if (made) {
			made_tail->next = q->head;
			if (!q->head) {
				q->tail = made_tail;
			}
			q->head = made;
		}
	}
	return 1;
}

static int entry_done(struct link_queue_entry *e)
{
	return e->offset == e->length && e->file_left == 0;
}

/* Write from the head entry once. Return the bytes written, zero if a write without waiting found no room or a
 * producer left the queue empty, or -1 if the queue failed. Set full when the link took less than it was offered. */
static ssize_t write_head(struct link_queue *q, time_t stoptime, int *full)
{
	if (!produce_head(q)) {
		return -1;
	}
	if (!q->head) {
		return 0;
	}
	if (!q->head->started && !gather_head(q)) {
		fail(q);
		return -1;
	}
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
			pop_head(q);
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
		ssize_t n = write_head(q, stoptime, &full);
		if (n < 0 || (n == 0 && q->head)) {
			fail(q);
			return 0;
		}
	}
	return !q->failed;
}

int link_queue_finish(struct link_queue *q, time_t stoptime)
{
	struct link_queue_entry *e = q->head;
	while (!q->failed && e && q->head == e && e->started) {
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
