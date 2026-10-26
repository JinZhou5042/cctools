/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "link_writer.h"
#include "debug.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Size of a file chunk, of merged small pushes, and of a gathered write. */
#define LINK_WRITER_CHUNK (64 * 1024)

typedef enum {
	ENTRY_DATA,	/* Bytes copied into the entry. */
	ENTRY_FILE,	/* A byte range of a file, given by path and opened at the head, or given open. */
	ENTRY_PRODUCER, /* A function that pushes the data ahead of the entry when it reaches the head. */
} entry_kind_t;

/* A data entry holds its bytes, a file entry its range and current chunk, and a producer entry its function. */
struct link_writer_entry {
	entry_kind_t kind;
	char *data;		       /* Bytes to write: all of a data entry, or the current chunk of a file. */
	size_t length;		       /* Number of bytes in data to write. */
	size_t capacity;	       /* Bytes allocated for data. */
	size_t offset;		       /* Bytes of data already written. */
	char *path;		       /* File of a file entry, or NULL. */
	int fd;			       /* Descriptor of a file entry: open from the push, or from when its path reaches the head. */
	int64_t file_offset;	       /* Offset in the file of the next chunk to read. */
	int64_t file_left;	       /* Bytes of the range not yet read into a chunk. */
	link_writer_producer_t produce; /* Function of a producer entry. */
	void (*release)(void *arg);    /* Frees the argument of a producer entry, or NULL. */
	void *arg;		       /* Argument of a producer entry. */
	int started;		       /* A write was attempted, so the bytes stay in place and take no further pushes. */
	struct link_writer_entry *next; /* The entry after this one, or NULL at the tail. */
};

/* A list of entries written from the head. A running producer's pushes go to the made list, then ahead of it. */
struct link_writer {
	struct link *link;		    /* Link written to, owned by the caller. */
	struct link_writer_entry *head;	    /* Next entry to write, or NULL when the writer is empty. */
	struct link_writer_entry *tail;	    /* Last entry, where pushes go. */
	struct link_writer_entry *made_head; /* First entry pushed by the running producer, or NULL. */
	struct link_writer_entry *made_tail; /* Last entry pushed by the running producer, or NULL. */
	int producing;			    /* A producer runs, so pushes go to the made list, even into a sealed writer. */
	timestamp_t last_progress;	    /* When the writer last wrote a byte, or became non-empty. */
	int failed;			    /* A write, file read, or producer failed, so the writer drops everything. */
	int sealed;			    /* The writer refuses new data, and writes what it holds. */
	char *spare;			    /* Bytes of the last data entry written, kept for the next one, or NULL. */
	size_t spare_capacity;		    /* Bytes allocated for spare. */
};

struct link_writer *link_writer_create(struct link *link)
{
	struct link_writer *lw = calloc(1, sizeof(*lw));
	if (!lw) {
		return NULL;
	}
	lw->link = link;
	return lw;
}

static void entry_delete(struct link_writer_entry *e)
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

static void delete_list(struct link_writer_entry *e)
{
	while (e) {
		struct link_writer_entry *next = e->next;
		entry_delete(e);
		e = next;
	}
}

static void drop_entries(struct link_writer *lw)
{
	delete_list(lw->head);
	delete_list(lw->made_head);
	lw->head = lw->tail = NULL;
	lw->made_head = lw->made_tail = NULL;
}

void link_writer_delete(struct link_writer *lw)
{
	if (!lw) {
		return;
	}
	drop_entries(lw);
	free(lw->spare);
	free(lw);
}

static void fail(struct link_writer *lw)
{
	lw->failed = 1;
	drop_entries(lw);
}

static struct link_writer_entry *entry_create(entry_kind_t kind)
{
	struct link_writer_entry *e = calloc(1, sizeof(*e));
	if (!e) {
		return NULL;
	}
	e->kind = kind;
	e->fd = -1;
	return e;
}

static int accepts(struct link_writer *lw)
{
	return !lw->failed && (!lw->sealed || lw->producing);
}

static struct link_writer_entry *last_entry(struct link_writer *lw)
{
	return lw->producing ? lw->made_tail : lw->tail;
}

/* Delete an entry, keeping a small data buffer as the spare for the next one, which saves allocations per batch of
 * messages. */
static void entry_retire(struct link_writer *lw, struct link_writer_entry *e)
{
	if (!lw->spare && e->kind == ENTRY_DATA && e->data && e->capacity <= LINK_WRITER_CHUNK) {
		lw->spare = e->data;
		lw->spare_capacity = e->capacity;
		e->data = NULL;
	}
	entry_delete(e);
}

static void take_spare(struct link_writer *lw, struct link_writer_entry *e)
{
	if (lw->spare && !e->data) {
		e->data = lw->spare;
		e->capacity = lw->spare_capacity;
		lw->spare = NULL;
		lw->spare_capacity = 0;
	}
}

static void append(struct link_writer *lw, struct link_writer_entry *e)
{
	if (lw->producing) {
		if (!lw->made_head) {
			lw->made_head = e;
		} else {
			lw->made_tail->next = e;
		}
		lw->made_tail = e;
		return;
	}
	if (!lw->head) {
		lw->head = e;
		lw->last_progress = timestamp_get();
	} else {
		lw->tail->next = e;
	}
	lw->tail = e;
}

int link_writer_push(struct link_writer *lw, const char *data, size_t length)
{
	if (!accepts(lw)) {
		return 0;
	}
	if (length == 0) {
		return 1;
	}

	/* Small consecutive pushes, such as the lines of a message, share an entry that no write has touched yet. */
	struct link_writer_entry *e = last_entry(lw);
	if (!e || e->kind != ENTRY_DATA || e->started || e->length + length > LINK_WRITER_CHUNK) {
		e = entry_create(ENTRY_DATA);
		if (!e) {
			fail(lw);
			return 0;
		}
		append(lw, e);
	}

	take_spare(lw, e);
	if (e->length + length > e->capacity) {
		size_t capacity = e->capacity ? e->capacity : 256;
		while (capacity < e->length + length) {
			capacity *= 2;
		}
		char *data_grown = realloc(e->data, capacity);
		if (!data_grown) {
			fail(lw);
			return 0;
		}
		e->data = data_grown;
		e->capacity = capacity;
	}
	memcpy(e->data + e->length, data, length);
	e->length += length;
	return 1;
}

int link_writer_push_file(struct link_writer *lw, const char *path, int64_t offset, int64_t length)
{
	if (!accepts(lw)) {
		return 0;
	}
	if (length <= 0) {
		return 1;
	}

	struct link_writer_entry *e = entry_create(ENTRY_FILE);
	if (!e) {
		fail(lw);
		return 0;
	}
	e->path = strdup(path);
	if (!e->path) {
		entry_delete(e);
		fail(lw);
		return 0;
	}
	e->file_offset = offset;
	e->file_left = length;
	append(lw, e);
	return 1;
}

int link_writer_push_fd(struct link_writer *lw, int fd, int64_t offset, int64_t length)
{
	if (!accepts(lw) || length <= 0) {
		close(fd);
		return accepts(lw);
	}

	struct link_writer_entry *e = entry_create(ENTRY_FILE);
	if (!e) {
		close(fd);
		fail(lw);
		return 0;
	}
	e->fd = fd;
	e->file_offset = offset;
	e->file_left = length;
	append(lw, e);
	return 1;
}

int link_writer_push_producer(struct link_writer *lw, link_writer_producer_t produce, void (*release)(void *arg), void *arg)
{
	if (!accepts(lw)) {
		if (release) {
			release(arg);
		}
		return 0;
	}

	struct link_writer_entry *e = entry_create(ENTRY_PRODUCER);
	if (!e) {
		if (release) {
			release(arg);
		}
		fail(lw);
		return 0;
	}
	e->produce = produce;
	e->release = release;
	e->arg = arg;
	append(lw, e);
	return 1;
}

static const char *entry_name(struct link_writer_entry *e)
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

static int entry_open(struct link_writer_entry *e)
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

static int read_chunk(struct link_writer_entry *e)
{
	if (!entry_open(e)) {
		return 0;
	}
	if (!e->data) {
		e->data = malloc(LINK_WRITER_CHUNK);
		if (!e->data) {
			return 0;
		}
		e->capacity = LINK_WRITER_CHUNK;
	}

	size_t wanted = e->file_left < LINK_WRITER_CHUNK ? (size_t)e->file_left : LINK_WRITER_CHUNK;
	if (!read_range(e->fd, entry_name(e), e->data, wanted, e->file_offset)) {
		return 0;
	}

	e->length = wanted;
	e->offset = 0;
	e->file_offset += wanted;
	e->file_left -= wanted;
	return 1;
}

static size_t entry_size(struct link_writer_entry *e)
{
	return e->kind == ENTRY_DATA ? e->length : (size_t)e->file_left;
}

static int entry_copy(struct link_writer_entry *e, char *data)
{
	if (e->kind == ENTRY_DATA) {
		memcpy(data, e->data, e->length);
		return 1;
	}
	return entry_open(e) && read_range(e->fd, entry_name(e), data, e->file_left, e->file_offset);
}

/* Copy the untouched entries at the head that fit in one chunk into one data entry, so small files between message
 * lines leave in one write. A producer ends the run. */
static int gather_head(struct link_writer *lw)
{
	struct link_writer_entry *end = lw->head;
	size_t total = 0;
	int count = 0;
	while (end && !end->started && end->kind != ENTRY_PRODUCER && total + entry_size(end) <= LINK_WRITER_CHUNK) {
		total += entry_size(end);
		count++;
		end = end->next;
	}
	if (count < 2) {
		return 1;
	}

	struct link_writer_entry *gathered = entry_create(ENTRY_DATA);
	if (!gathered) {
		return 0;
	}
	if (lw->spare && lw->spare_capacity >= total) {
		take_spare(lw, gathered);
	} else {
		gathered->data = malloc(total);
		if (!gathered->data) {
			entry_delete(gathered);
			return 0;
		}
		gathered->capacity = total;
	}
	while (lw->head != end) {
		struct link_writer_entry *e = lw->head;
		if (!entry_copy(e, gathered->data + gathered->length)) {
			entry_delete(gathered);
			return 0;
		}
		gathered->length += entry_size(e);
		lw->head = e->next;
		entry_retire(lw, e);
	}
	gathered->next = end;
	lw->head = gathered;
	if (!end) {
		lw->tail = gathered;
	}
	return 1;
}

static void pop_head(struct link_writer *lw)
{
	struct link_writer_entry *e = lw->head;
	lw->head = e->next;
	if (!lw->head) {
		lw->tail = NULL;
	}
	entry_retire(lw, e);
}

/* Run the producers at the head until the head holds data or the writer is empty. Return zero if a producer failed. */
static int produce_head(struct link_writer *lw)
{
	while (lw->head && lw->head->kind == ENTRY_PRODUCER) {
		struct link_writer_entry *p = lw->head;
		lw->producing = 1;
		int result = p->produce(lw, p->arg);
		lw->producing = 0;

		struct link_writer_entry *made = lw->made_head;
		struct link_writer_entry *made_tail = lw->made_tail;
		lw->made_head = lw->made_tail = NULL;
		if (lw->failed) {
			delete_list(made);
			return 0;
		}
		if (result < 0 || (result > 0 && !made)) {
			if (result > 0) {
				debug(D_NOTICE, "a producer of a link writer reported more data but pushed none");
			}
			delete_list(made);
			fail(lw);
			return 0;
		}
		if (result == 0) {
			pop_head(lw);
		}
		if (made) {
			made_tail->next = lw->head;
			if (!lw->head) {
				lw->tail = made_tail;
			}
			lw->head = made;
		}
	}
	return 1;
}

static int entry_done(struct link_writer_entry *e)
{
	return e->offset == e->length && e->file_left == 0;
}

/* Write from the head entry once. Return the bytes written, zero if there was no room or nothing to write, or -1 if
 * the writer failed. Set full when the link took less than offered. */
static ssize_t write_head(struct link_writer *lw, time_t stoptime, int *full)
{
	if (!produce_head(lw)) {
		return -1;
	}
	if (!lw->head) {
		return 0;
	}
	if (!lw->head->started && !gather_head(lw)) {
		fail(lw);
		return -1;
	}
	struct link_writer_entry *e = lw->head;
	if (e->offset == e->length && !read_chunk(e)) {
		fail(lw);
		return -1;
	}

	e->started = 1;
	size_t offered = e->length - e->offset;
	ssize_t n = link_write(lw->link, e->data + e->offset, offered, stoptime);
	if (n > 0) {
		*full = (size_t)n < offered;
		e->offset += n;
		lw->last_progress = timestamp_get();
		if (entry_done(e)) {
			pop_head(lw);
		}
		return n;
	}

	/* A write that may not wait reports a full socket as a timeout. */
	if (stoptime == LINK_NOWAIT && (errno == ETIMEDOUT || errno_is_temporary(errno))) {
		*full = 1;
		return 0;
	}
	fail(lw);
	return -1;
}

int link_writer_send_async(struct link_writer *lw, timestamp_t deadline)
{
	while (!lw->failed && lw->head) {
		int full = 0;
		if (write_head(lw, LINK_NOWAIT, &full) < 0 || full || timestamp_get() >= deadline) {
			break;
		}
	}
	return !lw->failed;
}

/* The stoptime of link_write for a deadline, rounded up so the deadline is never shortened. */
static time_t stoptime_of(timestamp_t deadline)
{
	return (time_t)((deadline + 999999) / 1000000);
}

/* The stoptime of the next write: the deadline, or idle seconds from now if sooner, or never for neither. */
static time_t next_stoptime(timestamp_t deadline, int idle)
{
	time_t stoptime = deadline ? stoptime_of(deadline) : LINK_FOREVER;
	if (idle > 0 && time(NULL) + idle < stoptime) {
		stoptime = time(NULL) + idle;
	}
	return stoptime;
}

int link_writer_send_sync(struct link_writer *lw, timestamp_t deadline, int idle)
{
	while (!lw->failed && lw->head) {
		int full = 0;
		ssize_t n = write_head(lw, next_stoptime(deadline, idle), &full);
		if (n < 0 || (n == 0 && lw->head)) {
			fail(lw);
			return 0;
		}
	}
	return !lw->failed;
}

int link_writer_send_partial(struct link_writer *lw, timestamp_t deadline)
{
	time_t stoptime = stoptime_of(deadline);
	struct link_writer_entry *e = lw->head;
	while (!lw->failed && e && lw->head == e && e->started) {
		int full = 0;
		if (write_head(lw, stoptime, &full) <= 0) {
			fail(lw);
			return 0;
		}
	}
	return !lw->failed;
}

int link_writer_empty(struct link_writer *lw)
{
	return lw->head == NULL;
}

void link_writer_seal(struct link_writer *lw)
{
	lw->sealed = 1;
}

int link_writer_failed(struct link_writer *lw)
{
	return lw->failed;
}

timestamp_t link_writer_last_progress(struct link_writer *lw)
{
	return lw->last_progress;
}
