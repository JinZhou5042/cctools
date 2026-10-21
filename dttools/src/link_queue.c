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

/* Data shared by the queues that hold it, in memory, in a file, or both. Data in both is in a file now, and keeps its
 * bytes in memory only while queue entries write them in place, as an SSL retry requires. */
struct link_queue_shared {
	int refs;      /* References of the creator and of the queue entries that hold the data. */
	int writers;   /* Queue entries that write the bytes in memory in place. */
	char *data;    /* The bytes held in memory, or NULL. */
	size_t length; /* Number of bytes of the data. */
	char *path;    /* The file that holds the data from its start, or NULL. */
};

/* One entry of the queue. A data entry holds its bytes. A file entry holds the range still to read, and its current
 * chunk in data while it is at the head. A shared entry holds a reference to shared data: data in memory is written in
 * place, and data in a file is read like the range of a file entry. */
struct link_queue_entry {
	char *data;                       /* Bytes to write: all of a data entry, or the current chunk of a file. */
	size_t length;                    /* Number of bytes to write, in data or in place in shared data. */
	size_t capacity;                  /* Bytes allocated for data. */
	size_t offset;                    /* Bytes already written. */
	char *path;                       /* File of a file entry, or NULL. */
	struct link_queue_shared *shared; /* Shared data of a shared entry, or NULL. */
	int fd;                           /* Descriptor of the file while its entry is at the head, else -1. */
	int64_t file_offset;              /* Offset in the file of the next chunk to read. */
	int64_t file_left;                /* Bytes not yet read into a chunk, or not yet made ready in shared data. */
	int started;                      /* A write was attempted, so the bytes stay in place and take no further pushes. */
	int writing;                      /* The entry writes shared bytes in place, counted in their writers. */
	struct link_queue_entry *next;    /* The entry after this one, or NULL at the tail. */
};

/* The queue is a singly linked list of entries, written from the head. */
struct link_queue {
	struct link *link;              /* Link written to, owned by the caller. */
	struct link_queue_entry *head;  /* Next entry to write, or NULL when the queue is empty. */
	struct link_queue_entry *tail;  /* Last entry, where pushes go. */
	timestamp_t last_progress;      /* When the queue last wrote a byte, or became non-empty. */
	int failed;                     /* A write or file read failed, so the queue drops everything. */
	int sealed;                     /* The queue refuses new data, and writes what it holds. */
	char *spare;                    /* Bytes of the last data entry written, kept for the next one, or NULL. */
	size_t spare_capacity;          /* Bytes allocated for spare. */
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
	if (e->writing && --e->shared->writers == 0 && e->shared->path) {
		/* The data is in its file now, and nobody writes the bytes in place anymore. */
		free(e->shared->data);
		e->shared->data = NULL;
	}
	free(e->data);
	free(e->path);
	link_queue_shared_release(e->shared);
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
	free(q->spare);
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

/* Delete an entry that was written or copied, keeping the bytes of a data entry of at most LINK_QUEUE_CHUNK as the
 * queue's spare. The messages queued to a link between two writes usually fill one data entry, which then needs no
 * allocation: growing a new one from 256 bytes reallocated it about three times per batch of messages. */
static void entry_retire(struct link_queue *q, struct link_queue_entry *e)
{
	if (!q->spare && e->data && !e->path && !e->shared && e->capacity <= LINK_QUEUE_CHUNK) {
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
	if (!q->head) {
		q->head = e;
		q->last_progress = timestamp_monotonic();
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
	if (!e || e->path || e->shared || e->started || e->length + length > LINK_QUEUE_CHUNK) {
		e = entry_create();
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

struct link_queue_shared *link_queue_shared_create(char *data, size_t length)
{
	struct link_queue_shared *s = calloc(1, sizeof(*s));
	if (!s) {
		return NULL;
	}
	s->refs = 1;
	s->data = data;
	s->length = length;
	return s;
}

struct link_queue_shared *link_queue_shared_create_file(const char *path, size_t length)
{
	struct link_queue_shared *s = calloc(1, sizeof(*s));
	if (!s) {
		return NULL;
	}
	s->path = strdup(path);
	if (!s->path) {
		free(s);
		return NULL;
	}
	s->refs = 1;
	s->length = length;
	return s;
}

struct link_queue_shared *link_queue_shared_hold(struct link_queue_shared *s)
{
	s->refs++;
	return s;
}

void link_queue_shared_release(struct link_queue_shared *s)
{
	if (!s || --s->refs > 0) {
		return;
	}
	free(s->data);
	free(s->path);
	free(s);
}

const char *link_queue_shared_bytes(const struct link_queue_shared *s)
{
	return s->data;
}

/* Read length bytes at an offset of an open file into data. Return zero if the file cannot be read or ends early. */
static int read_range(int fd, const char *path, char *data, size_t length, int64_t offset)
{
	size_t got = 0;
	while (got < length) {
		ssize_t n = pread(fd, data + got, length - got, offset + got);
		if (n < 0 && errno == EINTR) {
			continue;
		}
		if (n <= 0) {
			debug(D_NOTICE, "cannot read %s to send it: %s", path, n < 0 ? strerror(errno) : "the file is shorter than announced");
			return 0;
		}
		got += n;
	}
	return 1;
}

/* Read length bytes at an offset of a file into data, opening and closing the file. Return zero on failure. */
static int read_file(const char *path, char *data, size_t length, int64_t offset)
{
	int fd = open(path, O_RDONLY);
	if (fd < 0) {
		debug(D_NOTICE, "cannot open %s to send it: %s", path, strerror(errno));
		return 0;
	}
	int result = read_range(fd, path, data, length, offset);
	close(fd);
	return result;
}

int link_queue_shared_unload(struct link_queue_shared *s, const char *path)
{
	if (!s->data || s->path) {
		return 1;
	}
	s->path = strdup(path);
	if (!s->path) {
		return 0;
	}
	if (s->writers == 0) {
		free(s->data);
		s->data = NULL;
	}
	return 1;
}

int link_queue_shared_load(struct link_queue_shared *s)
{
	if (s->refs <= 1) {
		return 1;
	}
	if (s->data) {
		/* Writers still keep the bytes in memory, so they stay there and the file is no longer needed. */
		free(s->path);
		s->path = NULL;
		return 1;
	}
	char *data = malloc(s->length);
	if (!data) {
		return 0;
	}
	if (!read_file(s->path, data, s->length, 0)) {
		free(data);
		return 0;
	}
	s->data = data;
	free(s->path);
	s->path = NULL;
	return 1;
}

int link_queue_push_shared(struct link_queue *q, struct link_queue_shared *s)
{
	if (q->failed || q->sealed) {
		return 0;
	}
	if (s->length == 0) {
		return 1;
	}

	struct link_queue_entry *e = entry_create();
	if (!e) {
		fail(q);
		return 0;
	}
	e->shared = link_queue_shared_hold(s);
	e->file_left = s->length;
	append(q, e);
	return 1;
}

/* Make the next bytes of a file entry or a shared entry ready to write. Shared data in memory is written in place,
 * unless its entry started reading the file before the data was loaded. A file is read in chunks into data. Return
 * zero if the file cannot be read or ends early. */
static int read_chunk(struct link_queue_entry *e)
{
	if (e->shared && e->shared->data && !e->data) {
		e->length = e->file_left;
		e->offset = 0;
		e->file_left = 0;
		e->writing = 1;
		e->shared->writers++;
		return 1;
	}
	/* Shared data loaded while its entry was reading the file has no path anymore, but the open file still reads. */
	const char *path = e->path ? e->path : (e->shared->path ? e->shared->path : "loaded shared data");
	if (e->fd < 0) {
		e->fd = open(path, O_RDONLY);
		if (e->fd < 0) {
			debug(D_NOTICE, "cannot open %s to send it: %s", path, strerror(errno));
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
	if (!read_range(e->fd, path, e->data, wanted, e->file_offset)) {
		return 0;
	}

	e->length = wanted;
	e->offset = 0;
	e->file_offset += wanted;
	e->file_left -= wanted;
	return 1;
}

/* Return the bytes that read_chunk() made ready, or all bytes of a data entry. */
static const char *entry_bytes(struct link_queue_entry *e)
{
	return e->data ? e->data : e->shared->data;
}

/* Return the number of bytes of an entry that no write has touched. */
static size_t entry_size(struct link_queue_entry *e)
{
	return e->path || e->shared ? (size_t)e->file_left : e->length;
}

/* Copy the bytes of an entry that no write has touched into data. Return zero if a file cannot be read in full. */
static int entry_copy(struct link_queue_entry *e, char *data)
{
	size_t length = entry_size(e);
	if (e->shared && e->shared->data) {
		memcpy(data, e->shared->data, length);
		return 1;
	} else if (e->path || e->shared) {
		return read_file(e->path ? e->path : e->shared->path, data, length, e->file_offset);
	}
	memcpy(data, e->data, length);
	return 1;
}

/* Replace the untouched entries at the head that fit in one chunk by one data entry with a copy of their bytes, so that
 * entries that pushes could not merge, such as small files or shared data between the lines of messages, leave in one
 * write instead of one each. A single entry is left as it is, so its bytes are not copied for nothing. The copy lives
 * only while it is at the head, so a queue holds at most one. Return zero if a file cannot be read in full. */
static int gather_head(struct link_queue *q)
{
	struct link_queue_entry *end = q->head;
	size_t total = 0;
	int count = 0;
	while (end && !end->started && total + entry_size(end) <= LINK_QUEUE_CHUNK) {
		total += entry_size(end);
		count++;
		end = end->next;
	}
	if (count < 2) {
		return 1;
	}

	struct link_queue_entry *gathered = entry_create();
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

static int entry_done(struct link_queue_entry *e)
{
	return e->offset == e->length && e->file_left == 0;
}

/* Write from the head entry once. Return the bytes written, zero if a write without waiting found no room, or -1 if
 * the queue failed. Set full when the link took less than it was offered. */
static ssize_t write_head(struct link_queue *q, time_t stoptime, int *full)
{
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
	ssize_t n = link_write(q->link, entry_bytes(e) + e->offset, offered, stoptime);
	if (n > 0) {
		*full = (size_t)n < offered;
		e->offset += n;
		q->last_progress = timestamp_monotonic();
		if (entry_done(e)) {
			q->head = e->next;
			if (!q->head) {
				q->tail = NULL;
			}
			entry_retire(q, e);
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
		if (write_head(q, LINK_NOWAIT, &full) < 0 || full || timestamp_monotonic() >= deadline) {
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
	return timestamp_monotonic() - q->last_progress >= (timestamp_t)seconds * 1000000;
}
