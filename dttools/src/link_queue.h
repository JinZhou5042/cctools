/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LINK_QUEUE_H
#define LINK_QUEUE_H

/** @file link_queue.h
An output queue for a link. A process that serves several peers from one thread must not block writing to one of them:
a peer that stops reading would stall every other peer. Such a process queues what it sends to a peer and writes only
what the socket takes, keeping a partly written entry at the head, so the stream stays intact and in order. Its poll
asks for @ref LINK_WRITE while the queue is not empty and calls @ref link_queue_send when the link is writable.
<p>
An entry is either data, copied into the queue, a byte range of a file, which is opened when it reaches the head
and read and sent in chunks, so a large file is never held in memory, or shared data (@ref link_queue_push_shared),
which many queues send without a copy each. Once a write of an entry was attempted, the entry keeps its bytes in place
until they are written, as an SSL write that must be retried requires. Entries that reach the head together and fit in
one chunk, such as the lines of a message and the small files or shared data between them, are copied into that chunk
and leave in one write.
<p>
The queue judges its peer by progress: @ref link_queue_stalled reports a queue that has waited to write for a given
time without any byte taken. A queue that fails, because the link fails or a file range cannot be read in full, drops
its entries and refuses new ones, and the owner should then close the link, because the peer received part of the
stream at most. @ref link_queue_flush writes everything, waiting as needed, before a direct write or before reading a
reply to a request in the queue.
*/

#include "link.h"
#include "timestamp.h"

#include <stdint.h>
#include <sys/types.h>
#include <time.h>

/** An output queue for one link. The structure is opaque. */
struct link_queue;

/** Data that many queues send without a copy each, such as a small file that a server sends to many peers. Its bytes
are held in memory, or in a file that a queue reads when the data reaches its head. The data is immutable, but it may
move: data in memory that was written to a file may leave memory (@ref link_queue_shared_unload), and data in a file may
be read into memory (@ref link_queue_shared_load), so that the file may be removed while queues still hold it. Each
queue that holds the data keeps a reference, which it drops once the data is written or the queue drops it, and the
data is freed with its last reference. References are not atomic: the data and every queue that holds it belong to one
thread. The structure is opaque. */
struct link_queue_shared;

/** Create an empty output queue.
@param link The link the queue writes to. The queue does not own the link and never closes it.
@return A new queue, or NULL if memory is exhausted.
*/
struct link_queue *link_queue_create(struct link *link);

/** Delete a queue and drop the entries it still holds.
@param q The queue to delete.
*/
void link_queue_delete(struct link_queue *q);

/** Append data to the end of a queue. Nothing is written; the data is copied.
@param q The queue.
@param data The bytes to append.
@param length The number of bytes to append.
@return One if the data was queued, zero if the queue has failed or is sealed and dropped it.
*/
int link_queue_push(struct link_queue *q, const char *data, size_t length);

/** Append a byte range of a file to the end of a queue. Nothing is read or written now: the file is opened when the
range reaches the head of the queue. A file that then cannot be opened, or ends before the range does, fails the queue,
because the peer was promised the whole range.
@param q The queue.
@param path The path of the file.
@param offset The offset of the range in the file.
@param length The number of bytes of the range.
@return One if the range was queued, zero if the queue has failed or is sealed and dropped it.
*/
int link_queue_push_file(struct link_queue *q, const char *path, int64_t offset, int64_t length);

/** Create shared data held in memory, with one reference for the caller.
@param data The bytes, allocated with malloc. The shared data owns them from now on, or the caller keeps them on failure.
@param length The number of bytes.
@return The shared data, or NULL if memory is exhausted.
*/
struct link_queue_shared *link_queue_shared_create(char *data, size_t length);

/** Create shared data held in a file, with one reference for the caller. Nothing is read now: a queue reads the file
when the data reaches its head, and fails as for @ref link_queue_push_file if the file cannot be read in full then.
@param path The path of the file, which is copied.
@param length The number of bytes of the file to send, from its start.
@return The shared data, or NULL if memory is exhausted.
*/
struct link_queue_shared *link_queue_shared_create_file(const char *path, size_t length);

/** Add a reference to shared data.
@param s The shared data.
@return The same shared data.
*/
struct link_queue_shared *link_queue_shared_hold(struct link_queue_shared *s);

/** Drop a reference to shared data, and free the data with its last reference.
@param s The shared data, or NULL.
*/
void link_queue_shared_release(struct link_queue_shared *s);

/** Return the bytes of shared data held in memory.
@param s The shared data.
@return The bytes, valid while a reference is held and the data is not unloaded, or NULL for data held in a file.
*/
const char *link_queue_shared_bytes(const struct link_queue_shared *s);

/** Free the bytes of shared data held in memory that is now also held in a file, so that queues that hold it read the
file instead when it reaches their head. Bytes that a queue is writing in place stay until it wrote them.
@param s The shared data.
@param path The file that holds the data from its start, which is copied.
@return One if the data is held in the file now, or zero if memory is exhausted, in which case it stays in memory.
*/
int link_queue_shared_unload(struct link_queue_shared *s, const char *path);

/** Read shared data held in a file into memory while queues still hold it, so that the file may be removed. Data held
by nobody but the caller is left as it is, and data whose bytes are still in memory keeps them.
@param s The shared data.
@return One if the data no longer needs its file, or zero if the file could not be read in full, in which case queues
that still hold the data read the file as before.
*/
int link_queue_shared_load(struct link_queue_shared *s);

/** Append shared data to the end of a queue, which holds a reference to it until it is written or dropped. Nothing is
copied, read, or written now.
@param q The queue.
@param s The shared data.
@return One if the data was queued, zero if the queue has failed or is sealed and dropped it.
*/
int link_queue_push_shared(struct link_queue *q, struct link_queue_shared *s);

/** Write what the link takes without waiting, until the queue is empty, the link takes no more, or the deadline
passes. At least one write is attempted, so each call makes progress when the link has room.
@param q The queue.
@param deadline The time in microseconds since the epoch after which no further write is started.
@return One if the queue is intact, empty or waiting for room, or zero if it has failed.
*/
int link_queue_send(struct link_queue *q, timestamp_t deadline);

/** Write every queued byte, waiting for room as needed until the stoptime.
@param q The queue.
@param stoptime The absolute time at which to give up.
@return One if the queue is empty, or zero if it has failed.
*/
int link_queue_flush(struct link_queue *q, time_t stoptime);

/** Refuse new data. What the queue holds is still written, so a final message behind other data goes out.
@param q The queue.
*/
void link_queue_seal(struct link_queue *q);

/** Check whether a queue holds nothing to write.
@param q The queue.
@return Non-zero if the queue is empty, including a queue that has failed.
*/
int link_queue_empty(struct link_queue *q);

/** Check whether a queue has failed.
@param q The queue.
@return Non-zero if a write or a file read of the queue failed, after which the queue holds and accepts nothing.
*/
int link_queue_failed(struct link_queue *q);

/** Check whether a queue has waited for its peer too long. The wait starts when data is queued into an empty queue and
is renewed by every byte the link takes.
@param q The queue.
@param seconds The longest acceptable time without progress. Zero or less never reports a stall.
@return Non-zero if the queue is not empty and no byte was written for at least the given time.
*/
int link_queue_stalled(struct link_queue *q, int seconds);

#endif
