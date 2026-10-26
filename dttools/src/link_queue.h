/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LINK_QUEUE_H
#define LINK_QUEUE_H

/** @file link_queue.h
An output queue for a link. A process that talks to a peer must not block writing to it: a peer that is itself
writing and not reading would then wait for the process while the process waits for it, and a process that serves
several peers from one thread would stall every other peer behind one that stops reading. Such a process queues what
it sends and writes only what the socket takes, keeping a partly written entry at the head, so the stream stays intact
and in order. Its poll asks for @ref LINK_WRITE while the queue is not empty and calls @ref link_queue_send when the
link is writable.
<p>
An entry is data, copied into the queue, a byte range of a file, which is opened when it reaches the head
(@ref link_queue_push_file), a byte range of a file that is already open (@ref link_queue_push_fd), or a producer
(@ref link_queue_push_producer), which makes its data only when it reaches the head, such as a directory that is
walked as it is sent. A file is read and sent in chunks, so a large file is never held in memory. Once a write of an
entry was attempted, the entry keeps its bytes in place until they are written, as an SSL write that must be retried
requires. Entries that reach the head together and fit in one chunk, such as the lines of a message and the small files
between them, are copied into that chunk and leave in one write.
<p>
The queue judges its peer by progress: @ref link_queue_stalled reports a queue that has waited to write for a given
time without any byte taken. A queue that fails, because the link fails, a file range cannot be read in full, or a
producer fails, drops its entries and refuses new ones, and the owner should then close the link, because the peer
received part of the stream at most. @ref link_queue_flush writes everything, waiting as needed, where the owner has
nothing else to do, such as before it disconnects.
*/

#include "link.h"
#include "timestamp.h"

#include <stdint.h>
#include <sys/types.h>
#include <time.h>

/** An output queue for one link. The structure is opaque. */
struct link_queue;

/** A function that makes the data of a producer entry (@ref link_queue_push_producer). It is called when the entry
reaches the head of its queue, and everything it pushes to the queue during the call is placed ahead of the entry, in
order, and is written before the function is called again.
@param q The queue.
@param arg The argument given to @ref link_queue_push_producer.
@return One if the producer has more data, in which case it must have pushed at least one byte, zero if it is done, or
-1 if it failed, which fails the queue.
*/
typedef int (*link_queue_producer_t)(struct link_queue *q, void *arg);

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

/** Append a byte range of an open file to the end of a queue, which owns the descriptor from now on and closes it once
the range is written or dropped, also when it refuses the range. A file that may be removed or replaced before the
range is written stays readable this way. A file that ends before the range does fails the queue.
@param q The queue.
@param fd The open descriptor of the file.
@param offset The offset of the range in the file.
@param length The number of bytes of the range.
@return One if the range was queued, zero if the queue has failed or is sealed and dropped it.
*/
int link_queue_push_fd(struct link_queue *q, int fd, int64_t offset, int64_t length);

/** Append a producer to the end of a queue, which calls it when it reaches the head (see @ref link_queue_producer_t)
and releases its argument once it is done or dropped, also when the queue refuses it. A producer that pushes a little
at a time, such as one entry of a directory per call, holds no more than that in the queue however much it sends.
@param q The queue.
@param produce The function that makes the data.
@param release A function that frees the argument, or NULL.
@param arg The argument of both functions.
@return One if the producer was queued, zero if the queue has failed or is sealed and dropped it.
*/
int link_queue_push_producer(struct link_queue *q, link_queue_producer_t produce, void (*release)(void *arg), void *arg);

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

/** Write the rest of the entry that a write took only in part, waiting as needed, so the peer is not left in the
middle of what was pushed. A process calls it before it waits for data from a peer that may itself be waiting for the
rest of a message from it. Pushes are kept whole within an entry, so the peer then has complete messages, except for a
message that was pushed as several entries, such as a header and the file it announces.
@param q The queue.
@param stoptime The absolute time at which to give up.
@return One if no entry is written in part anymore, or zero if the queue has failed.
*/
int link_queue_finish(struct link_queue *q, time_t stoptime);

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
@return Non-zero if a write, a file read, or a producer of the queue failed, after which the queue holds and accepts
nothing.
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
