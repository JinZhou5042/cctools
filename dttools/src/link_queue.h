/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LINK_QUEUE_H
#define LINK_QUEUE_H

/** @file link_queue.h
An output queue for a link, so a process never blocks writing to a peer. A blocking write waits while the peer is busy
writing too, and a process serving many peers from one thread would stall them all behind one that stops reading.
The owner pushes what it sends, polls for @ref LINK_WRITE while the queue is not empty, and calls @ref link_queue_send
when the link is writable. A partly written entry stays at the head, so the stream stays intact and in order.
<p>
An entry is copied data, a byte range of a file read in chunks at the head, or a producer that makes its data when it
reaches the head. Small entries that reach the head together leave in one write. A queue that fails drops everything
and refuses new data, and the owner should close the link.
*/

#include "link.h"
#include "timestamp.h"

#include <stdint.h>
#include <sys/types.h>
#include <time.h>

/** An output queue for one link. */
struct link_queue;

/** Makes the data of a producer entry when it reaches the head. What it pushes goes ahead of it, and is written before
it is called again.
@param lq The queue.
@param arg The argument given to @ref link_queue_push_producer.
@return One if more data follows, in which case it must have pushed some, zero when done, or -1 to fail the queue.
*/
typedef int (*link_queue_producer_t)(struct link_queue *lq, void *arg);

/** Create an empty queue.
@param link The link to write to, which the queue never closes.
@return A new queue, or NULL if memory is exhausted.
*/
struct link_queue *link_queue_create(struct link *link);

/** Delete a queue and what it still holds.
@param lq The queue.
*/
void link_queue_delete(struct link_queue *lq);

/** Append a copy of data.
@param lq The queue.
@param data The bytes.
@param length The number of bytes.
@return One if queued, zero if the queue has failed or is sealed.
*/
int link_queue_push(struct link_queue *lq, const char *data, size_t length);

/** Append a byte range of a file, opened when it reaches the head. A file that then cannot be read in full fails the
queue.
@param lq The queue.
@param path The path of the file.
@param offset The offset of the range.
@param length The number of bytes.
@return One if queued, zero if the queue has failed or is sealed.
*/
int link_queue_push_file(struct link_queue *lq, const char *path, int64_t offset, int64_t length);

/** Append a byte range of an open file. The queue owns the descriptor, also when it refuses the range, so a file
removed meanwhile still reads.
@param lq The queue.
@param fd The open descriptor.
@param offset The offset of the range.
@param length The number of bytes.
@return One if queued, zero if the queue has failed or is sealed.
*/
int link_queue_push_fd(struct link_queue *lq, int fd, int64_t offset, int64_t length);

/** Append a producer (see @ref link_queue_producer_t).
@param lq The queue.
@param produce The function that makes the data.
@param release Frees the argument when the producer is done or dropped, also when the queue refuses it, or NULL.
@param arg The argument of both functions.
@return One if queued, zero if the queue has failed or is sealed.
*/
int link_queue_push_producer(struct link_queue *lq, link_queue_producer_t produce, void (*release)(void *arg), void *arg);

/** Write without waiting until the queue is empty, the link is full, or the deadline passes, with at least one write.
@param lq The queue.
@param deadline Microseconds since the epoch after which no write starts.
@return Zero if the queue has failed, else one.
*/
int link_queue_send(struct link_queue *lq, timestamp_t deadline);

/** Write everything, waiting as needed.
@param lq The queue.
@param deadline Microseconds since the epoch at which to give up.
@return One if the queue is empty, zero if it has failed.
*/
int link_queue_flush(struct link_queue *lq, timestamp_t deadline);

/** Write the rest of any push that a write took only in part, waiting as needed, so a peer never waits for the rest of
a push, such as before waiting for a peer that may be reading it. Whole pushes that shared that write may go too. A
message pushed in parts, such as a header and a file, is finished only to the end of the part being written, which
suffices for messages a peer reads at any time, because those are pushed whole.
@param lq The queue.
@param deadline Microseconds since the epoch at which to give up.
@return One if no push is partly written, zero if the queue has failed.
*/
int link_queue_finish(struct link_queue *lq, timestamp_t deadline);

/** Refuse new data and keep writing what is queued, such as a final message.
@param lq The queue.
*/
void link_queue_seal(struct link_queue *lq);

/** @param lq The queue.
@return Non-zero if the queue holds nothing to write.
*/
int link_queue_empty(struct link_queue *lq);

/** @param lq The queue.
@return Non-zero if a write, a file read, or a producer failed.
*/
int link_queue_failed(struct link_queue *lq);

/** Check whether the peer took nothing for too long while data waited.
@param lq The queue.
@param seconds The longest time without progress, or zero or less for never.
@return Non-zero if the queue is not empty and wrote nothing for that long.
*/
int link_queue_stalled(struct link_queue *lq, int seconds);

#endif
