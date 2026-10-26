/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LINK_WRITER_H
#define LINK_WRITER_H

/** @file link_writer.h
The write queue of one link, in one direction: every write to a link goes through its writer, and the link itself only
connects, accepts, reads, polls, and closes. A process that serves many peers from one thread writes without waiting
(@ref link_writer_send_async), polling for @ref LINK_WRITE while the writer is not empty, so it never stalls behind a
peer that stops reading. A connection that serves one request writes synchronously (@ref link_writer_send_sync).
A partly written entry stays at the head, so the stream stays intact and in order.
<p>
An entry is copied data, a byte range of a file read in chunks at the head, or a producer that makes its data when it
reaches the head. Small entries that reach the head together leave in one write. A writer that fails drops everything
and refuses new data, and the owner should close the link.
*/

#include "link.h"
#include "timestamp.h"

#include <stdint.h>
#include <sys/types.h>
#include <time.h>

/** The write queue of one link. */
struct link_writer;

/** Makes the data of a producer entry when it reaches the head. What it pushes goes ahead of it, and is written before
it is called again.
@param lw The writer.
@param arg The argument given to @ref link_writer_push_producer.
@return One if more data follows, in which case it must have pushed some, zero when done, or -1 to fail the writer.
*/
typedef int (*link_writer_producer_t)(struct link_writer *lw, void *arg);

/** Create an empty writer.
@param link The link to write to, which the writer never closes.
@return A new writer, or NULL if memory is exhausted.
*/
struct link_writer *link_writer_create(struct link *link);

/** Delete a writer and what it still holds.
@param lw The writer.
*/
void link_writer_delete(struct link_writer *lw);

/** Append a copy of data.
@param lw The writer.
@param data The bytes.
@param length The number of bytes.
@return One if queued, zero if the writer has failed or is sealed.
*/
int link_writer_push(struct link_writer *lw, const char *data, size_t length);

/** Append a byte range of a file, opened when it reaches the head. A file that then cannot be read in full fails the
writer.
@param lw The writer.
@param path The path of the file.
@param offset The offset of the range.
@param length The number of bytes.
@return One if queued, zero if the writer has failed or is sealed.
*/
int link_writer_push_file(struct link_writer *lw, const char *path, int64_t offset, int64_t length);

/** Append a byte range of an open file. The writer owns the descriptor, also when it refuses the range, so a file
removed meanwhile still reads.
@param lw The writer.
@param fd The open descriptor.
@param offset The offset of the range.
@param length The number of bytes.
@return One if queued, zero if the writer has failed or is sealed.
*/
int link_writer_push_fd(struct link_writer *lw, int fd, int64_t offset, int64_t length);

/** Append a producer (see @ref link_writer_producer_t).
@param lw The writer.
@param produce The function that makes the data.
@param release Frees the argument when the producer is done or dropped, also when the writer refuses it, or NULL.
@param arg The argument of both functions.
@return One if queued, zero if the writer has failed or is sealed.
*/
int link_writer_push_producer(struct link_writer *lw, link_writer_producer_t produce, void (*release)(void *arg), void *arg);

/** Write without waiting until the writer is empty, the link is full, or the deadline passes, with at least one write.
@param lw The writer.
@param deadline Microseconds since the epoch after which no write starts.
@return Zero if the writer has failed, else one.
*/
int link_writer_send_async(struct link_writer *lw, timestamp_t deadline);

/** Write everything, waiting as needed. Each write gets a new stoptime of idle seconds, so a large file never fails
while it moves, and a peer that takes nothing for that long fails the writer.
@param lw The writer.
@param deadline Microseconds since the epoch at which to give up, or zero for none.
@param idle Seconds that a write may wait without progress, or zero for no limit.
@return One if the writer is empty, zero if it has failed.
*/
int link_writer_send_sync(struct link_writer *lw, timestamp_t deadline, int idle);

/** Write the rest of any push that a write took only in part, waiting as needed, so a peer never waits for the rest of
a push, such as before waiting for a peer that may be reading it. Whole pushes that shared that write may go too. A
message pushed in parts, such as a header and a file, is finished only to the end of the part being written, which
suffices for messages a peer reads at any time, because those are pushed whole.
@param lw The writer.
@param deadline Microseconds since the epoch at which to give up.
@return One if no push is partly written, zero if the writer has failed.
*/
int link_writer_send_partial(struct link_writer *lw, timestamp_t deadline);

/** Refuse new data and keep writing what is queued, such as a final message.
@param lw The writer.
*/
void link_writer_seal(struct link_writer *lw);

/** @param lw The writer.
@return Non-zero if the writer holds nothing to write.
*/
int link_writer_empty(struct link_writer *lw);

/** @param lw The writer.
@return Non-zero if a write, a file read, or a producer failed.
*/
int link_writer_failed(struct link_writer *lw);

/** Check whether the peer took nothing for too long while data waited.
@param lw The writer.
@param seconds The longest time without progress, or zero or less for never.
@return Non-zero if the queue is not empty and wrote nothing for that long.
*/
int link_writer_stalled(struct link_writer *lw, int seconds);

#endif
