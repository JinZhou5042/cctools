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
On a link where both sides send unprompted, two writers that wait for each other deadlock. The side that starts a
synchronous exchange reads and handles the peer's messages while it writes its request, and the side that answers
finishes any partly written push (@ref link_writer_send_partial) before it reads the next request. A link where only
one side sends data, such as a transfer or a pipe to a library, may use @ref link_writer_send_sync freely.
<p>
Deadlines and progress are times of @ref timestamp_monotonic, so a change of the time of day moves neither.
<p>
An entry is copied data, a byte range of a file read in chunks at the head, shared data that many writers hold by
reference, or a producer that makes its data when it reaches the head. Small entries that reach the head together leave in one write. A writer that fails drops everything
and refuses new data, and the owner should close the link.
*/

#include "link.h"
#include "timestamp.h"

#include <stdint.h>
#include <sys/types.h>
#include <time.h>

/** The write queue of one link. */
struct link_writer;

/** Immutable data that many writers send without a copy each, held in memory, in a file read at the head, or both. It
may move between the two while writers hold it, and is freed with its last reference. It belongs to one thread. */
struct link_writer_shared;

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

/** Create shared data in memory, with one reference for the caller.
@param data Bytes from malloc, owned by the shared data from now on, or still the caller's on failure.
@param length The number of bytes.
@return The shared data, or NULL if memory is exhausted.
*/
struct link_writer_shared *link_writer_shared_create(char *data, size_t length);

/** Create shared data in a file, read at the head of each writer, with one reference for the caller.
@param path The path of the file, which is copied.
@param length The number of bytes from its start.
@return The shared data, or NULL if memory is exhausted.
*/
struct link_writer_shared *link_writer_shared_create_file(const char *path, size_t length);

/** @param s The shared data.
@return The same shared data, with one more reference.
*/
struct link_writer_shared *link_writer_shared_hold(struct link_writer_shared *s);

/** Drop a reference, freeing the data with the last one.
@param s The shared data, or NULL.
*/
void link_writer_shared_release(struct link_writer_shared *s);

/** @param s The shared data.
@return Its bytes in memory, or NULL for data in a file.
*/
const char *link_writer_shared_bytes(const struct link_writer_shared *s);

/** Free the bytes in memory of data now also in a file, which writers then read. Bytes a writer is writing stay until
written.
@param s The shared data.
@param path The file that holds the data from its start, which is copied.
@return One if the data is in the file now, zero if memory is exhausted.
*/
int link_writer_shared_unload(struct link_writer_shared *s, const char *path);

/** Read data in a file into memory, so the file may be removed while writers hold it.
@param s The shared data.
@return One if the data no longer needs its file, zero if the file could not be read in full.
*/
int link_writer_shared_load(struct link_writer_shared *s);

/** Append a reference to shared data.
@param lw The writer.
@param s The shared data.
@return One if queued, zero if the writer has failed or is sealed.
*/
int link_writer_push_shared(struct link_writer *lw, struct link_writer_shared *s);

/** Write without waiting until the writer is empty, the link is full, or the deadline passes, with at least one write.
@param lw The writer.
@param deadline The time of @ref timestamp_monotonic after which no write starts.
@return Zero if the writer has failed, else one.
*/
int link_writer_send_async(struct link_writer *lw, timestamp_t deadline);

/** Write everything, waiting as needed. Each write gets a new stoptime of idle seconds, so a large file never fails
while it moves, and a peer that takes nothing for that long fails the writer. A write is one entry of at most 64 KB
for data read from a file, and a write that ends short with an idle bound fails the writer, so a peer that stops in
the middle of a write costs one idle period.
@param lw The writer.
@param deadline The time of @ref timestamp_monotonic at which to give up, or zero for none.
@param idle Seconds that a write may wait without progress, or zero for no limit.
@return One if the writer is empty, zero if it has failed.
*/
int link_writer_send_sync(struct link_writer *lw, timestamp_t deadline, int idle);

/** Write the rest of any push that a write took only in part, waiting as needed, so a peer never waits for the rest of
a push, such as before waiting for a peer that may be reading it. Whole pushes that shared that write may go too. A
message pushed in parts, such as a header and a file, is finished only to the end of the part being written, which
suffices for messages a peer reads at any time, because those are pushed whole.
@param lw The writer.
@param deadline The time of @ref timestamp_monotonic at which to give up.
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

/** Report when the writer last made progress, so the owner judges its peer with its own threshold, together with what
it reads from the peer.
@param lw The writer.
@return The time of @ref timestamp_monotonic at which the writer last wrote a byte or became non-empty.
*/
timestamp_t link_writer_last_progress(struct link_writer *lw);

/** Report how many bytes the link took from the writer since it was created. The count never decreases. A write that
the local kernel accepts proves nothing about the peer by itself: the owner compares the bytes written since its last
evidence of the peer with the bytes the buffers on the path can hold.
@param lw The writer.
@return The bytes written so far.
*/
uint64_t link_writer_bytes_written(struct link_writer *lw);

#endif
