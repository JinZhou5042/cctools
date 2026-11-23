/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* Receive cached files and directories with the streaming transfer protocol of the Worker transfer server. Workers
receive from peers and from the Manager data port with it, and the Manager data service receives checkpoints with it.
The sending side is vine_transfer_send.c, for both.

Each file, directory, or symlink is one header line, "file <name> <size> <mode> <mtime>", "dir <name> <mode> <mtime>",
or "symlink <name> <length>", with a URL-encoded name. A file or symlink is followed by its contents, and a directory
by the entries it contains and then "end". A sender reports an item it cannot read with "error <name> <errno>". */

#ifndef VINE_TRANSFER_RECEIVE_H
#define VINE_TRANSFER_RECEIVE_H

#include "budget.h"
#include "link.h"

#include <stdint.h>

/* Receive one item into the directory dirname, under the name that the sender gives, and add the bytes of its files
and symlinks to totalsize. Returns 0 on failure, -1 when the sender reported with "error" that it cannot send the item
or an entry inside it, which a retry from the same sender cannot change, 1 after one item, and 2 after the "end" of a
directory. The receive
fails when no data arrives for timeout seconds, however long the whole transfer takes. On failure, a partial item may
remain in dirname, and error_message, if not NULL, receives an allocated explanation unless it already holds one. The
debug log gets the item's header line and, for a directory, one line with its count of entries and bytes when it ends,
not a line per entry. */

int vine_transfer_get_any(struct link *lnk, const char *dirname, int64_t *totalsize, int *mode, int *mtime, int timeout, char **error_message);

/* A receive of one item that its caller continues in pieces, for a loop that must never wait for the sender: the
Worker reads a put from the Manager's control link with it. It receives the same protocol, with the same results and
log lines, as vine_transfer_get_any(), but it reads only what has arrived: a header line only once it is whole in the
link's buffer (link_buffer_has_line()), and contents with reads that do not wait. The caller judges a sender that stops
in the middle, since a receiver never waits. */

struct vine_transfer_receiver;

/* What one call of vine_transfer_receiver_continue() reached. */
typedef enum {
	VINE_TRANSFER_RECEIVE_PENDING,  /* Waits for more data: call again once the link has input. */
	VINE_TRANSFER_RECEIVE_COMPLETE, /* The whole item arrived and is in place. */
	VINE_TRANSFER_RECEIVE_FAILED,   /* The link failed or carried something invalid, or the item could not be written. */
	VINE_TRANSFER_RECEIVE_REFUSED,  /* The sender reported an item it cannot send (see vine_transfer_get_any()). */
} vine_transfer_receive_status_t;

/* Start a receive of one item into the directory dirname. Returns NULL when out of memory. */
struct vine_transfer_receiver *vine_transfer_receiver_create(const char *dirname);

/* Receive what has arrived on lnk, piece by piece of at most 64 KB, while the budget lasts and at least one piece. A
receive that is not PENDING is over: on FAILED or REFUSED a partial item may remain in dirname, as with
vine_transfer_get_any(). */
vine_transfer_receive_status_t vine_transfer_receiver_continue(struct vine_transfer_receiver *r, struct link *lnk, const struct budget *b);

/* The bytes of the files and symlinks received so far, and the mode and mtime of the item, valid once COMPLETE. */
int64_t vine_transfer_receiver_size(const struct vine_transfer_receiver *r);
int vine_transfer_receiver_mode(const struct vine_transfer_receiver *r);
int vine_transfer_receiver_mtime(const struct vine_transfer_receiver *r);

/* End a receive, closing the file it was writing. What it wrote stays in dirname. */
void vine_transfer_receiver_delete(struct vine_transfer_receiver *r);

#endif
