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

#endif
