/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TRANSFER_SEND_H
#define VINE_TRANSFER_SEND_H

/*
The sending half of the transfer protocol (file, dir, symlink, end, error), shared by every sender of files and
directories, so its rules live in one place. An item is queued to a link writer, and a directory is walked as the
writer reaches it, one open file at a time.
*/

#include "link_writer.h"

/* What a receiver accepts. */
typedef enum {
	VINE_TRANSFER_MODE_ANY,	      /* A file or a directory. */
	VINE_TRANSFER_MODE_FILE_ONLY, /* Only a file: any other item is sent as an error. */
} vine_transfer_mode_t;

/* Queue the item at path, sent under name. The item itself is opened now, so a caller learns at once that it cannot be
 * sent. Return zero once it is queued, an errno for an item that cannot be sent, of which nothing was queued, or -1 if
 * the writer refused it. */
int vine_transfer_queue_item(struct link_writer *lw, const char *path, const char *name, vine_transfer_mode_t mode);

/* Queue the error of an item that cannot be sent, which fails its receiver. Return zero if the writer refused it. */
int vine_transfer_queue_error(struct link_writer *lw, const char *name, int errnum);

#endif
