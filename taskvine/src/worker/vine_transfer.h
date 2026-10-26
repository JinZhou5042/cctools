/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_TRANSFER_H
#define VINE_TRANSFER_H

#include "vine_cache.h"
#include "link.h"
#include "link_queue.h"

typedef enum {
	VINE_TRANSFER_MODE_ANY,
	VINE_TRANSFER_MODE_FILE_ONLY
} vine_transfer_mode_t;

/* Queue any cached file/dir to be sent to a remote party. A directory is walked as the queue writes it, so the queue
 * holds no more than one entry of it at a time. Return 1 if it was queued, or 0 if the queue refused it. */

int vine_transfer_put_any(struct link_queue *queue, struct vine_cache *cache, const char *filename, vine_transfer_mode_t mode);

/* Receive a named file/dir from the connection to a local transfer path. */

int vine_transfer_get_any(struct link *lnk, const char *dirname, int64_t *totalsize, int *mode, int *mtime, time_t stoptime, char **error_message);

/* Request an item by name, and then receive it in the same way as vine_transfer_get_any. */

int vine_transfer_request_any(struct link *lnk, const char *request_name, const char *dirname, int64_t *totalsize, int *mode, int *mtime, time_t stoptime, char **error_message);

#endif
