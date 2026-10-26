/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* Send and receive cached files and directories using the Worker transfer protocol. */

#ifndef VINE_TRANSFER_H
#define VINE_TRANSFER_H

#include "vine_cache.h"
#include "vine_transfer_receive.h"
#include "link.h"
#include "link_queue.h"

/* These prefixes identify sources handled by the native transfer protocols. */
#define VINE_TRANSFER_MANAGER_PREFIX "manager://"
#define VINE_TRANSFER_WORKER_PREFIX "worker://"
#define VINE_TRANSFER_WORKER_IP_PREFIX "workerip://"

typedef enum {
	VINE_TRANSFER_MODE_ANY,
	VINE_TRANSFER_MODE_FILE_ONLY
} vine_transfer_mode_t;

/* Queue any cached file/dir to be sent to a remote party. A directory is walked as the queue writes it, so the queue
 * holds no more than one entry of it at a time. Return 1 if it was queued, or 0 if the queue refused it. */

int vine_transfer_put_any(struct link_queue *queue, struct vine_cache *cache, const char *filename, vine_transfer_mode_t mode);

/* Request an item by name, and then receive it in the same way as vine_transfer_get_any. */

int vine_transfer_request_any(struct link *lnk, const char *request_name, const char *dirname, int64_t *totalsize, int *mode, int *mtime, int timeout, char **error_message);

#endif
