#ifndef VINE_WORKER_H
#define VINE_WORKER_H

/* Public interface to various items in vine_worker.c */

#include "vine_workspace.h"
#include "vine_worker_options.h"
#include "vine_cache_file.h"

#include "timestamp.h"
#include "link.h"

void vine_worker_send_cache_update( struct link *manager, const char *cachename, struct vine_cache_file *f );
void vine_worker_send_cache_invalid( struct link *manager, const char *cachename, const char *message );
void vine_worker_send_cache_capacity_update( struct link *manager );
void vine_worker_send_cache_transfer_start(const char *cachename);
void vine_worker_send_cache_transfer_progress(
		const char *cachename, uint64_t bytes);
void vine_worker_send_cache_transfer_cleanup(
		const char *cachename, uint64_t bytes, int path_absent);

extern struct vine_workspace *workspace;
extern struct vine_worker_options *options;

#endif
