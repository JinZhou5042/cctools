/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_WORKER_INFO_H
#define VINE_WORKER_INFO_H

#include "taskvine.h"
#include "vine_resources.h"

#include "domain_name.h"
#include "hash_table.h"
#include "link.h"
#include "link_writer.h"
#include "itable.h"

typedef enum {
	VINE_WORKER_TYPE_UNKNOWN = 1,    // connection has not yet identified itself
	VINE_WORKER_TYPE_WORKER  = 2,    // connection is known to be a worker
	VINE_WORKER_TYPE_STATUS  = 4,    // connection is known to be a status client
} vine_worker_type_t;

struct vine_transfer_pair;

struct vine_worker_info {
	/* Type of connection: unknown, worker, status client. */
	vine_worker_type_t type;

	/* Connection to the worker or other client. */
	struct link *link;

	/* Everything sent to the worker. */
	struct link_writer *lw;

	/* Entry in the manager's sending_workers, or NULL. */
	struct list_cursor *sending;

	/* Entry in the manager's send_held_workers, or NULL. */
	struct list_cursor *send_held;

	/* Static properties reported by worker when it connects. */
	char *hostname;
	char *os;
	char *arch;
	char *version;
	char *factory_name;
	char *workerid;

	/* Remote address of worker. */
	char *addrport;

	/* Hash key used to locally identify this worker. */
	char *hashkey;

	/* Host (address or hostname) and port where this worker will accept transfers from peers. */
	char transfer_host[DOMAIN_NAME_MAX];
	int  transfer_port;
	int  transfer_port_active;
	char *transfer_url;       /* worker(ip)?://transfer_addr:transfer_port */
	int  transfer_connect_back; /* The Manager cannot reach the transfer port, so the Worker connects back to send files. */

	/* Response time X in seconds that the worker declared (info response-timeout), or zero. The manager judges the
	 * worker failed when it makes no progress for T = 2X, see vine_manager_worker_timeout(). */
	int  response_timeout;

	/* Scratch space of vine_schedule_task_to_worker(): bytes of the task's inputs cached on this worker, and whether it
	 * holds the task's first input without a transfer source, valid only while schedule_stamp equals the manager's
	 * schedule_stamp. */
	int64_t schedule_cached_bytes;
	int schedule_holds_unsourced;
	uint64_t schedule_stamp;

	/* Entry of the manager's capacity index (workers_by_capacity): the worker's free capacity priority when it was last
	 * placed there, and its serial, which is unique among workers and zero while the worker is not in the index. */
	double schedule_capacity;
	uint64_t schedule_serial;

	/* Blocking inputs of ready tasks that this worker holds, each with a reference. When the worker frees a transfer
	 * slot, gains room for a task, or leaves, each of them is woken (see vine_schedule.h). */
	struct set *blocked_files;

	/* Worker condition that may affect task start or cancellation. */
	int  draining;                          // if 1, worker does not accept anymore tasks. It is shutdown if no task running.
	int  pending_removal;                   // if not 0, the vine_worker_disconnect_reason_t with which the worker will be removed
	                                        // by disconnect_failed_workers. Such a worker is not in worker_table, but may still be
	                                        // reachable from tasks and replica sets, which should skip it.
	int  alarm_slow_worker;                 // if 1, no task has finished since a slow running task triggered a disconnection.
	                                        // 0 otherwise. A 2nd task triggering disconnection will cause the worker to disconnect
	int64_t     end_time;                   // epoch time (in seconds) at which the worker terminates
	                                        // If -1, means the worker has not reported in. If 0, means no limit.

	/* Resources and features that describe this worker. */
	struct vine_resources *resources;
	struct hash_table     *features;

	/* Current files and tasks that have been transfered to this worker */
	struct hash_table   *current_files;
	struct itable       *current_tasks;
	struct itable		*current_libraries;

	/* Dynamic information summed by count_worker_resources. */
	int	    tasks_committed;         /* # tasks at worker in any state. */
	int	    tasks_running;           /* # tasks at worker in RUNNING state */
	int	    tasks_waiting_retrieval; /* # tasks at worker in the WAITING_RETRIEVAL state */
	int         dynamic_tasks_running;   /* # tasks last reported by worker actually executing. */
	
	/* Accumulated stats about tasks about this worker. */
	int         finished_tasks;
	int64_t     total_tasks_complete;
	int64_t     total_bytes_transferred;
	int         forsaken_tasks;
	int64_t     inuse_cache;

	timestamp_t total_task_time;
	timestamp_t total_transfer_time;
	timestamp_t last_transfer_failure;
	/* Time of the last failed transfer from this worker as a source. It is not chosen as a source again until a
	 * message from it arrives after this time. */
	timestamp_t last_source_failure;
	timestamp_t start_time;
	timestamp_t last_msg_recv_time;
	timestamp_t last_update_msg_time;
	timestamp_t last_failure_time;

	/* consecutive failed peer transfers with worker serving as source or destination */
	int xfer_streak_bad_source_counter;

	/* consecutive failed peer transfers with worker serving as destination or destination */
	int xfer_streak_bad_destination_counter;

	/* total transfers */
	int xfer_total_good_source_counter;
	int xfer_total_bad_source_counter;
	int xfer_total_good_destination_counter;
	int xfer_total_bad_destination_counter;

	int incoming_xfer_counter;
	int outgoing_xfer_counter;
	/* Checkpoints of the data service that this worker sends: copies to the vault's shared directory and receives over
	 * connections. They are counted apart from outgoing_xfer_counter and limited by worker-source-max-checkpoints, so
	 * a worker that anchors its outputs still serves the tasks that read them. */
	int outgoing_checkpoints;

	/* Bytes of the files that this worker sends to other workers in the manager's transfer records, and the smallest file
	 * refused since the last wake because it would not fit beside them (-1 if none). See
	 * vine_current_transfers_source_has_room(). */
	int64_t outgoing_xfer_bytes;
	int64_t outgoing_refused_bytes;

	/* The manager's transfer records with this worker as destination and as source, so removing the worker drops its
	 * records without a pass over the records of all workers. See vine_current_transfers.c. */
	struct vine_transfer_pair *incoming_transfers;
	struct vine_transfer_pair *outgoing_transfers;

	/* Quarantine of tasks charged a lost worker (see vine_schedule.h): the id of the quarantined task that runs here,
	 * so the worker takes no other task, and the id of the quarantined task for which the worker takes no new task
	 * until it runs nothing, or zero. */
	int quarantine_task_id;
	int quarantine_reserved_id;

	/* The outgoing transfers of every worker on this worker's host, named by its hostname, or null before the worker
	 * reported it (see vine_current_transfers.h). */
	struct vine_transfer_host *transfer_host_load;
};

struct vine_worker_info * vine_worker_create( struct link * lnk );
void vine_worker_delete( struct vine_worker_info *w );

struct jx * vine_worker_to_jx( struct vine_worker_info *w );

int vine_worker_source_is_suspect( struct vine_worker_info *w );
#endif
