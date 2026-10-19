/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MANAGER_H
#define VINE_MANAGER_H

/*
This module defines the structures and types of the manager process as a whole.
This module is private to the manager and should not be invoked by the end user.
*/

#include "taskvine.h"
#include "budget.h"
#include <limits.h>

/*
The result of a variety of internal operations, indicating whether
the operation succeeded, or failed due to the fault of the worker,
the application, or the manager.
*/

typedef enum {
	VINE_SUCCESS = 0,
	VINE_WORKER_FAILURE,
	VINE_APP_FAILURE,
	VINE_MGR_FAILURE,
	VINE_END_OF_LIST,
} vine_result_code_t;

/*
The result of vine_manager_recv{_no_retry}, indicating whether an
incoming message was processed, and the expected next state of the connection.
*/

typedef enum {
	VINE_MSG_PROCESSED = 0,        /* Message was processed and connection is still good. */
	VINE_MSG_PROCESSED_DISCONNECT, /* Message was processed and disconnect now expected. */
	VINE_MSG_NOT_PROCESSED,        /* Message was not processed, waiting to be consumed. */
	VINE_MSG_FAILURE               /* Message not received, connection failure. */
} vine_msg_code_t;

/* The current resource monitoring configuration of the manager. */

typedef enum {
	VINE_MON_DISABLED = 0,
	VINE_MON_SUMMARY  = 1,   /* generate only summary. */
	VINE_MON_FULL     = 2,   /* generate summary, series and monitoring debug output. */
	VINE_MON_WATCHDOG = 4    /* kill tasks that exhaust resources */
} vine_monitoring_mode_t;

/* The various reasons why a worker process may disconnect from the manager. */

typedef enum {
	VINE_WORKER_DISCONNECT_UNKNOWN  = 0,
	VINE_WORKER_DISCONNECT_EXPLICIT,
	VINE_WORKER_DISCONNECT_STATUS_WORKER,
	VINE_WORKER_DISCONNECT_IDLE_OUT,
 	VINE_WORKER_DISCONNECT_FAST_ABORT,
	VINE_WORKER_DISCONNECT_FAILURE,
	VINE_WORKER_DISCONNECT_XFER_ERRORS
} vine_worker_disconnect_reason_t;

/* States known about libraries */

typedef enum {
	VINE_LIBRARY_WAITING = 0,
	VINE_LIBRARY_SENT,
	VINE_LIBRARY_STARTED,
	VINE_LIBRARY_FAILURE
} vine_library_state_t;

struct vine_worker_info;
struct vine_task;
struct vine_file;

struct vine_manager {

	/* Connection and communication settings */

	char *name;          /* Project name describing this manager at the catalog server. */
	int   port;          /* Port number on which this manager is listening for connections. */
	int   priority;      /* Priority of this manager relative to other managers with the same name. */
	char *catalog_hosts; /* List of catalogs to which this manager reports. */
	char *manager_preferred_connection; /* Recommended method for connecting to this manager.  @ref vine_set_manager_preferred_connection */
	char  workingdir[PATH_MAX];         /* Current working dir, for reporting to the catalog server. */
	char *uuid;          /* Unique identifier of manager when reported to catalog. */
	struct hash_table *properties;   /* Set of additional properties to report to catalog server. */

	struct vine_manager_data_service *ds; /* Own the data listener and bidirectional transfer executors. */
	struct link *manager_link;       /* Listening TCP connection for accepting new workers. */
	struct link_poll_set *poll_set;  /* Links that a step waits on: the manager link, the data service, the wake pipe, every worker in worker_table (with its worker as data), and departing links. */
	struct list *sending_workers;    /* Workers whose output queue held data, visited by send_to_workers() in every step. */
	int send_holds;                  /* Nesting depth of vine_manager_send_hold(). While positive, sends only queue. */
	struct list *send_held_workers;  /* Workers given data while sends were held, written once at the outermost release. */
	int connections_waiting;         /* The manager link was readable at the last poll. */
	struct list *departing_links;    /* Links of removed workers whose sealed queues still hold a final message. */
	struct budget round;             /* Time for the work of one step of vine_wait(), started after its poll. */
	struct link *wake_link;          /* Read end of the pipe that vine_wake() writes to from other threads. */
	int wake_fd;                     /* Write end of that pipe. */
	int woken;                       /* A wake arrived during the current vine_wait(), which then returns. */

	/* Security configuration */

	char *password;      /* Shared secret between manager and worker. Usable with or without SSL. */
	char *ssl_key;       /* Filename of SSL private key */
	char *ssl_cert;      /* Filename of SSL certificate. */
	int   ssl_enabled;   /* If true, SSL transport is used between manager and workers */

	/* Primary data structures for tracking task state. */

	struct itable *tasks;           /* Maps task_id -> vine_task of all tasks in any state. */
	int standard_tasks_count;       /* Number of standard tasks in the tasks table, so that vine_empty() takes constant time. */
	struct skip_list   *ready_tasks;       /* Skip list of vine_task that are waiting to execute. */
	struct itable   *running_table;      /* Table of vine_task that are running at workers. */
	struct list   *waiting_retrieval_list;      /* List of vine_task that are waiting to be retrieved. */
	struct list   *retrieved_list;      /* List of vine_task that have been retrieved. */
	struct list   *task_info_list;  /* List of last N vine_task_infos for computing capacity. */
	/* Sums of the reports in task_info_list, kept as reports are added and removed, so that the capacity costs O(1) per
	 * returned task instead of a pass over the list. limit is the list's bound, which never shrinks. */
	struct {
		timestamp_t transfer_time;
		timestamp_t exec_time;
		timestamp_t manager_time;
		double cores;
		double memory;
		double disk;
		double gpus;
		int limit;
	} task_info_sums;
	struct hash_table *categories;  /* Maps category_name -> struct category */
	struct category *last_category; /* The category that vine_category_lookup_or_create() returned last, which most calls ask for again. */
	struct hash_table *library_templates; /* Maps library name -> vine_task of library with that name. */
	uint64_t library_templates_serial;    /* Advanced whenever a library template is added or removed. */
	char *library_checked_name;           /* The library name whose presence was checked last, a copy, or NULL. */
	uint64_t library_checked_serial;      /* library_templates_serial when library_checked_name was checked. */
	int library_checked_found;            /* Whether that library had a template then. */

	struct skip_list_cursor *ready_tasks_cr;      /* Iterates ready_list in order */

	/* Primary data structures for tracking worker state. */

	struct hash_table *worker_table;     /* Maps link -> vine_worker_info */
	struct list *zombie_workers;         /* Workers marked for removal. They are not in worker_table, and are removed in disconnect_failed_workers. */
	struct hash_table *worker_blocklist; /* Maps hostname -> vine_blocklist_info */
	struct hash_table *factory_table;    /* Maps factory_name -> vine_factory_info */
	struct hash_table *workers_with_watched_file_updates;  /* Maps link -> vine_worker_info */
	struct hash_table *current_transfer_table; 	/* Maps transfer id -> struct transfer_pair */
	uint64_t next_transfer_id;	/* Number of the next transfer record, from a random start, so ids of another manager never match. */
	struct hash_table *current_transfer_url_table; 	/* Maps source url -> int count of current transfers */
	struct itable     *task_group_table; 	/* Maps group id -> list vine_task */
	struct hash_table *workers_idle_disconnecting;  /* set of workers that were granted a request to idle disconnect, and are in the process of disconnecting. */

	/* Primary data structures for tracking files. */

	struct hash_table *file_table;      /* Maps fileid -> struct vine_file.* */
	struct hash_table *file_worker_table; /* Maps cachename -> struct set of workers with a replica of the file.* */
	struct priority_queue *temp_files_to_replicate; /* Priority queue of temp files to be replicated, those with less replicas are at the top. */


	/* Primary scheduling controls. */

	vine_schedule_t worker_selection_algorithm;    /* Mode for selecting best worker for task in main scheduler. */
	uint64_t schedule_stamp;                       /* Marks worker scratch space of the current vine_schedule_task_to_worker(). */
	struct skip_list *workers_by_capacity;         /* Capacity index: the workers of worker_table by free cores and function slots, see vine_schedule.h. */
	uint64_t worker_serial;                        /* Last serial given to a worker that entered the capacity index. */
	struct hash_table *blocked_sources;            /* Blocking inputs by the URL of their source, each a set of file references, see vine_schedule.h. */
	uint64_t blocked_epoch;                        /* Advanced by vine_schedule_wake_all(), which ends the wait of every blocked ready task. */
	uint64_t blocked_pass;                         /* Counts dispatch passes, so a blocking input is checked once per pass. */
	uint64_t schedule_wakes;                       /* Counts the events that may end the wait of a blocked ready task, and changes of the ready tasks. */
	uint64_t blocked_files_registered;             /* Files registered as blocking at their current serial, so a wake by name of a file looks it up only while some file is. */
	uint64_t schedule_waiting_wakes;               /* The value of schedule_wakes while schedule_waiting_run counts. */
	uint64_t schedule_waiting_run;                 /* Ready tasks found waiting in a row by dispatch passes since schedule_wakes last changed. */
	timestamp_t time_last_blocked_wake;            /* When every blocked ready task was last woken by time. */
	vine_category_mode_t allocation_default_mode;  /* Mode for computing resources allocations for each task. */

	/* Internal state modified by the manager */

	int next_task_id;       /* Next integer task_id to be assigned to a created task. */
	int fixed_location_in_queue; /* Number of fixed location tasks currently being managed */
	int num_tasks_left;    /* Optional: Number of tasks remaining, if given by user.  @ref vine_set_num_tasks */
	int nothing_happened_last_wait_cycle; /* Set internally in main loop if no messages or tasks were processed during the last wait loop.
																					 If set, poll longer to avoid wasting cpu cycles, and growing log files unnecessarily.*/
	int dispatch_blocked;  /* Set when the last dispatch attempt found no worker with room for a task, or every ready task waiting for an event. */
	int gpus_requested;    /* Set once a ready task or a category requests gpus. Until then, idle gpus are not room for a task. */

	/* Accumulation of statistics for reporting to the caller. */

	struct vine_stats *stats;
	struct vine_stats *stats_measure;

	/* Time of most recent events for computing various timeouts */

	timestamp_t time_last_wait;
	timestamp_t time_last_log_stats;
	timestamp_t time_last_large_tasks_check;
	timestamp_t time_last_ready_tasks_check;
	timestamp_t time_last_worker_checks; /* When keepalives, slow and drained workers, and blocks were last checked. */
	timestamp_t time_last_compaction;    /* When the tables were last compacted, see compact_tables(). */
	int64_t compacted_since_trim;        /* Removed table entries freed since the heap was last trimmed. */
	int64_t tables_peak_since_trim;      /* Most tasks and files held at a compaction since the last trim. */
	int64_t tables_held_last;            /* Tasks and files held at the last compaction. */
	timestamp_t time_unexplained_worker_losses[2]; /* The two most recent unexpected losses of workers that held tasks
							  that had never lost a worker, newest first, or zero. See cleanup_worker(). */
	timestamp_t link_poll_end;
	time_t      catalog_last_update_time;
	time_t      resources_last_update_time;

	/* Logging configuration. */

    char *runtime_directory;
	FILE *perf_logfile;        /* Performance logfile for tracking metrics by time. */
	FILE *txn_logfile;         /* Transaction logfile for recording every event of interest. */
	FILE *graph_logfile;       /* Graph logfile for visualizing application structure. */
	int perf_log_interval;	   /* Minimum interval for performance log entries in seconds. */
	int perf_log_pending;      /* An event since the last performance log entry waits for the interval to pass. */
	
	/* Resource monitoring configuration. */

	vine_monitoring_mode_t monitor_mode;
	struct vine_file *monitor_exe;
    int monitor_interval;

	struct rmsummary *measured_local_resources;
	struct rmsummary *current_max_worker;
	struct rmsummary *max_task_resources_requested;

	/* Peer Transfer Configuration */
	int peer_transfers_enabled;
	int file_source_max_transfers;
	int worker_source_max_transfers;

	/* Hungry call optimization */
	timestamp_t time_last_hungry;      /* Last time vine_hungry_computation was called. */
	int tasks_to_sate_hungry;          /* Number of tasks that would sate the queue since last call to vine_hungry_computation. */
	int tasks_waiting_last_hungry;     /* Number of tasks originally waiting when call to vine_hungry_computation was made. */
	timestamp_t hungry_check_interval; /* Maximum interval between vine_hungry_computation checks. */

	/* Task Groups Configuration */
	int task_groups_enabled; 
	int group_id_counter; 

	/* Various performance knobs that can be tuned. */
	int short_timeout;            /* Timeout in seconds to send/recv a brief message from worker */
	int long_timeout;             /* Timeout if in the middle of an incomplete message. */
	int minimum_transfer_timeout; /* Minimum number of seconds to allow for a manager<-> worker file transfer. */
	int transfer_outlier_factor;  /* Factor to consider a given transfer time to be an outlier justifying cancellation. */
	int default_transfer_rate;    /* Assumed data transfer rate for computing timeouts, prior to collecting observations. */
	int process_pending_check;    /* Enables check for waiting processes in main loop via @ref process_pending */
	int keepalive_interval;	      /* Time between keepalive request transmissions. */
	int keepalive_timeout;	      /* Keepalive response must be received within this time, otherwise worker disconnected. */
	int hungry_minimum;           /* Minimum number of waiting tasks to consider queue not hungry. */
	int hungry_minimum_factor;    /* queue is hungry if number of waiting tasks is less than hungry_minimum_factor * number of connected workers. */
	int wait_for_workers;         /* Wait for these many workers to connect before dispatching tasks at start of execution. */
	int max_workers;              /* Specify the maximum number of workers to use during execution. */
	int attempt_schedule_depth;   /* number of submitted tasks to attempt scheduling before we continue to retrievals */
	int max_retrievals;           /* Do at most this number of task retrievals of either receive_one_task or receive_all_tasks_from_worker. If less
                                     than 1, prefer to receive all completed tasks before submitting new tasks. */
	int worker_retrievals;        /* retrieve all completed tasks from a worker as opposed to recieving one of any completed task*/
	int prefer_dispatch;          /* try to dispatch tasks even if there are retrieved tasks ready to return  */
	int load_from_shared_fs_enabled;/* Allow worker to load file from shared filesytem instead of through manager */

	int fetch_factory;            /* If true, manager queries catalog for factory configuration. */
	int proportional_resources;   /* If true, tasks divide worker resources proportionally. */
	int proportional_whole_tasks; /* If true, round-up proportions to whole number of tasks. */
	int ramp_down_heuristic;      /* If true, and there are more workers than tasks waiting, then tasks are allocated all the free resources of a worker large enough to run them.
																	 If monitoring watchdog is not enabled, then this heuristic has no effect. */
	int immediate_recovery;       /* If true, recovery tasks for tmp files are created as soon as the worker that had them
																	 disconnects. Otherwise, create them only when a tasks needs then as inputs (this is
																	 the default). */
	int transfer_temps_recovery;  /* If true, attempt to recover temp files from lost worker to reach threshold required */
	int transfer_replica_per_cycle;  /* Maximum number of replica to request per temp file per iteration */
	int temp_replica_count;       /* Number of replicas per temp file */
	int clean_redundant_replicas; /* If true, remove redundant replicas of temp files to save disk space. */
	int shift_disk_load;          /* If true, shift storage burden to more available workers to minimize disk usage peaks. */
	int external_recovery_handling; /* If true, recovery tasks are returned by vine_wait to the user. */

	double resource_submit_multiplier; /* Factor to permit overcommitment of resources at each worker.  */
	double bandwidth_limit;            /* Artificial limit on bandwidth of manager<->worker transfers. */
	int disk_avail_threshold; /* Ensure this minimum amount of available disk space. (in MB) */

	int update_interval;			/* Seconds between updates to the catalog. */
	int resource_management_interval;	/* Seconds between measurement of manager local resources. */
	timestamp_t transient_error_interval; /* microseconds between new attempts on task rescheduling and using a file replica as source after a failure. */

	int max_library_retries;        /* The maximum time that a library can be failed and retry another one, if over this count the library template will be removed */
	int watch_library_logfiles;     /* If true, watch the output files produced by each of the library processes running on the remote workers, take them back the current logging directory */

	double sandbox_grow_factor;         /* When task disk sandboxes are exhausted, increase the allocation using their measured valued times this factor */
	double disk_proportion_available_to_task;   /* intentionally reduces disk allocation for tasks to reserve some space for cache growth. */

	/* todo: confirm datatype. int or int64 */
	int max_task_stdout_storage;	/* Maximum size of standard output from task.  (If larger, send to a separate file.) */
	int max_new_workers;			/* Maximum number of workers to add in a single cycle before dealing with other matters. */

	timestamp_t large_task_check_interval;	/* How frequently to check for tasks that do not fit any worker. */
	double option_blocklist_slow_workers_timeout;	/* Default timeout for slow workers to come back to the pool, can be set prior to creating a manager. */

	/* Testing mode parameters */
	timestamp_t enforce_worker_eviction_interval;   /* Enforce worker eviction interval in seconds */
	timestamp_t time_start_worker_eviction;         /* Track the time when we start evicting workers */
};

/*
These are not public API functions, but utility methods that may
be called on the manager object by other elements of the manager process.
*/

/* Declares file f. If a file with the same f->cached_name is already declared, f
 * is ****deleted**** and the previous file is returned. Otherwise f is returned. */
struct vine_file *vine_manager_declare_file(struct vine_manager *m, struct vine_file *f);
struct vine_file *vine_manager_lookup_file(struct vine_manager *q, const char *cached_name);

/*
Send a printf-style message to a remote worker. Everything sent to a worker goes through its queue in order, and the
manager writes only what the worker's link takes, so a worker that stops reading never blocks the manager. Return the
length of the message, or -1 if the worker's link has failed.
*/
#ifndef SWIG
__attribute__ (( format(printf,3,4) ))
#endif
int vine_manager_send( struct vine_manager *q, struct vine_worker_info *w, const char *fmt, ... );

/* Send bytes to a remote worker through its queue, such as the body of a message. Return 0, or -1 if the worker's link
 * has failed. */
int vine_manager_send_data(struct vine_manager *q, struct vine_worker_info *w, const char *data, size_t length);

/* Hold the immediate write that a send to a worker with an empty queue makes, until the matching
 * vine_manager_send_release(). The messages of one operation, such as the input files and description of a dispatched
 * task or the unlinks of released files, then leave in one write per worker instead of one write per message. Holds
 * nest, and the outermost release writes each worker that was given data once, as much as its link takes. Whatever
 * remains is written by later steps of vine_wait(), as for any queue that the link did not take at once. */
void vine_manager_send_hold(struct vine_manager *q);
void vine_manager_send_release(struct vine_manager *q);

/* Send a byte range of a local file to a remote worker through its queue. The file is read in chunks when the range
 * reaches the head of the queue, so it must not shrink before then. Return 0, or -1 if the worker's link has failed. */
int vine_manager_send_file(struct vine_manager *q, struct vine_worker_info *w, const char *path, int64_t offset, int64_t length);

/* Send shared data to a remote worker through its queue, which holds a reference instead of a copy until it writes the
 * data, so data sent to many workers is held once. Return 0, or -1 if the worker's link has failed. */
struct link_queue_shared;
int vine_manager_send_shared(struct vine_manager *q, struct vine_worker_info *w, struct link_queue_shared *s);

/* Receive a line-oriented message from a remote worker, as the reply to a request. Everything queued for the worker,
 * including the request, is written first, so the exchange stays synchronous. */
vine_msg_code_t vine_manager_recv( struct vine_manager *q, struct vine_worker_info *w, char *line, int length );

/* Compute the expected wait time for a transfer of length bytes. */
int vine_manager_transfer_time( struct vine_manager *q, struct vine_worker_info *w, int64_t length );

/* Various functions to compute expected properties of tasks. */
const struct rmsummary *vine_manager_task_resources_min(struct vine_manager *q, struct vine_task *t);
const struct rmsummary *vine_manager_task_resources_max(struct vine_manager *q, struct vine_task *t);

/* Find a library template on the manager */
struct vine_task *vine_manager_find_library_template(struct vine_manager *q, const char *library_name);

/* Internal: Enable shortcut of main loop upon child process completion. Needed for Makeflow to interleave local and remote execution. */
void vine_manager_enable_process_shortcut(struct vine_manager *q);

struct rmsummary *vine_manager_choose_resources_for_task( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t );

int64_t overcommitted_resource_total(struct vine_manager *q, int64_t total);

/* Internal: Shut down a specific worker. */
int vine_manager_shut_down_worker(struct vine_manager *q, struct vine_worker_info *w);

/** Return any completed task without doing any manager work. */
struct vine_task *vine_manager_no_wait(struct vine_manager *q, const char *tag, int task_id);

void vine_manager_remove_worker(struct vine_manager *q, struct vine_worker_info *w, vine_worker_disconnect_reason_t reason);

/* Take a worker out of worker_table, and remove it at the next disconnect_failed_workers in the wait loop. Use instead of
 * vine_manager_remove_worker outside of the wait loop, where the caller may still use the worker, or its
 * link may still be in the poll table. Only the first reason is kept. The reason cannot be VINE_WORKER_DISCONNECT_UNKNOWN. */
void vine_manager_mark_worker_for_removal(struct vine_manager *q, struct vine_worker_info *w, vine_worker_disconnect_reason_t reason);

/* Check if the worker is able to transfer the necessary files for this task. */
int vine_manager_transfer_capacity_available(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t);
int vine_manager_limits_sends(struct vine_manager *q, struct vine_file *f);

/* Check if an input that a worker does not hold can be fetched now, whatever the worker. */
int vine_manager_transfer_source_available(struct vine_manager *q, struct vine_file *f);


/* Delete a file from a worker. */
int delete_worker_file(struct vine_manager *q, struct vine_worker_info *w, const char *filename, vine_cache_level_t cache_level, vine_cache_level_t delete_upto_level);

/* The expected format of files created by the resource monitor.*/
#define RESOURCE_MONITOR_TASK_LOCAL_NAME "vine-task-%d"
#define RESOURCE_MONITOR_REMOTE_NAME "cctools-monitor"
#define RESOURCE_MONITOR_REMOTE_NAME_EVENTS RESOURCE_MONITOR_REMOTE_NAME "events.json"

#endif
