/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_cache.h"
#include "vine_cache_file.h"
#include "vine_catalog.h"
#include "vine_file.h"
#include "vine_manager.h"
#include "vine_mount.h"
#include "vine_process.h"
#include "vine_protocol.h"
#include "vine_resources.h"
#include "vine_sandbox.h"
#include "vine_transfer.h"
#include "vine_transfer_server.h"
#include "vine_watcher.h"
#include "vine_worker_options.h"
#include "vine_workspace.h"

#include "address.h"
#include "budget.h"
#include "catalog_query.h"
#include "cctools.h"
#include "change_process_title.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "envtools.h"
#include "full_io.h"
#include "gpu_info.h"
#include "hash_cache.h"
#include "hash_table.h"
#include "host_disk_info.h"
#include "host_memory_info.h"
#include "itable.h"
#include "jx.h"
#include "jx_eval.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "link.h"
#include "link_auth.h"
#include "list.h"
#include "load_average.h"
#include "macros.h"
#include "md5.h"
#include "path.h"
#include "path_disk_size_info.h"
#include "pattern.h"
#include "process.h"
#include "random.h"
#include "stringtools.h"
#include "trash.h"
#include "unlink_recursive.h"
#include "url_encode.h"
#include "xpu_tracker.h"
#include "xxmalloc.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

/***************************************************************/
/* Primary Worker Data Structures for Tracking Tasks and Files */
/***************************************************************/

/* The workspace is the top level directory under which all worker state is stored. */
struct vine_workspace *workspace;

/* Table of all processes in any state, indexed by task_id. */
/* Processes should be created/deleted when added/removed from this table. */
static struct itable *procs_table = NULL;

/* Table of all processes currently running, indexed by task_id. */
/* These are additional pointers into procs_table and should not be deleted */
static struct itable *procs_running = NULL;

/* List of all procs that are waiting to be run. */
/* These are additional pointers into procs_table and should not be deleted */
static struct list *procs_waiting = NULL;

/* List of asynchronous messages pending to be sent to the manager */
static struct list *pending_async_messages = NULL;

/* Bytes of the message at the head of pending_async_messages that the socket already took. */
static size_t pending_async_offset = 0;
/* The manager link that the queued messages go to. Only a direct write to this link waits for them. */
static struct link *pending_async_link = 0;

/* Processes that do_kill() signalled and dropped before they exited, indexed by pid. Their tasks are gone, so only
 * their exit status remains to be collected, which the main loop does without waiting and a disconnect does waiting. */
static struct itable *procs_killed = NULL;

/* Table of killed function calls still running in their library, indexed by task_id, with the library as value. */
static struct itable *procs_killed_functions = NULL;

/* Table of all processes with results to be sent back, indexed by task_id. */
/* These are additional pointers into procs_table and should not be deleted */
static struct itable *procs_complete = NULL;

/* Processes that ended and whose outputs are moving into the cache, in the order they ended. A process is in no other
 * table but procs_table until continue_stageout() moves it to procs_complete, so it is neither running nor reported.
 * These are additional pointers into procs_table and should not be deleted. */
static struct list *procs_stageout = NULL;

/* Table of current transfers and their id. */
static struct hash_table *current_transfers = NULL;

/* The cache manager object keeping track of files stored by the worker. */
struct vine_cache *cache_manager = 0;

/* The watcher object is responsible for periodically checking whether */
/* files marked with VINE_WATCH have been modified and should be streamed back. */
static struct vine_watcher *watcher = 0;

/***************************************************************/
/*         Machine Resources Managed by the Worker             */
/***************************************************************/

/* The resources measured and available at this worker. */
static struct vine_resources *total_resources = 0;

/* The resources currently allocated to running tasks. */
static int64_t cores_allocated = 0;
static int64_t memory_allocated = 0;
static int64_t disk_allocated = 0;
static int64_t gpus_allocated = 0;

/* Trackers for specific cores and gpus units in use. */
struct xpu_tracker *gpu_tracker = 0;
struct xpu_tracker *core_tracker = 0;

/***************************************************************/
/*     State of Interactions Between Manager and Worker        */
/***************************************************************/

/* A complete description of the address of a manager and how to connect. */
struct manager_address {
	char host[DOMAIN_NAME_MAX];
	int port;
	char addr[DOMAIN_NAME_MAX];
};

/* The list of matching manager addresses obtained from the catalog server. */
static struct list *manager_addresses;

/* The address of the current manager we are (attempting to) talk to. */
static struct manager_address *current_manager_address;

/* The worker ID is a unique string generated by the worker to uniquely identify  */
/* to the manager, even across multiple connections. */
static char *worker_id;

/* True when the worker has informed the manager that there are results pending. */
static int results_to_be_sent_msg = 0;

/* If flag is set, then the worker proceeds to immediately cleanup and shut down. */
/* This can be set by Ctrl-C or by any condition that prevents further progress.  */
static int abort_flag = 0;

/* Record the signal received, to inform the manager if appropiate. */
static int abort_signal_received = 0;

/* Flag used to indicate a child must be waited for. */
static int sigchld_received_flag = 0;

/* True if manager sent explicit message to release worker from its service. */
static int released_by_manager = 0;

/***************************************************************/
/*       Accumulated Statistics Tracked by Worker              */
/***************************************************************/

/* Tracks the most recent taskid recevied from the manager. */
/* This is used to signal the "freshness" of status data given back to the manager. */
static int64_t last_task_received = 0;

// Unique counter for generating unique mini task ids and sandboxes
static int mini_task_id = 0;

/* The timestamp at which the worker began executing. */
static timestamp_t worker_start_time = 0;

/* The accumulated time of all task executions. */
static timestamp_t total_task_execution_time = 0;

/* Total count of tasks executed. */
static int total_tasks_executed = 0;

/* Total number of files counted when measuring worker disk. */
static int64_t files_counted = 0;

/***************************************************************/
/*       Configuration Options Given on the Command Line       */
/***************************************************************/

struct vine_worker_options *options = 0;

extern int vine_hack_do_not_compute_cached_name;

static int flush_async_messages(struct link *l, time_t stoptime);
static void finish_running_task(struct vine_process *p, vine_result_t result);

/* Send a printf-formatted message to the current manager, or to a peer. */

__attribute__((format(printf, 2, 3))) void send_message(struct link *l, const char *fmt, ...)
{
	char debug_msg[2 * VINE_LINE_MAX];
	va_list va;
	va_list debug_va;

	if (!flush_async_messages(l, time(0) + options->active_timeout)) {
		return;
	}

	va_start(va, fmt);

	string_nformat(debug_msg, sizeof(debug_msg), "tx: %s", fmt);
	va_copy(debug_va, va);

	vdebug(D_VINE, debug_msg, debug_va);
	link_vprintf(l, time(0) + options->active_timeout, fmt, va);

	va_end(va);
	va_end(debug_va);
}

/*
Send the asynchronous messages that the socket takes without blocking. The worker never blocks writing a message of
its own: the manager may be blocked writing a long task to this worker at the same time, and then neither side would
read. A message that the socket takes only in part keeps its remainder at the head of the queue, so the stream stays
intact, and the main loop waits for the link to become writable while messages remain.
*/

void deliver_async_messages(struct link *l)
{
	char *message;
	while ((message = list_peek_head(pending_async_messages))) {
		size_t length = strlen(message + pending_async_offset);
		ssize_t written = link_write(l, message + pending_async_offset, length, LINK_NOWAIT);
		if (written <= 0) {
			break;
		}
		if ((size_t)written < length) {
			pending_async_offset += written;
			break;
		}
		debug(D_VINE, "tx: %s", message);
		list_pop_head(pending_async_messages);
		free(message);
		pending_async_offset = 0;
	}
}

/*
Write every queued message to the manager, waiting as needed, before the worker writes to that link directly. A direct
write in the middle of a message that the socket took only in part would cut it. The worker writes directly only when
the manager reads: in the reply to a request, or right before it disconnects. Return 0 if the link failed.
*/

static int flush_async_messages(struct link *l, time_t stoptime)
{
	if (l != pending_async_link) {
		return 1;
	}

	char *message;
	while ((message = list_peek_head(pending_async_messages))) {
		size_t length = strlen(message + pending_async_offset);
		ssize_t written = link_write(l, message + pending_async_offset, length, stoptime);
		if (written > 0) {
			pending_async_offset += written;
		}
		if (written < 0 || (size_t)written < length) {
			return 0;
		}
		debug(D_VINE, "tx: %s", message);
		list_pop_head(pending_async_messages);
		free(message);
		pending_async_offset = 0;
	}
	return 1;
}

/* Buffer an asynchronous message to be sent to the manager */

void send_async_message(struct link *l, const char *fmt, ...)
{
	assert(l);
	pending_async_link = l;

	va_list va;
	va_start(va, fmt);
	int length = vsnprintf(NULL, 0, fmt, va);
	va_end(va);

	/* Abort if the message is longer than VINE_LINE_MAX. */
	if (length >= VINE_LINE_MAX) {
		fatal("message to the manager is %d bytes, more than VINE_LINE_MAX", length);
	}

	char *message = xxmalloc(length + 1);
	va_start(va, fmt);
	vsnprintf(message, length + 1, fmt, va);
	va_end(va);

	list_push_tail(pending_async_messages, message);
	deliver_async_messages(l); // attempt to deliver message, will be delivered later if buffer is full.
}

/* Send asynchronous task completion messages for current complete processes */

void send_complete_tasks(struct link *l)
{
	int size = itable_size(procs_complete);
	int visited;
	struct vine_process *p;
	for (visited = 0; visited < size; visited++) {
		p = itable_pop(procs_complete);
		if (p->output_length <= 1024 && p->output_length > 0) {

			char *output = xxmalloc(p->output_length + 1);
			int output_file = open(p->output_file_name, O_RDONLY);
			ssize_t length = output_file >= 0 ? full_read(output_file, output, p->output_length) : -1;
			if (output_file >= 0) {
				close(output_file);
			}
			/* The manager reads exactly the length sent, so a short read or a NUL byte shortens the output. */
			output[MAX(length, 0)] = '\0';
			length = strlen(output);
			send_async_message(l,
					"complete %d %d %lld %lld %llu %llu %d %d\n%s",
					p->result,
					p->exit_code,
					(long long)length,
					(long long)length,
					(unsigned long long)p->execution_start,
					(unsigned long long)p->execution_end,
					p->sandbox_size,
					p->task->task_id,
					output);
			free(output);
		} else {
			send_async_message(l,
					"complete %d %d %lld %lld %llu %llu %d %d\n",
					p->result,
					p->exit_code,
					(long long)p->output_length,
					0,
					(unsigned long long)p->execution_start,
					(unsigned long long)p->execution_end,
					p->sandbox_size,
					p->task->task_id);
		}
	}
}

static void report_changes(struct link *manager)
{
	if (!flush_async_messages(manager, time(0) + options->active_timeout)) {
		return;
	}
	vine_watcher_send_changes(watcher, manager, time(0) + options->active_timeout);
	send_message(manager, "end\n");

	results_to_be_sent_msg = 0;
}

/* Receive a single-line message from the current manager. */

int recv_message(struct link *l, char *line, int length, time_t stoptime)
{
	int result = link_readline(l, line, length, stoptime);
	if (result)
		debug(D_VINE, "rx: %s", line);
	return result;
}

/*
We track how much time has elapsed since the manager assigned a task.
If time(0) > idle_stoptime, then the worker will disconnect.
*/

static void reset_idle_timer()
{
	options->idle_stoptime = time(0) + options->idle_timeout;
}

/*
Disk measurement runs on the main loop in pieces, each limited by the budget of a round. A measurement starts at most
every check_resources_interval seconds. It measures the cache directory, then the sandbox of every task that requested
disk and was running when the measurement started, and resumes in the next round where the budget stopped it. A large
cache or sandbox, or a slow disk, then never keeps the main loop from reading the manager.
*/

/* Non-zero while a measurement is in progress. */
static int disk_measurement_active = 0;

/* State of the cache directory measurement between rounds. NULL when it has not started or has completed. */
static struct path_disk_size_info *cache_disk_state = NULL;

/* Non-zero once the current measurement completed the cache directory and moved on to sandboxes. */
static int cache_disk_measured_now = 0;

/* Ids of the tasks whose sandboxes the current measurement has yet to measure. The head is the one in progress. */
static struct list *disk_measurement_tasks = NULL;

/* Megabytes and files of the cache directory, from the last complete measurement. */
static int64_t cache_disk_measured = 0;
static int64_t cache_files_measured = 0;

static void start_disk_measurement()
{
	if (disk_measurement_active || !cache_manager) {
		return;
	}

	disk_measurement_active = 1;
	cache_disk_measured_now = 0;

	struct vine_process *p;
	uint64_t task_id;
	int iteration;
	ITABLE_ITERATE(procs_running, iteration, task_id, p)
	{
		if (p->task->resources_requested->disk >= 1) {
			list_push_tail(disk_measurement_tasks, (void *)(uintptr_t)task_id);
		}
	}
}

/* Abandon a measurement in progress, as when the worker disconnects from its manager. */

static void stop_disk_measurement()
{
	path_disk_size_info_delete_state(cache_disk_state);
	cache_disk_state = NULL;
	while (list_pop_head(disk_measurement_tasks)) {
	}
	disk_measurement_active = 0;
	cache_disk_measured_now = 0;
}

/* Measure the cache directory until the budget expires. Return 1 once the measurement of the cache completed. */

static int measure_cache_disk(const struct budget *b)
{
	char *cache_dir = vine_cache_data_path(cache_manager, ".");
	int result = path_disk_size_info_get_budget(cache_dir, b, &cache_disk_state, NULL);
	free(cache_dir);

	if (result == 0 && !cache_disk_state->complete_measurement) {
		return 0;
	}

	if (result == 0 && cache_disk_state->last_byte_size_complete >= 0) {
		cache_disk_measured = (int64_t)ceil(cache_disk_state->last_byte_size_complete / (1.0 * MEGA));
		cache_files_measured = cache_disk_state->last_file_count_complete;
		debug(D_VINE, "disk measurement: cache uses %" PRId64 " MB in %" PRId64 " files", cache_disk_measured, cache_files_measured);
	} else {
		debug(D_VINE, "disk measurement of the cache did not complete");
	}

	path_disk_size_info_delete_state(cache_disk_state);
	cache_disk_state = NULL;
	return 1;
}

/*
Continue the disk measurement until the budget expires, and end each running task whose sandbox went over the disk
it requested once its measurement completes. Return 0 if a task was ended.
*/

static int continue_disk_measurement(const struct budget *b)
{
	if (!disk_measurement_active) {
		return 1;
	}

	if (!cache_disk_measured_now) {
		if (!measure_cache_disk(b)) {
			return 1;
		}
		cache_disk_measured_now = 1;
	}

	int ok = 1;
	void *item;
	while ((item = list_peek_head(disk_measurement_tasks))) {
		if (budget_expired(b)) {
			return ok;
		}

		/* The task may have ended since the measurement began. */
		struct vine_process *p = itable_lookup(procs_running, (uint64_t)(uintptr_t)item);
		if (p && !vine_process_measure_disk(p, b)) {
			return ok;
		}
		list_pop_head(disk_measurement_tasks);

		if (p && p->sandbox_size > p->task->resources_requested->disk) {
			debug(D_VINE,
					"Task %d went over its disk size limit: %s > %s\n",
					p->task->task_id,
					rmsummary_resource_to_str("disk", p->sandbox_size, /* with units */ 1),
					rmsummary_resource_to_str("disk", p->task->resources_requested->disk, 1));
			finish_running_task(p, VINE_RESULT_SANDBOX_EXHAUSTION);
			trash_move(p->sandbox);
			ok = 0;
		}
	}

	disk_measurement_active = 0;
	cache_disk_measured_now = 0;
	return ok;
}

/*
Measure the disk used by the worker: start a new measurement unless one is in progress, and return the usage of the
cache directory from the last complete measurement and of task sandboxes from their latest measurements.
*/

static int64_t measure_worker_disk()
{
	if (!cache_manager) {
		return 0;
	}

	start_disk_measurement();

	int64_t disk_measured = cache_disk_measured;
	files_counted = cache_files_measured;

	struct vine_process *p;
	uint64_t task_id;
	int iteration;

	ITABLE_ITERATE(procs_table, iteration, task_id, p)
	{
		if (p->sandbox_size > 0) {
			disk_measured += p->sandbox_size;
			files_counted += p->sandbox_file_count;
		}
	}

	return disk_measured;
}

/*
Measure the resources associated with this worker
and apply any local options that override it.
*/

static void measure_worker_resources()
{
	static int disk_set = 0;
	static time_t last_resources_measurement = 0;
	if (time(0) < last_resources_measurement + options->check_resources_interval) {
		return;
	}

	struct vine_resources *r = total_resources;

	vine_resources_measure_locally(r, workspace->workspace_dir);

	if (options->cores_total > 0)
		r->cores.total = options->cores_total;
	if (options->memory_total > 0)
		r->memory.total = options->memory_total;
	if (options->gpus_total > -1)
		r->gpus.total = options->gpus_total;

	if (options->disk_total > 0) {
		r->disk.total = MIN(r->disk.total, options->disk_total);
	} else if (!disk_set) {
		/* XXX If no disk is specified we will allocate half of the worker disk available
		   at startup. We will not update the allocation since it should remain static.
		   If something else is consuming disk on the machine it would cause issues with tasks which
		   request the whole available worker disk. Leaving half of the disk to other processes
		   should leave the worker free to use the other half without the need to re measure. */
		r->disk.total = ceil(r->disk.total * options->disk_percent / 100) + r->disk.inuse;
		disk_set = 1;
	}

	r->disk.inuse = measure_worker_disk();
	r->tag = last_task_received;

	/* Create a tracker for specific gpus and cores, if not already created. */
	if (!gpu_tracker)
		gpu_tracker = xpu_tracker_create("gpus", r->gpus.total);
	if (!core_tracker)
		core_tracker = xpu_tracker_create("cores", r->cores.total);

	last_resources_measurement = time(0);
}

/*
Send a message to the manager with user defined features.
*/

static void send_features(struct link *manager)
{
	char *f;
	void *dummy;
	int iteration;

	HASH_TABLE_ITERATE(options->features, iteration, f, dummy)
	{
		char feature_encoded[VINE_LINE_MAX];
		url_encode(f, feature_encoded, VINE_LINE_MAX);
		send_async_message(manager, "feature %s\n", feature_encoded);
	}
}

static void vine_resource_send(struct link *manager, struct vine_resource *r, const char *name)
{
	send_async_message(manager, "%s %lf\n", name, r->total);
}

static void vine_resources_send(struct link *manager, struct vine_resources *r)
{
	debug(D_VINE, "Sending resource description to manager:");
	vine_resources_debug(r);

	send_async_message(manager, "resources\n");

	vine_resource_send(manager, &r->cores, "cores");
	vine_resource_send(manager, &r->memory, "memory");
	vine_resource_send(manager, &r->disk, "disk");
	vine_resource_send(manager, &r->gpus, "gpus");
	vine_resource_send(manager, &r->workers, "workers");

	send_async_message(manager, "tag %" PRId64 "\n", r->tag);

	send_async_message(manager, "end\n");
}

/*
Send a message to the manager with my current resources.
*/

static void send_resource_update(struct link *manager)
{
	// if workers are set to expire in some time, send the expiration time to manager
	if (options->manual_wall_time_option > 0) {
		options->end_time = worker_start_time + (options->manual_wall_time_option * 1e6);
	}

	vine_resources_send(manager, total_resources);
}

/*
Send a message to the manager with my current statistics information.
*/

static void send_stats_update(struct link *manager)
{
	send_async_message(manager, "info tasks_running %lld\n", (long long)itable_size(procs_running));
}

/*
Send a periodic keepalive message to the manager, otherwise it will
think that the worker has crashed and gone away.
*/

static int send_keepalive(struct link *manager, int force_resources)
{
	send_async_message(manager, "alive\n");
	send_resource_update(manager);
	return 1;
}

/*
Send an asynchronmous message to the manager indicating that an item was successfully loaded into the cache, along with
its size in bytes and transfer time in usec.
*/

void vine_worker_send_cache_update(struct link *manager, const char *cachename, struct vine_cache_file *f)
{
	if (!manager) {
		return;
	}

	char *transfer_id = hash_table_remove(current_transfers, cachename);
	if (!transfer_id) {
		transfer_id = xxstrdup("X");
	}

	send_async_message(manager,
			"cache-update %s %d %d %lld %lld %lld %lld %s\n",
			cachename,
			f->original_type,
			f->cache_level,
			(long long)f->size,
			(long long)f->mtime,
			(long long)f->transfer_time,
			(long long)f->start_time,
			transfer_id);

	free(transfer_id);
}

/*
Send an asynchronous message to the manager indicating that an item previously queued in the cache is invalid because it
could not be loaded.  Accompanied by a corresponding error message.
*/

void vine_worker_send_cache_invalid(struct link *manager, const char *cachename, const char *message)
{
	if (!manager) {
		return;
	}

	int length = strlen(message);
	char *transfer_id = hash_table_remove(current_transfers, cachename);
	if (transfer_id) {
		debug(D_VINE, "Sending Cache invalid transfer id: %s", transfer_id);
		send_async_message(manager, "cache-invalid %s %d %s\n%s", cachename, length, transfer_id, message);
		free(transfer_id);
	} else {
		send_async_message(manager, "cache-invalid %s %d\n%s", cachename, length, message);
	}
}

/*
Send an asynchronous message to the manager indicating where the worker is listening for transfers.
*/

static void send_transfer_address(struct link *manager)
{
	if (!vine_transfer_server_running()) {
		send_async_message(manager, "info transfer_port_bind_failed %d-%d\n", options->transfer_port_min, options->transfer_port_max);
		return;
	}
	char addr[LINK_ADDRESS_MAX];
	int port;

	vine_transfer_server_address(addr, &port);
	if (options->reported_transfer_port > 0) {
		port = options->reported_transfer_port;
	}

	if (options->reported_transfer_host) {
		send_async_message(manager, "transfer-hostport %s %d\n", options->reported_transfer_host, port);
	} else {
		send_async_message(manager, "transfer-port %d\n", port);
	}
}

/*
Send the initial "ready" message to the manager with the version and so forth.
The manager will not start sending tasks until this message is recevied.
*/

static void report_worker_ready(struct link *manager)
{
	/*
	The hostname is useful for troubleshooting purposes, but not required.
	If there are naming problems, just use "unknown".
	*/

	char hostname[DOMAIN_NAME_MAX];
	if (!domain_name_cache_guess(hostname)) {
		strcpy(hostname, "unknown");
	}

	send_async_message(manager,
			"taskvine %d %s %s %s %d.%d.%d\n",
			VINE_PROTOCOL_VERSION,
			hostname,
			options->os_name,
			options->arch_name,
			CCTOOLS_VERSION_MAJOR,
			CCTOOLS_VERSION_MINOR,
			CCTOOLS_VERSION_MICRO);
	send_async_message(manager, "info worker-id %s\n", worker_id);
	vine_cache_scan(cache_manager, manager);

	send_features(manager);
	send_transfer_address(manager);
	if (options->connect_back) {
		send_async_message(manager, "info connect-back 1\n");
	}
	send_async_message(manager, "info worker-end-time %" PRId64 "\n", (int64_t)DIV_INT_ROUND_UP(options->end_time, USECOND));

	if (options->factory_name) {
		send_async_message(manager, "info from-factory %s\n", options->factory_name);
	}

	send_keepalive(manager, 1);
}

/*
Start executing the given process on the local host,
accounting for the resources as necessary.
Should maintain parallel structure to reap_process() above.
*/

static int start_process(struct vine_process *p, struct link *manager)
{
	struct vine_task *t = p->task;

	/* Create the sandbox environment for the task. */
	if (!vine_sandbox_stagein(p, cache_manager)) {
		p->execution_start = p->execution_end = timestamp_get();
		p->result = VINE_RESULT_FORSAKEN;
		p->exit_code = 1;
		itable_insert(procs_complete, p->task->task_id, p);
		return 0;
	}

	/* Mark the resources claimed by this task as in use. */
	cores_allocated += t->resources_requested->cores;
	memory_allocated += t->resources_requested->memory;
	disk_allocated += t->resources_requested->disk;
	gpus_allocated += t->resources_requested->gpus;

	/* Mark which specific cores and gpus are assigned. */
	if (t->resources_requested->cores > 0) {
		xpu_tracker_alloc(core_tracker, t->resources_requested->cores, t->task_id);
	}

	if (t->resources_requested->gpus > 0) {
		xpu_tracker_alloc(gpu_tracker, t->resources_requested->gpus, t->task_id);
	}

	/* Now start the process (or function) running. */
	if (vine_process_execute(p)) {

		/* If this process represents a library, notify the manager that it is running.
		 * Also set the sigchld flag so the worker will immediately check for the library
		 * startup instead of sleeping and waiting for the manager. */
		if (p->task->provides_library) {
			send_async_message(manager, "info library-update %d %d\n", p->task->task_id, VINE_LIBRARY_STARTED);
			sigchld_received_flag = 1;
		}

		/* If this process represents a function, update the number running. */
		if (t->needs_library) {
			p->library_process->functions_running++;
		}
	} else {
		fatal("unable to start task_id %d!", p->task->task_id);
	}

	itable_insert(procs_running, p->task->task_id, p);

	return 1;
}

/*
This process has ended so mark it complete and
account for the resources as necessary.
Should maintain parallel structure to start_process() above.
With a manager, its outputs are moved into the cache by continue_stageout(), which then reports it complete.
*/

/* Measure the standard output after stage-out, which moves it into the cache when it is also a task output. */

static void complete_process(struct vine_process *p)
{
	struct stat info;
	if (!stat(p->output_file_name, &info)) {
		p->output_length = info.st_size;
	} else {
		p->output_length = 0;
	}
	itable_insert(procs_complete, p->task->task_id, p);
}

static void reap_process(struct vine_process *p, struct link *manager)
{
	p->execution_end = timestamp_get();

	cores_allocated -= p->task->resources_requested->cores;
	memory_allocated -= p->task->resources_requested->memory;
	disk_allocated -= p->task->resources_requested->disk;
	gpus_allocated -= p->task->resources_requested->gpus;

	xpu_tracker_free(gpu_tracker, p->task->task_id);
	xpu_tracker_free(core_tracker, p->task->task_id);

	if (p->type == VINE_PROCESS_TYPE_FUNCTION) {
		p->library_process->functions_running--;
	}

	itable_remove(procs_running, p->task->task_id);
	if (manager) {
		list_push_tail(procs_stageout, p);
	} else {
		complete_process(p);
	}

	total_task_execution_time += (p->execution_end - p->execution_start);
	total_tasks_executed++;
}

/*
Move the outputs of ended processes into the cache, in the order the processes ended, until the budget expires, and
then report each process complete. Measuring a directory output resumes in the next round where the budget stopped
it. Each call makes progress even with an expired budget, so a process whose outputs are regular files is reported in
the round it ended.
*/

static void continue_stageout(struct link *manager, const struct budget *b)
{
	struct vine_process *p;
	while ((p = list_peek_head(procs_stageout))) {
		if (!vine_sandbox_stageout(p, cache_manager, manager, b)) {
			return;
		}
		list_pop_head(procs_stageout);
		complete_process(p);
		if (budget_expired(b)) {
			return;
		}
	}
}

/*
Find any processes that have overrun their declared absolute end time,
and send a kill signal.  The actual exit of the process will be detected at a later time.
*/

static void expire_procs_running()
{
	struct vine_process *p;
	uint64_t task_id;
	int iteration;

	double current_time = timestamp_get() / USECOND;

	ITABLE_ITERATE(procs_running, iteration, task_id, p)
	{
		if (p->task->resources_requested->end > 0 && current_time > p->task->resources_requested->end) {
			p->result = VINE_RESULT_MAX_END_TIME;
			vine_process_kill(p);
		}
	}
}

/* Force a single running task to finish with the given result flag. */

static void finish_running_task(struct vine_process *p, vine_result_t result)
{
	p->result |= result;
	vine_process_kill(p);
}

/* Force all running tasks to finish with the given result flag. */

static void finish_running_tasks(vine_result_t result)
{
	struct vine_process *p;
	uint64_t task_id;
	int iteration;

	ITABLE_ITERATE(procs_running, iteration, task_id, p)
	{
		finish_running_task(p, result);
	}
}

/*
Reap the functions running in a library process that ends. They run in children of the library and end with it, so
they are forsaken, and the manager runs them again. A function thus never runs without its library, and no process
refers to a library process after it is gone.
*/

static void reap_library_functions(struct vine_process *library)
{
	struct vine_process *p;
	uint64_t task_id;
	int iteration;

	ITABLE_ITERATE(procs_running, iteration, task_id, p)
	{
		if (p->library_process == library) {
			debug(D_VINE, "killing function task %d running on library task %d", (int)task_id, library->task->task_id);
			finish_running_task(p, VINE_RESULT_FORSAKEN);
			reap_process(p, /* do not stage out */ NULL);
			p->library_process = NULL;
		}
	}

	ITABLE_ITERATE(procs_killed_functions, iteration, task_id, p)
	{
		if (p == library) {
			itable_remove(procs_killed_functions, task_id);
		}
	}
}

/*
Kill a failed library process.
This library process may either not existent or no longer responses.
*/

static void handle_failed_library_process(struct vine_process *p, struct link *manager)
{
	if (p->type != VINE_PROCESS_TYPE_LIBRARY) {
		return;
	}

	p->library_ready = 0;
	p->exit_code = 1;

	/* Mark this library as failed */
	finish_running_task(p, VINE_RESULT_LIBRARY_EXIT);

	/* Forsake the tasks that are running on this library */
	/* It no available libraries on this worker, tasks waiting for this library will be forsaken */
	reap_library_functions(p);

	reap_process(p, manager);
}

/*
Scan over all of the processes known by the worker,
and if they have exited, move them into the procs_complete table
for later processing.
*/

static int handle_completed_tasks(struct link *manager)
{
	struct vine_process *p;
	struct vine_process *fp;
	uint64_t task_id;
	int iteration;
	uint64_t done_task_id;
	int done_exit_code;

	struct list *failed_libraries = list_create();

	ITABLE_ITERATE(procs_running, iteration, task_id, p)
	{
		/* Check to see if this process itself is completed. */

		if (vine_process_is_complete(p)) {
			if (p->type == VINE_PROCESS_TYPE_LIBRARY) {
				/* Kill the library process if it completes. */
				debug(D_VINE, "Library %s task id %d is detected to be failed. Killing it.", p->task->provides_library, p->task->task_id);

				/* collect the library process here as we need to iterate procs_running */
				list_push_tail(failed_libraries, p);
			} else {
				reap_process(p, manager);
			}
		}

		/* If p is a library, check to see if any results waiting. */
		while (vine_process_library_get_result(p, &done_task_id, &done_exit_code)) {
			if (itable_lookup(procs_killed_functions, done_task_id) == p) {
				itable_remove(procs_killed_functions, done_task_id);
				p->functions_running--;
				continue;
			}
			fp = itable_lookup(procs_table, done_task_id);
			if (fp) {
				fp->exit_code = done_exit_code;
				reap_process(fp, manager);
			}
		}
	}

	while ((p = list_pop_head(failed_libraries))) {
		handle_failed_library_process(p, manager);
	}

	list_delete(failed_libraries);

	return 1;
}

/*
For a task run locally, if the resources are all set to -1,
then assume that the task occupies all worker resources.
Otherwise, just make sure all values are non-zero.
*/

static void normalize_resources(struct vine_process *p)
{
	struct vine_task *t = p->task;

	if (t->resources_requested->cores < 0 && t->resources_requested->memory < 0 && t->resources_requested->disk < 0 && t->resources_requested->gpus < 0) {
		t->resources_requested->cores = total_resources->cores.total;
		t->resources_requested->memory = total_resources->memory.total;
		t->resources_requested->disk = total_resources->disk.total;
		t->resources_requested->gpus = total_resources->gpus.total;
	} else {
		t->resources_requested->cores = MAX(t->resources_requested->cores, 0);
		t->resources_requested->memory = MAX(t->resources_requested->memory, 0);
		t->resources_requested->disk = MAX(t->resources_requested->disk, 0);
		t->resources_requested->gpus = MAX(t->resources_requested->gpus, 0);
	}
}

/*
Handle an incoming task message from the manager.
Generate a vine_process wrapped around a vine_task,
and deposit it into the waiting list.
*/

static struct vine_task *do_task_body(struct link *manager, int task_id, time_t stoptime)
{
	char line[VINE_LINE_MAX];
	char localname[VINE_LINE_MAX];
	char taskname[VINE_LINE_MAX];
	char taskname_encoded[VINE_LINE_MAX];
	char library_name[VINE_LINE_MAX];
	char category[VINE_LINE_MAX];
	int flags, length, groupid;
	int64_t n;

	timestamp_t nt;

	struct vine_task *task = vine_task_create(0);
	task->task_id = task_id;

	while (recv_message(manager, line, sizeof(line), stoptime)) {
		if (!strcmp(line, "end")) {
			break;
		} else if (sscanf(line, "category %s", category)) {
			vine_task_set_category(task, category);
		} else if (sscanf(line, "cmd %d", &length) == 1) {
			char *cmd = malloc(length + 1);
			link_read(manager, cmd, length, stoptime);
			cmd[length] = 0;
			vine_task_set_command(task, cmd);
			debug(D_VINE, "rx: %s", cmd);
			free(cmd);
		} else if (sscanf(line, "needs_library %s", library_name) == 1) {
			vine_task_set_library_required(task, library_name);
		} else if (sscanf(line, "provides_library %s", library_name) == 1) {
			vine_task_set_library_provided(task, library_name);
		} else if (sscanf(line, "function_slots %" PRId64, &n) == 1) {
			/* Set the number of slots requested by the user. */
			task->function_slots_requested = n;
			/* Also set the total number determined by the manager. */
			task->function_slots_total = n;
		} else if (sscanf(line, "func_exec_mode %" PRId64, &n) == 1) {
			vine_task_func_exec_mode_t func_exec_mode = n;
			if (func_exec_mode == VINE_TASK_FUNC_EXEC_MODE_INVALID) {
				debug(D_VINE | D_NOTICE, "invalid func_exec_mode from manager: %s", line);
				vine_task_delete(task);
				return 0;
			}
			task->func_exec_mode = func_exec_mode;
		} else if (sscanf(line, "infile %s %s %d", localname, taskname_encoded, &flags)) {
			url_decode(taskname_encoded, taskname, VINE_LINE_MAX);
			vine_hack_do_not_compute_cached_name = 1;
			vine_task_add_input_file(task, localname, taskname, flags);
		} else if (sscanf(line, "outfile %s %s %d", localname, taskname_encoded, &flags)) {
			url_decode(taskname_encoded, taskname, VINE_LINE_MAX);
			vine_hack_do_not_compute_cached_name = 1;
			vine_task_add_output_file(task, localname, taskname, flags);
		} else if (sscanf(line, "cores %" PRId64, &n)) {
			vine_task_set_cores(task, n);
		} else if (sscanf(line, "memory %" PRId64, &n)) {
			vine_task_set_memory(task, n);
		} else if (sscanf(line, "disk %" PRId64, &n)) {
			vine_task_set_disk(task, n);
		} else if (sscanf(line, "gpus %" PRId64, &n)) {
			vine_task_set_gpus(task, n);
		} else if (sscanf(line, "groupid %d", &groupid)) {
			task->group_id = groupid;
		} else if (sscanf(line, "wall_time %" PRIu64, &nt)) {
			vine_task_set_time_max(task, nt);
		} else if (sscanf(line, "end_time %" PRIu64, &nt)) {
			vine_task_set_time_end(task, nt * USECOND); // end_time needs it usecs
		} else if (sscanf(line, "env %d", &length) == 1) {
			char *env = malloc(length + 2); /* +2 for \n and \0 */
			link_read(manager, env, length + 1, stoptime);
			env[length] = 0; /* replace \n with \0 */
			char *value = strchr(env, '=');
			if (value) {
				*value = 0;
				value++;
				vine_task_set_env_var(task, env, value);
			}
			free(env);
		} else {
			debug(D_VINE | D_NOTICE, "invalid command from manager: %s", line);
			vine_task_delete(task);
			return 0;
		}
	}

	return task;
}

/* Handle the receipt of a task description and add it to the proper data structures. */

static int do_task(struct link *manager, int task_id, time_t stoptime)
{
	struct vine_task *task = do_task_body(manager, task_id, stoptime);
	if (!task)
		return 0;

	last_task_received = task->task_id;

	vine_process_type_t type = VINE_PROCESS_TYPE_STANDARD;

	if (task->needs_library) {
		type = VINE_PROCESS_TYPE_FUNCTION;
	} else if (task->provides_library) {
		type = VINE_PROCESS_TYPE_LIBRARY;
	} else {
		type = VINE_PROCESS_TYPE_STANDARD;
	}

	struct vine_process *p = vine_process_create(task, type);
	if (!p)
		return 0;

	itable_insert(procs_table, task_id, p);

	normalize_resources(p);

	list_push_tail(procs_waiting, p);
	vine_watcher_add_process(watcher, p);

	return 1;
}

/*
Handle a request to put a file by receiving the file stream
into a temporary transfer path, and then (if successful)
add the file to the cache manager.
*/

static int do_put(struct link *manager, const char *cachename, vine_cache_level_t cache_level, int64_t expected_size)
{
	int64_t actual_size = 0;
	int mode = 0;
	int mtime = 0;

	char *transfer_dir = vine_cache_transfer_path(cache_manager, ".");
	char *transfer_path = vine_cache_transfer_path(cache_manager, cachename);

	timestamp_t start = timestamp_get();
	char *error_message = NULL;
	int r = vine_transfer_get_any(manager, transfer_dir, &actual_size, &mode, &mtime, options->active_timeout, &error_message);
	timestamp_t stop = timestamp_get();

	/* XXX actual_size should equal expected size, but only for a simple file, not a dir. */

	if (r) {
		vine_cache_add_file(cache_manager, cachename, transfer_path, cache_level, mode, actual_size, mtime, start, stop - start, manager);
	} else {
		trash_move(transfer_path);
	}

	free(transfer_path);
	free(transfer_dir);

	return r;
}

/*
Accept a url specification and queue it for later transfer.
*/

static int do_put_url(const char *cache_name, vine_cache_level_t cache_level, int64_t size, int mode, const char *source)
{
	return vine_cache_add_transfer(cache_manager, cache_name, source, cache_level, mode, size, VINE_CACHE_FLAGS_ON_TASK);
}

/*
Accept a url specification and transfer immediately.
*/

static int do_put_url_now(const char *cache_name, vine_cache_level_t cache_level, int64_t size, int mode, const char *source)
{
	return vine_cache_add_transfer(cache_manager, cache_name, source, cache_level, mode, size, VINE_CACHE_FLAGS_NOW);
}

/*
Accept a mini_task that is executed on demand.
We will then extract the file "source" from the sandbox in order to produce "cache_name".
*/

static int do_put_mini_task(struct link *manager, time_t stoptime, const char *cache_name, vine_cache_level_t cache_level, int64_t size, int mode, const char *source)
{
	mini_task_id++;

	struct vine_task *mini_task = do_task_body(manager, mini_task_id, stoptime);
	if (!mini_task)
		return 0;

	return vine_cache_add_mini_task(cache_manager, cache_name, source, mini_task, cache_level, mode, size);
}

/*
The manager has requested the deletion of a file in the cache
directory.  If the request is valid, then move the file to the
trash and deal with it there.
*/

static int do_unlink(struct link *manager, const char *path)
{
	char *cached_path = vine_cache_data_path(cache_manager, path);

	int result = 0;

	if (path_within_dir(cached_path, workspace->workspace_dir)) {
		vine_cache_remove(cache_manager, path, manager);
		result = 1;
	} else {
		debug(D_VINE, "%s is not within workspace %s", cached_path, workspace->workspace_dir);
		result = 0;
	}

	free(cached_path);
	return result;
}

/*
do_kill removes a process currently known by the worker.
Note that a kill message from the manager is used for every case
where a task is to be removed, whether it is waiting, running,
of finished.  Regardless of the state, we kill the process and
remove all of the associated files and other state.
*/

static int do_kill(int task_id)
{
	struct vine_process *p;

	p = itable_remove(procs_table, task_id);

	if (!p) {
		debug(D_VINE, "manager requested kill of task %d which does not exist!", task_id);
		return 1;
	}

	if (itable_remove(procs_running, task_id)) {
		/* The functions of a library end with it, so they are reaped while the library process still exists. */
		if (p->type == VINE_PROCESS_TYPE_LIBRARY) {
			reap_library_functions(p);
		}
		/* The process is not waited for here, which would stop the main loop until it exits. Its sandbox goes to the
		 * trash below, so a task sent again under the same id gets a new one, and its exit status is collected later. */
		vine_process_kill(p);
		if (p->pid > 0) {
			itable_insert(procs_killed, p->pid, (void *)(uintptr_t)p->pid);
		}
		reap_process(p, /* do not stage out */ NULL);
		if (p->type == VINE_PROCESS_TYPE_FUNCTION) {
			p->library_process->functions_running++;
			itable_insert(procs_killed_functions, task_id, p->library_process);
			if (p->library_process->library_can_cancel) {
				vine_process_cancel_function(p);
			}
		}
	}

	itable_remove(procs_complete, p->task->task_id);
	list_remove(procs_waiting, p);
	list_remove(procs_stageout, p);

	vine_watcher_remove_process(watcher, p);

	vine_process_delete(p);

	return 1;
}

/*
Collect the exit status of processes that do_kill() dropped. Without waiting, collect only those that exited. With
waiting, as when disconnecting, wait for each, which is short because each was sent SIGKILL.
*/

static void reap_killed_processes(int wait)
{
	uint64_t pid;
	void *dummy;
	int iteration;
	ITABLE_ITERATE(procs_killed, iteration, pid, dummy)
	{
		pid_t result;
		do {
			result = waitpid((pid_t)pid, NULL, wait ? 0 : WNOHANG);
		} while (wait && result < 0 && errno == EINTR);
		/* An error other than an interruption means the process is no longer a child to wait for. */
		if (result != 0) {
			itable_remove(procs_killed, pid);
		}
	}
}

/*
Kill off all known tasks by iterating over the complete
procs_table and calling do_kill.  This should result in
all empty procs_* structures and zero resources allocated.
If this failed to bring the system back to a fresh state,
then we need to abort to clean things up.
*/

static void kill_all_tasks()
{
	struct vine_process *p;
	uint64_t task_id;
	int iteration;

	ITABLE_ITERATE(procs_table, iteration, task_id, p)
	{
		do_kill(task_id);
	}

	assert(itable_size(procs_table) == 0);
	assert(itable_size(procs_running) == 0);
	assert(itable_size(procs_complete) == 0);
	assert(list_size(procs_waiting) == 0);
	assert(list_size(procs_stageout) == 0);
	assert(cores_allocated == 0);
	assert(memory_allocated == 0);
	assert(disk_allocated == 0);
	assert(gpus_allocated == 0);

	debug(D_VINE, "all data structures are clean");
}

/*
We check maximum_running_time by itself (not in enforce_processes_limits),
as other running tasks should not be affected by a task timeout.
*/

static void enforce_processes_max_running_time()
{
	struct vine_process *p;
	uint64_t task_id;
	int iteration;

	timestamp_t now = timestamp_get();

	ITABLE_ITERATE(procs_running, iteration, task_id, p)
	{

		/* If the task did not set wall_time, return right away. */
		if (p->task->resources_requested->wall_time < 1)
			continue;

		if (now > p->execution_start + (1e6 * p->task->resources_requested->wall_time)) {
			debug(D_VINE,
					"Task %d went over its running time limit: %s > %s\n",
					p->task->task_id,
					rmsummary_resource_to_str("wall_time", (now - p->execution_start) / 1e6, 1),
					rmsummary_resource_to_str("wall_time", p->task->resources_requested->wall_time, 1));
			p->result = VINE_RESULT_MAX_WALL_TIME;
			vine_process_kill(p);
		}
	}

	return;
}

/* Handle a release message from the manager, asking the worker to cleanly exit. */

static int do_release()
{
	debug(D_VINE, "released by manager %s:%d.\n", current_manager_address->addr, current_manager_address->port);
	released_by_manager = 1;
	return 0;
}

/* Handle an unexpected disconnection by the current manager, and clean up everything. */

static void disconnect_manager(struct link *manager)
{
	debug(D_VINE, "disconnecting from manager %s:%d", current_manager_address->addr, current_manager_address->port);

	/* Queued messages, such as info vacating, go out before the link closes, unless the link fails. */
	flush_async_messages(manager, time(0) + options->active_timeout);
	link_close(manager);

	/* Messages that this manager did not take belong to its session, and a partly sent one would corrupt the next. */
	char *message;
	while ((message = list_pop_head(pending_async_messages))) {
		free(message);
	}
	pending_async_offset = 0;
	pending_async_link = 0;

	debug(D_VINE, "killing all outstanding tasks");
	kill_all_tasks();
	reap_killed_processes(1);

	if (released_by_manager) {
		released_by_manager = 0;
	} else if (abort_flag) {
		// Bail out quickly
	} else {
		sleep(5);
	}
}

void send_stdout(struct link *l, int64_t task_id)
{
	char line[VINE_LINE_MAX];
	struct vine_process *p = itable_lookup(procs_table, task_id);
	if (!p) {
		strcpy(line, "process unknown: no stdout");
		errno = ESRCH;
		send_message(l, "error %s %d\n", line, errno);
		return;
	}

	int output_file = open(p->output_file_name, O_RDONLY);
	if (output_file < 0) {
		send_message(l, "error %s %d\n", p->output_file_name, errno);
		return;
	}

	send_message(l, "stdout %" SCNd64 " %lld\n", task_id, (long long)p->output_length);

	if (output_file >= 0) {
		link_stream_from_fd(l, output_file, p->output_length, time(0) + options->active_timeout);
		close(output_file);
	}
	return;
}

/* Handle the next incoming message from the currently connected manager. */

static int handle_manager(struct link *manager)
{
	char line[VINE_LINE_MAX];
	char filename_encoded[VINE_LINE_MAX];
	char filename[VINE_LINE_MAX];
	char source_encoded[VINE_LINE_MAX];
	char source[VINE_LINE_MAX];
	char transfer_id[VINE_LINE_MAX];
	int64_t length;
	int64_t task_id = 0;
	uint64_t token;
	int mode, n;
	int r = 0;
	int cache_level;

	if (recv_message(manager, line, sizeof(line), options->idle_stoptime)) {
		if (sscanf(line, "task %" SCNd64, &task_id) == 1) {
			r = do_task(manager, task_id, time(0) + options->active_timeout);
		} else if (sscanf(line, "connect-back %" SCNu64, &token) == 1) {
			/* A failure only fails this receive, which the manager retries. */
			vine_transfer_server_connect_back(cache_manager, manager, token);
			r = 1;
		} else if (sscanf(line, "manager_transfer_port %" SCNd64, &length) == 1) {
			char manager_ip[LINK_ADDRESS_MAX];
			int manager_port;
			r = length > 0 && length <= 65535 && link_address_remote(manager, manager_ip, &manager_port) &&
				vine_cache_set_manager_transfer(cache_manager, manager_ip, (int)length);
		} else if (sscanf(line, "put %s %d %" SCNd64, filename_encoded, &cache_level, &length) == 3) {
			url_decode(filename_encoded, filename, sizeof(filename));
			r = do_put(manager, filename, cache_level, length);
			reset_idle_timer();
		} else if (sscanf(line, "puturl %s %s %d %" SCNd64 " %o %s", source_encoded, filename_encoded, &cache_level, &length, &mode, transfer_id) == 6) {
			url_decode(filename_encoded, filename, sizeof(filename));
			url_decode(source_encoded, source, sizeof(source));
			r = do_put_url(filename, cache_level, length, mode, source);
			reset_idle_timer();
			hash_table_insert(current_transfers, filename, strdup(transfer_id));
		} else if (sscanf(line, "puturl_now %s %s %d %" SCNd64 " %o %s", source_encoded, filename_encoded, &cache_level, &length, &mode, transfer_id) == 6) {
			url_decode(filename_encoded, filename, sizeof(filename));
			url_decode(source_encoded, source, sizeof(source));
			r = do_put_url_now(filename, cache_level, length, mode, source);
			reset_idle_timer();
			hash_table_insert(current_transfers, filename, strdup(transfer_id));
		} else if (sscanf(line, "mini_task %s %s %d %" SCNd64 " %o", source_encoded, filename_encoded, &cache_level, &length, &mode) == 5) {
			url_decode(source_encoded, source, sizeof(source));
			url_decode(filename_encoded, filename, sizeof(filename));
			r = do_put_mini_task(manager, time(0) + options->active_timeout, filename, cache_level, length, mode, source);
			reset_idle_timer();
		} else if (sscanf(line, "unlink %s", filename_encoded) == 1) {
			url_decode(filename_encoded, filename, sizeof(filename));
			r = do_unlink(manager, filename);
		} else if (sscanf(line, "getfile %s", filename_encoded) == 1) {
			url_decode(filename_encoded, filename, sizeof(filename));
			r = vine_transfer_put_any(manager, cache_manager, filename, VINE_TRANSFER_MODE_FILE_ONLY, time(0) + options->active_timeout);
		} else if (sscanf(line, "get %s", filename_encoded) == 1) {
			url_decode(filename_encoded, filename, sizeof(filename));
			r = vine_transfer_put_any(manager, cache_manager, filename, VINE_TRANSFER_MODE_ANY, time(0) + options->active_timeout);
		} else if (sscanf(line, "kill %" SCNd64, &task_id) == 1) {
			if (task_id >= 0) {
				r = do_kill(task_id);
			} else {
				kill_all_tasks();
				r = 1;
			}
		} else if (!strncmp(line, "release", 8)) {
			r = do_release();
		} else if (!strncmp(line, "exit", 5)) {
			abort_flag = 1;
			r = 1;
		} else if (!strncmp(line, "check", 6)) {
			r = send_keepalive(manager, 0);
		} else if (!strncmp(line, "auth", 4)) {
			fprintf(stderr, "vine_worker: this manager requires a password. (use the -P option)\n");
			r = 0;
		} else if (sscanf(line, "send_results %d", &n) == 1) {
			report_changes(manager);
			r = 1;
		} else if (sscanf(line, "send_stdout %" SCNd64 "", &task_id) == 1) {
			send_stdout(manager, task_id);
			r = 1;
		} else {
			debug(D_VINE, "Unrecognized manager message: %s.\n", line);
			r = 0;
		}
	} else {
		debug(D_VINE, "Failed to read from manager.\n");
		r = 0;
	}

	return r;
}

/*
Return true if this task can run with the resources currently available.
*/

static int task_resources_fit_now(struct vine_task *t)
{
	return (cores_allocated + t->resources_requested->cores <= total_resources->cores.total) &&
	       (memory_allocated + t->resources_requested->memory <= total_resources->memory.total) &&
	       ((t->needs_library || disk_allocated + t->resources_requested->disk <= total_resources->disk.total)) && (gpus_allocated + t->resources_requested->gpus <= total_resources->gpus.total);
	// XXX Disk is constantly shrinking, and library disk requests are currently static. Once we generate some files things will hang.
	// Known limitation: the disk check above is skipped entirely whenever t->needs_library is
	// true, so library/function-call tasks are never weighed against total_resources->disk.total.
	// As functions write output/temp files, worker disk keeps shrinking with no enforcement for
	// this task class. A real fix needs a real (even approximate) per-function disk accounting
	// so this branch can re-enable the disk.total check for library/function tasks instead of
	// bypassing it. Left as a known gap rather than a tracked issue for now.
}

/*
Return true if this task can eventually run with the resources available. For
example, this is needed for when the worker is launched without the --memory
option, and the free available memory of the system is consumed by some other
process.
*/

static int task_resources_fit_eventually(struct vine_task *t)
{
	struct vine_resources *r;

	r = total_resources;

	return (t->resources_requested->cores <= r->cores.total) && (t->resources_requested->memory <= r->memory.total) && (t->resources_requested->disk <= r->disk.total) &&
	       (t->resources_requested->gpus <= r->gpus.total);
}

/*
Find a suitable library process that can provide a slot to run this library right NOW.
*/

static struct vine_process *find_running_library_for_function(const char *library_name)
{
	uint64_t task_id;
	struct vine_process *p;
	int iteration;

	ITABLE_ITERATE(procs_running, iteration, task_id, p)
	{
		if (p->task->provides_library && !strcmp(p->task->provides_library, library_name)) {
			if (p->library_ready && p->functions_running < p->task->function_slots_total) {
				return p;
			}
		}
	}

	return 0;
}

/*
Return true if this process is ready to run at this moment, and match to a library process if needed.
*/

static int process_ready_to_run_now(struct vine_process *p, struct vine_cache *cache, struct link *manager)
{
	if (!task_resources_fit_now(p->task))
		return 0;

	if (p->task->needs_library) {
		if (itable_lookup(procs_killed_functions, p->task->task_id)) {
			return 0;
		}

		/* Here is where we attach a function to a specific library. */
		p->library_process = find_running_library_for_function(p->task->needs_library);
		if (!p->library_process)
			return 0;
	}

	vine_cache_status_t status = vine_sandbox_ensure(p, cache, manager, procs_table);
	if (status != VINE_CACHE_STATUS_READY)
		return 0;

	return 1;
}

/*
Find a suitable library process that could serve this function in the future.
*/

static struct vine_process *find_future_library_for_function(const char *library_name)
{
	uint64_t task_id;
	struct vine_process *p;
	int iteration;

	ITABLE_ITERATE(procs_table, iteration, task_id, p)
	{
		if (p->task->provides_library && !strcmp(p->task->provides_library, library_name)) {
			return p;
		}
	}
	return 0;
}

/*
Return true if this process can run eventually, supposing that other processes will complete.
*/

static int process_can_run_eventually(struct vine_process *p, struct vine_cache *cache, struct link *manager)
{
	if (!task_resources_fit_eventually(p->task)) {
		debug(D_VINE, "task %d does not fit the total resources", p->task->task_id);
		return 0;
	}

	if (p->task->needs_library) {
		/* Note that we check for *some* library but do not bind to it. */
		struct vine_process *p_future = find_future_library_for_function(p->task->needs_library);
		if (!p_future || p_future->result == VINE_RESULT_LIBRARY_EXIT) {
			debug(D_VINE, "task %d does not match any library \"%s\"", p->task->task_id, p->task->needs_library);
			return 0;
		}
	}

	vine_cache_status_t status = vine_sandbox_ensure(p, cache, manager, procs_table);
	switch (status) {
	case VINE_CACHE_STATUS_FAILED:
	case VINE_CACHE_STATUS_UNKNOWN:
		debug(D_VINE, "task %d requires failed input cache transfers", p->task->task_id);
		return 0;
	default:
		break;
	}

	return 1;
}

void forsake_waiting_process(struct link *manager, struct vine_process *p)
{
	/* the task cannot run in this worker */
	p->result = VINE_RESULT_FORSAKEN;
	itable_insert(procs_complete, p->task->task_id, p);

	debug(D_VINE, "Waiting task %d has been forsaken.", p->task->task_id);

	/* we also send updated resources to the manager. */
	send_keepalive(manager, 1);
}

/*
If 0, the worker is using more resources than promised. 1 if resource usage holds that promise.
*/

static int enforce_worker_limits(struct link *manager)
{
	if (options->disk_total > 0 && total_resources->disk.inuse > options->disk_total) {
		fprintf(stderr,
				"vine_worker: %s used more than declared disk space (--disk - < disk used) %" PRIu64 " < %lf MB\n",
				workspace->workspace_dir,
				options->disk_total,
				total_resources->disk.inuse);

		if (manager) {
			send_message(manager, "info disk_exhausted %lf\n", total_resources->disk.inuse);
		}

		return 0;
	}

	if (options->memory_total > 0 && total_resources->memory.inuse > options->memory_total) {
		fprintf(stderr,
				"vine_worker: used more than declared memory (--memory < memory used) %" PRIu64 " < %lf MB\n",
				options->memory_total,
				total_resources->memory.inuse);

		if (manager) {
			send_message(manager, "info memory_exhausted %lf\n", total_resources->memory.inuse);
		}

		return 0;
	}

	return 1;
}

/*
If 0, the worker has less resources than promised. 1 otherwise.
*/

static int enforce_worker_promises(struct link *manager)
{
	if (options->end_time > 0 && timestamp_get() > ((uint64_t)options->end_time)) {
		warn(D_NOTICE, "vine_worker: reached the wall time limit %" PRIu64 " s\n", (uint64_t)options->manual_wall_time_option);
		if (manager) {
			send_message(manager, "info wall_time_exhausted %" PRIu64 "\n", (uint64_t)options->manual_wall_time_option);
		}
		return 0;
	}

	if (options->disk_total > 0 && total_resources->disk.total < options->disk_total) {
		fprintf(stderr,
				"vine_worker: has less than the promised disk space (--disk > disk total) %" PRIu64 " < %lf MB\n",
				options->disk_total,
				total_resources->disk.total);

		if (manager) {
			send_message(manager, "info disk_error %lf\n", total_resources->disk.total);
		}

		return 0;
	}

	return 1;
}

/*
Given a freshly started process, wait for it to initialize and send
back the library startup message with JSON containing the name of
the library, which should match the task's provides_library label.
*/

static int check_library_startup(struct vine_process *p)
{
	char buffer_len[VINE_LINE_MAX];
	int length = 0;

	/* Read a line that gives the length of the response message. */
	if (!link_readline(p->library_read_link, buffer_len, VINE_LINE_MAX, LINK_NOWAIT)) {
		return 0;
	}
	sscanf(buffer_len, "%d", &length);

	/* Now read that length of message and null-terminate it. */
	char buffer[length + 1];
	if (link_read(p->library_read_link, buffer, length, LINK_NOWAIT) <= 0) {
		return 0;
	}
	buffer[length] = 0;

	/* Check that the response is JX and contains the expected name. */
	struct jx *response = jx_parse_string(buffer);

	const char *name = jx_lookup_string(response, "name");
	int taskid = jx_lookup_integer(response, "taskid");
	const char *exec_mode = jx_lookup_string(response, "exec_mode");

	int ok = 1;

	if (!name || !taskid || !exec_mode) {
		ok = 0;
	} else {
		vine_task_func_exec_mode_t converted_exec_mode = vine_task_func_exec_mode_from_string(exec_mode);

		if (!p->task->provides_library || strcmp(name, p->task->provides_library)) {
			ok = 0;
		}
		if (taskid != p->task->task_id) {
			ok = 0;
		}
		if (p->task->func_exec_mode && converted_exec_mode != p->task->func_exec_mode) {
			ok = 0;
		}
		p->library_can_cancel = jx_lookup_boolean(response, "cancel");
	}
	if (response) {
		jx_delete(response);
	}
	return ok;
}

/* Check whether all known libraries are ready to execute functions.
 * A library starts up and tells the vine_worker it's ready by reporting
 * back its library name. */

static void check_libraries_ready(struct link *manager)
{
	uint64_t library_task_id;
	struct vine_process *library_process;
	int iteration;
	struct list *failed_libraries = list_create();

	struct link_info library_link_info;
	library_link_info.events = LINK_READ;
	library_link_info.revents = 0;

	/* Loop through all processes to find libraries and check if they are alive. */
	ITABLE_ITERATE(procs_running, iteration, library_task_id, library_process)
	{
		/* Skip non-library processes or libraries that are already ready */
		if (library_process->type != VINE_PROCESS_TYPE_LIBRARY || library_process->library_ready)
			continue;

		library_link_info.link = library_process->library_read_link;

		/* Check if library has sent its startup message. */
		if (link_poll(&library_link_info, 1, 0) && (library_link_info.revents & LINK_READ)) {
			if (check_library_startup(library_process)) {
				debug(D_VINE, "Library %s reports ready to execute functions.", library_process->task->provides_library);
				library_process->library_ready = 1;
			} else {
				/* Collect failed libraries so procs_running is not re-iterated here. */
				debug(D_VINE,
						"Library %s task id %" PRIu64 " verification failed (unexpected response). Killing it.",
						library_process->task->provides_library,
						library_task_id);
				list_push_tail(failed_libraries, library_process);
			}
		} else {
			/* The library is running and the link has no readable data, do nothing until the
			 * handle_completed_tasks detects its failure or it sends back the startup message */
		}

		library_link_info.revents = 0;
	}

	while ((library_process = list_pop_head(failed_libraries))) {
		handle_failed_library_process(library_process, manager);
	}

	list_delete(failed_libraries);
}

/* Signal handlers write a byte here, so a signal that arrives just before the main loop waits still ends the wait. */
static int wake_pipe[2] = {-1, -1};
static struct link *wake_link = NULL;

static void wake_main_loop(void)
{
	int saved_errno = errno;
	/* A failed write means the pipe is full, which already wakes the loop. */
	if (write(wake_pipe[1], "", 1) < 0) {
	}
	errno = saved_errno;
}

/*
Each round of the main loop waits for the manager, and then works for at most a budget of one second before it waits
again. A step that may take long, such as measuring the disk, measuring outputs as they move into the cache, emptying
the trash, or reading many buffered messages, checks the budget between items and continues in the next round, so a
request of the manager never waits long behind background work. When a round leaves work, the next round does not wait
for the manager.

Some steps remain unbounded by design: hard-linking a directory input into a sandbox (file_link_recursive) and single
system calls.
*/
#define ROUND_BUDGET USECOND

/* Start working for the (newly connected) manager on this given link. */

static void vine_worker_serve_manager(struct link *manager)
{
	debug(D_VINE, "working for manager at %s:%d.\n", current_manager_address->addr, current_manager_address->port);

	reset_idle_timer();

	/* Non-zero when the last round left work that continues in the next round, which then does not wait. */
	int work_remains = 0;

	// Start serving managers
	while (!abort_flag) {

		/* Propose a disconnect from the manager, but do not do it until requested */
		if (time(0) > options->idle_stoptime) {
			debug(D_NOTICE,
					"requesting disconnect from %s:%d because I did not receive any task in %d seconds (--idle-timeout).\n",
					current_manager_address->addr,
					current_manager_address->port,
					options->idle_timeout);
			send_async_message(manager, "info idle-disconnect-request %lld\n", (long long)options->idle_timeout);
			reset_idle_timer();
		}

		if (options->initial_ppid != 0 && getppid() != options->initial_ppid) {
			debug(D_NOTICE, "parent process exited, shutting down\n");
			break;
		}

		/* Wait for the manager or a signal. A flag set before the wait skips it, and the wake pipe ends it for a
		 * signal that arrives in between. Work left by the last round also skips it. */
		int wait_msec = 5000;

		if (sigchld_received_flag) {
			wait_msec = 0;
			sigchld_received_flag = 0;
		}
		if (work_remains) {
			wait_msec = 0;
		}

		/* Queued messages also wait for the manager to make room for them. */
		int manager_events = LINK_READ | (list_size(pending_async_messages) ? LINK_WRITE : 0);
		struct link_info waits[2] = {{manager, manager_events, 0}, {wake_link, LINK_READ, 0}};
		int ready = link_poll(waits, wake_link ? 2 : 1, wait_msec);
		char drain[64];
		while (wake_link && read(wake_pipe[0], drain, sizeof(drain)) > 0) {
		}
		/* Messages that an earlier round left in the link's buffer do not make the socket readable. */
		int manager_activity = (ready > 0 && (waits[0].revents & LINK_READ)) || !link_buffer_empty(manager);

		struct budget round;
		budget_start(&round, ROUND_BUDGET);

		/* Handle the messages already in the link's buffer until the budget expires, as the manager does, so that a
		 * long task, such as one with thousands of inputs, is read at once rather than one line per round. */
		int ok = 1;
		if (manager_activity) {
			do {
				ok &= handle_manager(manager);
			} while (ok && !abort_flag && !link_buffer_empty(manager) && !budget_expired(&round));
		}

		expire_procs_running();

		ok &= handle_completed_tasks(manager);
		reap_killed_processes(0);

		continue_stageout(manager, &round);

		ok &= vine_cache_check_xfer_files(cache_manager, manager);

		measure_worker_resources();

		if (!enforce_worker_promises(manager)) {
			finish_running_tasks(VINE_RESULT_FORSAKEN);
			abort_flag = 1;
			break;
		}

		enforce_processes_max_running_time();

		/* end a running processes if goes above its declared limits.
		 * Mark offending process as RESOURCE_EXHASTION. */
		continue_disk_measurement(&round);

		/* end running processes if worker resources are exhasusted, and marked
		 * them as FORSAKEN, so they can be resubmitted somewhere else. */
		if (!enforce_worker_limits(manager)) {
			finish_running_tasks(VINE_RESULT_FORSAKEN);
			// finish all tasks, disconnect from manager, but don't kill the worker (no abort_flag = 1)
			break;
		}

		/* Periodically start some pending transfers. */
		vine_cache_start_transfers(cache_manager);

		/* Check all known libraries if they are ready to execute functions. */
		check_libraries_ready(manager);

		/* deliver queued asynchronous messages if available */
		deliver_async_messages(manager);

		int task_event = 0;
		if (ok) {
			struct vine_process *p;
			int visited;
			int waiting = list_size(procs_waiting);

			for (visited = 0; visited < waiting; visited++) {
				p = list_pop_head(procs_waiting);
				if (!p) {
					break;
				} else if (process_ready_to_run_now(p, cache_manager, manager)) {
					start_process(p, manager);
					task_event++;
				} else if (process_can_run_eventually(p, cache_manager, manager)) {
					list_push_tail(procs_waiting, p);
				} else {
					forsake_waiting_process(manager, p);
					task_event++;
				}
			}
		}

		if (ok) {
			if (vine_watcher_check(watcher)) {
				send_async_message(manager, "available_results\n");
			}
			if (itable_size(procs_complete) > 0) {
				send_complete_tasks(manager);
			}
			if (task_event > 0) {
				send_stats_update(manager);
			}
		}

		/* Delete what was moved to the trash, after everything else this round. */
		int trash_done = trash_empty_budget(&round);

		if (!ok) {
			break;
		}

		work_remains = !link_buffer_empty(manager) || list_size(procs_stageout) > 0 || disk_measurement_active || !trash_done;

		// Reset options->idle_stoptime if something interesting is happening at this worker.
		if (list_size(procs_waiting) > 0 || itable_size(procs_table) > 0 || itable_size(procs_complete) > 0) {
			reset_idle_timer();
		}
	}
}

/* Attempt to connect, authenticate, and work with the manager at this specific host and port. */

static int vine_worker_serve_manager_by_hostport(const char *host, int port, const char *verify_project, int use_ssl)
{
	if (!domain_name_cache_lookup(host, current_manager_address->addr)) {
		fprintf(stderr, "couldn't resolve hostname %s", host);
		return 0;
	}

	/*
	For the preliminary steps of password and project verification, we use the
	idle timeout, because we have not yet been assigned any work and should
	leave if the manager is not responsive.

	It is tempting to use a short timeout here, but DON'T. The name and
	password messages are ayncronous; if the manager is busy handling other
	workers, a short window is not enough for a response to come back.
	*/

	reset_idle_timer();

	struct link *manager = link_connect(current_manager_address->addr, port, options->idle_stoptime);

	if (!manager) {
		fprintf(stderr, "couldn't connect to %s:%d: %s\n", current_manager_address->addr, port, strerror(errno));
		return 0;
	}

	if (options->ssl_requested && !use_ssl) {
		fprintf(stderr, "vine_worker: --ssl was given, but manager %s:%d is not using ssl.\n", host, port);
		link_close(manager);
		return 0;
	} else if (options->ssl_requested || use_ssl) {
		const char *sni_host = options->tls_sni;
		if (!sni_host) {
			sni_host = host;
		}

		if (link_ssl_wrap_connect(manager, sni_host) < 1) {
			fprintf(stderr, "vine_worker: could not setup ssl connection.\n");
			link_close(manager);
			return 0;
		}
	}

	link_tune(manager, LINK_TUNE_INTERACTIVE);

	char local_addr[LINK_ADDRESS_MAX];
	int local_port;
	link_address_local(manager, local_addr, &local_port);

	printf("connected to manager %s:%d via local address %s:%d\n", host, port, local_addr, local_port);
	debug(D_VINE, "connected to manager %s:%d via local address %s:%d", host, port, local_addr, local_port);

	if (options->password) {
		debug(D_VINE, "authenticating to manager");
		if (!link_auth_password(manager, options->password, options->idle_stoptime)) {
			fprintf(stderr, "vine_worker: wrong password for manager %s:%d\n", host, port);
			link_close(manager);
			return 0;
		}
	}

	if (verify_project) {
		char line[VINE_LINE_MAX];
		debug(D_VINE, "verifying manager's project name");
		send_message(manager, "name\n");
		if (!recv_message(manager, line, sizeof(line), options->idle_stoptime)) {
			debug(D_VINE, "no response from manager while verifying name");
			link_close(manager);
			return 0;
		}

		if (strcmp(line, verify_project)) {
			fprintf(stderr, "vine_worker: manager has project %s instead of %s\n", line, verify_project);
			link_close(manager);
			return 0;
		}
	}

	/* Setup the various directories in the workspace. */
	vine_workspace_prepare(workspace);

	/* Start the cache manager and scan for existing files. */
	cache_manager = vine_cache_create(workspace->cache_dir, options->max_transfer_procs);
	vine_cache_load(cache_manager);

	/* Start the transfer server, which serves up the cache directory. */
	if (!vine_transfer_server_start(cache_manager, manager, options->transfer_port_min, options->transfer_port_max)) {
		fprintf(stderr, "vine_worker: unable to bind transfer port (check --transfer-port or cluster permissions)\n");
	}

	measure_worker_resources();

	report_worker_ready(manager);

	vine_worker_serve_manager(manager);

	if (abort_signal_received) {
		send_async_message(manager, "info vacating %d\n", abort_signal_received);
	}

	last_task_received = 0;
	results_to_be_sent_msg = 0;

	disconnect_manager(manager);
	printf("disconnected from manager %s:%d\n", host, port);

	/* Do these in the opposite order of setup: */

	/* Stop the transfer server from serving the cache directory. */
	vine_transfer_server_stop();

	/* A disk measurement in progress belongs to this manager's cache. */
	stop_disk_measurement();

	/* Remove all cached files of workflow or less. */
	vine_cache_prune(cache_manager, VINE_CACHE_LEVEL_WORKFLOW);

	/* Stop the cache manager. */
	vine_cache_delete(cache_manager);
	cache_manager = 0;

	/* Clean up the workspace and remove state from this manager. */
	vine_workspace_cleanup(workspace);

	return 1;
}

/* Attempt to connect and work with any opf the managers in this list. */

static int vine_worker_serve_manager_by_hostport_list(struct list *manager_addresses, int use_ssl)
{
	int result = 0;

	/* keep trying managers in the list, until all manager addresses
	 * are tried, or a succesful connection was done */
	LIST_ITERATE(manager_addresses, current_manager_address)
	{
		result = vine_worker_serve_manager_by_hostport(current_manager_address->host,
				current_manager_address->port,
				/*verify name*/ 0,
				use_ssl);
		if (result) {
			break;
		}
	}

	return result;
}

static struct list *interfaces_to_list(const char *canonical_host_or_addr, int port, struct jx *host_aliases)
{
	struct list *l = list_create();
	struct jx *host_alias;

	int found_canonical = 0;

	if (host_aliases) {
		for (void *i = NULL; (host_alias = jx_iterate_array(host_aliases, &i));) {
			const char *address = jx_lookup_string(host_alias, "address");

			if (!address) {
				warn(D_NOTICE, "Skipping interface entry with missing or invalid 'address' field.");
				continue;
			}

			if (strcmp(canonical_host_or_addr, address) == 0) {
				found_canonical = 1;
			}

			// copy ip addr to hostname to work as if the user had entered a particular ip
			// for the manager.
			struct manager_address *m = calloc(1, sizeof(*m));
			strncpy(m->host, address, DOMAIN_NAME_MAX - 1);
			m->port = port;

			list_push_tail(l, m);
		}
	}

	if (host_aliases && !found_canonical) {
		warn(D_NOTICE, "Did not find the manager address '%s' in the list of interfaces.", canonical_host_or_addr);
	}

	if (!found_canonical) {
		/* We get here if no interfaces were defined, or if canonical_host_or_addr was not found in the
		 * interfaces. */

		struct manager_address *m = calloc(1, sizeof(*m));
		strncpy(m->host, canonical_host_or_addr, DOMAIN_NAME_MAX - 1);
		m->port = port;

		list_push_tail(l, m);
	}

	return l;
}

/* Attempt to connect and work with managers found in the catalog matching a project regex. */

static int vine_worker_serve_manager_by_name(const char *catalog_hosts, const char *project_regex)
{
	struct list *managers_list = vine_catalog_query_cached(catalog_hosts, -1, project_regex);

	debug(D_VINE, "project name %s matches %d managers", project_regex, list_size(managers_list));

	if (list_size(managers_list) == 0)
		return 0;

	// shuffle the list by r items to distribute the load across managers
	int r = rand() % list_size(managers_list);
	int i;
	for (i = 0; i < r; i++) {
		list_push_tail(managers_list, list_pop_head(managers_list));
	}

	static struct manager_address *last_addr = NULL;

	while (1) {
		struct jx *jx = list_peek_head(managers_list);

		const char *project = jx_lookup_string(jx, "project");
		const char *name = jx_lookup_string(jx, "name");
		const char *addr = jx_lookup_string(jx, "address");
		const char *pref = jx_lookup_string(jx, "manager_preferred_connection");
		struct jx *host_aliases = jx_lookup(jx, "network_interfaces");
		int port = jx_lookup_integer(jx, "port");
		int use_ssl = jx_lookup_boolean(jx, "ssl");

		// give priority to worker's preferred connection option
		if (options->preferred_connection) {
			pref = options->preferred_connection;
		}

		if (last_addr) {
			if (time(0) > options->idle_stoptime && strcmp(addr, last_addr->host) == 0 && port == last_addr->port) {
				if (list_size(managers_list) < 2) {
					free(last_addr);
					last_addr = NULL;

					/* convert options->idle_stoptime into connect_stoptime (e.g., time already
					 * served). */
					options->connect_stoptime = options->idle_stoptime;
					debug(D_VINE, "Previous idle disconnection from only manager available project=%s name=%s addr=%s port=%d", project, name, addr, port);

					return 0;
				} else {
					list_push_tail(managers_list, list_pop_head(managers_list));
					continue;
				}
			}
		}

		int result;

		if (pref && strcmp(pref, "by_hostname") == 0) {
			debug(D_VINE, "selected manager with project=%s hostname=%s addr=%s port=%d", project, name, addr, port);
			manager_addresses = interfaces_to_list(name, port, NULL);
		} else if (pref && strcmp(pref, "by_apparent_ip") == 0) {
			debug(D_VINE, "selected manager with project=%s apparent_addr=%s port=%d", project, addr, port);
			manager_addresses = interfaces_to_list(addr, port, NULL);
		} else {
			debug(D_VINE, "selected manager with project=%s addr=%s port=%d", project, addr, port);
			manager_addresses = interfaces_to_list(addr, port, host_aliases);
		}

		result = vine_worker_serve_manager_by_hostport_list(manager_addresses, use_ssl);

		struct manager_address *m;
		while ((m = list_pop_head(manager_addresses))) {
			free(m);
		}
		list_delete(manager_addresses);
		manager_addresses = NULL;

		if (result) {
			free(last_addr);
			last_addr = calloc(1, sizeof(*last_addr));
			strncpy(last_addr->host, addr, DOMAIN_NAME_MAX - 1);
			last_addr->port = port;
		}

		return result;
	}
}

static void vine_worker_serve_managers()
{
	int backoff_interval = options->init_backoff_interval;

	while (1) {
		int result = 0;

		if (options->initial_ppid != 0 && getppid() != options->initial_ppid) {
			debug(D_NOTICE, "parent process exited, shutting down\n");
			break;
		}

		measure_worker_resources();
		if (!enforce_worker_promises(NULL)) {
			abort_flag = 1;
			break;
		}

		if (options->project_regex) {
			result = vine_worker_serve_manager_by_name(options->catalog_hosts, options->project_regex);
		} else {
			result = vine_worker_serve_manager_by_hostport_list(manager_addresses, /* use ssl only if --ssl */ options->ssl_requested);
		}

		/*
		If the last attempt was a succesful connection, then reset the backoff_interval,
		and the connect timeout, then try again if a project name was given.
		If the connect attempt failed, then slow down the retries.
		*/

		if (result) {
			if (options->single_shot_mode) {
				debug(D_DEBUG, "stopping: single shot mode");
				break;
			}
			backoff_interval = options->init_backoff_interval;
			options->connect_stoptime = time(0) + options->connect_timeout;

			if (!options->project_regex && (time(0) > options->idle_stoptime)) {
				debug(D_NOTICE, "stopping: no other managers available");
				break;
			}
		} else {
			backoff_interval = MIN(backoff_interval * 2, options->max_backoff_interval);
		}

		if (abort_flag) {
			debug(D_NOTICE, "stopping: abort signal received");
			break;
		}

		if (time(0) > options->connect_stoptime) {
			debug(D_NOTICE, "stopping: could not connect after %d seconds.", options->connect_timeout);
			break;
		}

		sleep(backoff_interval);
	}
}

/* Generate a unique worker ID string from local information. */

static char *make_worker_id()
{
	char *salt_and_pepper = string_format("%d%d%d", getpid(), getppid(), rand());

	unsigned char digest[MD5_DIGEST_LENGTH];
	md5_buffer(salt_and_pepper, strlen(salt_and_pepper), digest);

	char *id = string_format("worker-%s", md5_to_string(digest));

	free(salt_and_pepper);

	return id;
}

static void handle_abort(int sig)
{
	abort_flag = 1;
	abort_signal_received = sig;
	wake_main_loop();
}

static void handle_sigchld(int sig)
{
	sigchld_received_flag = 1;
	wake_main_loop();
}

struct list *parse_manager_addresses(const char *specs, int default_port)
{
	struct list *managers = list_create();

	char *managers_args = xxstrdup(specs);

	char *next_manager = strtok(managers_args, ";");
	while (next_manager) {
		char host[DOMAIN_NAME_MAX];
		int port = default_port;

		if (!address_parse_hostport(next_manager, host, &port, default_port)) {
			fatal("Invalid manager address '%s'", next_manager);
		}

		if (port < 1) {
			fatal("Invalid port for manager '%s'", next_manager);
		}

		struct manager_address *m = calloc(1, sizeof(*m));
		snprintf(m->host, sizeof(m->host), "%s", host);
		m->port = port;

		list_push_tail(managers, m);
		next_manager = strtok(NULL, ";");
	}
	free(managers_args);

	return (managers);
}

/* Set up initial shared data structures. */

void vine_worker_create_structures()
{
	procs_table = itable_create(0);
	procs_running = itable_create(0);
	procs_waiting = list_create();
	pending_async_messages = list_create();
	procs_complete = itable_create(0);
	procs_stageout = list_create();
	procs_killed = itable_create(0);
	procs_killed_functions = itable_create(0);
	disk_measurement_tasks = list_create();

	current_transfers = hash_table_create(0, 0);

	watcher = vine_watcher_create();

	total_resources = vine_resources_create();

	worker_id = make_worker_id();
}

/* Final cleanup of all worker structures before exiting */

static void vine_worker_delete_structures()
{
	if (worker_id)
		free(worker_id);

	if (total_resources)
		vine_resources_delete(total_resources);
	if (watcher)
		vine_watcher_delete(watcher);
	if (current_transfers)
		hash_table_delete(current_transfers);

	if (procs_table)
		itable_delete(procs_table);
	if (procs_running)
		itable_delete(procs_running);
	if (procs_complete)
		itable_delete(procs_complete);
	if (procs_stageout)
		list_delete(procs_stageout);
	if (procs_killed)
		itable_delete(procs_killed);
	if (procs_killed_functions)
		itable_delete(procs_killed_functions);
	if (disk_measurement_tasks)
		list_delete(disk_measurement_tasks);
	if (procs_waiting)
		list_delete(procs_waiting);
}

int main(int argc, char *argv[])
{
	/* This must come first in main, allows us to change process titles in ps later. */
	change_process_title_init(argv);

	/* Pass the program name to the debug subsystem */
	debug_config(argv[0]);

	/* Start the clock on the worker operation. */
	worker_start_time = timestamp_get();

	/* The random number generator must be initialized exactly once at startup. */
	random_init();

	/* Allocate all of the data structures to track tasks an files. */
	vine_worker_create_structures();

	/* Create the options structure with defaults. */
	options = vine_worker_options_create();

	/* Now process the command line options */
	vine_worker_options_get(options, argc, argv);

	cctools_version_debug(D_DEBUG, argv[0]);

	/* Print command line when debugging output enabled */
	debug(D_VINE, "worker command line:");
	int i;
	for (i = 0; i < argc; i++) {
		debug(D_VINE, "    argv %d: %s", i, argv[i]);
	}

	/* The caller must either provide a project regex or an explicit manager host and port. */
	if (!options->project_regex) {
		if ((argc - optind) < 1 || (argc - optind) > 2) {
			vine_worker_options_show_help(argv[0], options);
			exit(1);
		}

		int default_manager_port = (argc - optind) == 2 ? atoi(argv[optind + 1]) : 0;
		manager_addresses = parse_manager_addresses(argv[optind], default_manager_port);

		if (list_size(manager_addresses) < 1) {
			vine_worker_options_show_help(argv[0], options);
			fatal("No manager has been specified");
		}
	}

	/* Set up signal handlers so that they call dummy functions that interrupt I/O operations. */
	signal(SIGTERM, handle_abort);
	signal(SIGQUIT, handle_abort);
	signal(SIGINT, handle_abort);
	// Also do cleanup on SIGUSR1 & SIGUSR2 to allow using -notify and -l s_rt= options if submitting
	// this worker process with SGE qsub. Otherwise task processes are left running when SGE
	// terminates this process with SIGKILL.
	signal(SIGUSR1, handle_abort);
	signal(SIGUSR2, handle_abort);
	signal(SIGCHLD, handle_sigchld);

	/* Neither end of the wake pipe blocks, and tasks do not inherit it. */
	if (pipe(wake_pipe) == 0) {
		for (int i = 0; i < 2; i++) {
			fcntl(wake_pipe[i], F_SETFL, O_NONBLOCK);
			fcntl(wake_pipe[i], F_SETFD, FD_CLOEXEC);
		}
		wake_link = link_attach_to_fd(wake_pipe[0]);
	}

	/* Create the workspace directory and move there. */
	workspace = vine_workspace_create(options->workspace_dir);
	if (!workspace) {
		fprintf(stderr, "vine_worker: failed to setup workspace directory.\n");
		exit(1);
	}

	/* Check that programs can actually execute in the workspace, this is an occasional problem with HPCs. */
	if (!vine_workspace_check(workspace)) {
		return 1;
	}

	/* Move to the workspace directory. */
	chdir(workspace->workspace_dir);

	/* If the total number of cores was not set manually, fix it to the observed number of cores. */
	if (options->cores_total < 1) {
		options->cores_total = load_average_get_cpus();
	}

	options->connect_stoptime = time(0) + options->connect_timeout;

	/* Display the available resources once at startup. */
	measure_worker_resources();
	printf("vine_worker: using %lf cores, %lf MB memory, %lf MB disk, %lf gpus\n",
			total_resources->cores.total,
			total_resources->memory.total,
			total_resources->disk.total,
			total_resources->gpus.total);

	/* If a GPU is installed, then display and describe as a feature. */
	char *gpu_name = gpu_name_get();
	if (gpu_name) {
		printf("vine_worker: gpu is called feature \"%s\"\n", gpu_name);
		hash_table_insert(options->features, gpu_name, "feature");
		free(gpu_name);
	}

	/* MAIN LOOP: get to work */
	vine_worker_serve_managers();

	/* Clean up data structures to satisfy valgrind at process exit. */

	vine_workspace_delete(workspace);
	workspace = 0;
	vine_worker_delete_structures();
	vine_worker_options_delete(options);
	options = 0;

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
