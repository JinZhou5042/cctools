#ifndef VINE_GRAPH_EXECUTOR_H
#define VINE_GRAPH_EXECUTOR_H

/* Execute one workflow graph on a TaskVine Manager. This is the interface every workflow frontend uses, whatever its
 * language. The frontend describes nodes and the files they read and write. The executor declares those files, runs
 * the nodes, retries infrastructure failures, releases intermediate data, checkpoints it to the Manager, fetches
 * results on request, and undeclares everything when it is deleted. It never terminates the process and never
 * installs signal handlers.
 *
 * Handles. Nodes and files are identified by ids that the executor assigns, starting from one. Zero means failure.
 * Ids are valid only for the executor that returned them, and they are never reused. A node the frontend drops is
 * removed once nothing in the run needs it, which frees its memory in a long run.
 *
 * Growth. The graph may grow while it runs, and a graph known in advance is the case where every node is submitted
 * before the first wait. A node is built by adding it and mounting its files, then submitted. It may read only outputs
 * of nodes submitted before it, which keeps the graph acyclic. Nothing depends on nodes not submitted yet.
 *
 * Outputs. Every node output is intermediate data: it stays on Workers, is checkpointed to the Manager, is released
 * once every submitted node that reads it no longer needs it, and is recomputed if lost. A node submitted later may
 * still read a released output, which is then recomputed. A frontend that may read an output later pins it, which
 * keeps it until the frontend unpins it. A pinned output can be fetched to the Manager and read there.
 *
 * Failures. A node fails when its function fails, when a file only the frontend provides is lost, or when its tasks
 * fail more often than max-retries for other reasons. Any other node fails exactly when it needs an output of a failed
 * node that exists nowhere, neither at the Manager nor on a Worker: an unfinished node, or a completed node that must
 * recreate its own outputs. A failed node that never completed has no outputs. A node that fails because of another
 * names the node whose own failure explains it. Other nodes continue, including those that read only outputs of a
 * failed node that still exist. The run as a whole fails only when the executor itself cannot continue, such as when
 * TaskVine removes the library after it keeps failing to start on workers.
 *
 * Runner contract. Every node except a command node (vine_graph_executor_set_command()) runs as a call of one function
 * in one TaskVine library that the frontend installs before the run and removes after it. Node tasks carry no infile,
 * so the library calls the function without arguments. Everything a node needs, including a description of what to run,
 * is a file the frontend mounts in the node's sandbox. A node succeeds when its function returns and every output file
 * it declares exists. A function that fails must exit its task process with a non-zero status. Its standard output then
 * becomes the node's error message. The executor never reads file contents, so their formats belong to the frontend and
 * its runner.
 *
 * Strings. Strings passed in are copied. Strings returned are owned by the executor and stay valid until it is
 * deleted, unless documented otherwise. */

#include <stdint.h>

#include "taskvine.h"

/* State of a run, returned by vine_graph_executor_wait. */
typedef enum {
	VINE_GRAPH_RUNNING = 0, /* Submitted nodes or requested fetches remain. Call vine_graph_executor_wait again. */
	VINE_GRAPH_DONE,	/* Every submitted node completed or failed, and every requested fetch is available. */
	VINE_GRAPH_FAILED	/* The executor cannot continue. vine_graph_executor_get_error explains why. */
} vine_graph_status_t;

/* State of a node, returned by vine_graph_executor_get_node_state. */
typedef enum {
	VINE_GRAPH_NODE_WAITING = 0, /* Not submitted, or submitted and not finished. */
	VINE_GRAPH_NODE_COMPLETED,   /* Its function succeeded, and its outputs exist or can be recomputed. */
	VINE_GRAPH_NODE_FAILED	     /* It cannot complete, or its outputs cannot be restored. */
} vine_graph_node_state_t;

/* Why a node that is a failure source failed. */
typedef enum {
	VINE_GRAPH_FAILURE_NONE = 0,  /* The node did not fail. */
	VINE_GRAPH_FAILURE_FUNCTION,  /* Its function raised an exception or exited with an error status. */
	VINE_GRAPH_FAILURE_SIGNAL,    /* Its function was killed by a signal. */
	VINE_GRAPH_FAILURE_WORKERS,   /* Workers kept disconnecting while they ran its task, as when it exceeds their memory. */
	VINE_GRAPH_FAILURE_INPUT,     /* An input at the Manager is missing. */
	VINE_GRAPH_FAILURE_RETRIES,   /* More attempts failed for other reasons than max-retries allows. */
	VINE_GRAPH_FAILURE_CANCELLED, /* The frontend cancelled it. */
	VINE_GRAPH_FAILURE_SUBMIT,    /* Its task could not be created or submitted. */
	VINE_GRAPH_FAILURE_RESOURCES, /* It used more than it requested: a command of a profile with memory exited with
				       * status 137, as after a SIGKILL, or TaskVine reported it exhausted its resources or
				       * its sandbox. */
	VINE_GRAPH_FAILURE_TIMEOUT,   /* It ran longer than the time limit of its profile. */
	VINE_GRAPH_FAILURE_EXPAND     /* The frontend could not decide the inputs of an open node (vine_graph_executor_fail_open_node()). */
} vine_graph_failure_t;

/* Failures of a node's own task that the retries of its profile cover, as bits of vine_graph_profile.retry, besides a
 * function whose runner exits with VINE_GRAPH_RETRY_STATUS, which always counts: a time limit that passed, a use of
 * more than it requested, and a command's exit status other than zero. */
#define VINE_GRAPH_RETRY_TIMEOUT 1
#define VINE_GRAPH_RETRY_RESOURCES 2
#define VINE_GRAPH_RETRY_COMMAND 4

/* The exit status with which a function runner says that the function's failure is one its profile retries, because
 * it matched what the frontend retries. Any other non-zero status fails the node at once. */
#define VINE_GRAPH_RETRY_STATUS 75

/* An opaque executor. */
struct vine_graph_executor;

/* The workers connected to an executor's Manager and their resources. Each core runs one node of one core at a time.
 * The largest values are each the largest of any connected worker, which need not be one worker. */
struct vine_graph_capacity {
	int64_t workers;	/* workers connected */
	int64_t cores;		/* cores of those workers */
	int64_t largest;	/* cores of the largest of them, or zero without workers */
	int64_t memory;		/* MB of memory of those workers */
	int64_t gpus;		/* GPUs of those workers */
	int64_t largest_memory; /* MB of memory of the largest of them, or zero without workers */
	int64_t largest_gpus;	/* GPUs of the largest of them, or zero */
};

/* What the task of a node requests. Many nodes share one profile, which the executor interns by value, so the fit of
 * a profile against the largest worker is decided once for all of its nodes. Profile 0 is the default: one core and
 * nothing else, which a function call takes as one slot of its library without a request. */
struct vine_graph_profile {
	int cores;	    /* cores, or library slots of a function call; at least one */
	int64_t memory;	    /* MB of memory the task requests, or 0 for none */
	int gpus;	    /* GPUs the task requests, or 0 for none; a task without GPUs sees CUDA_VISIBLE_DEVICES empty */
	int64_t time_limit; /* seconds the task may run, or 0 for no limit; a task past it fails as TIMEOUT */
	int retries;	    /* new attempts after failures of the task itself that retry covers, 0 for none */
	int retry;	    /* VINE_GRAPH_RETRY_* bits: which failures of the task itself the retries cover */
};

/* How far durability lags behind completions. An output is anchored when it has a copy in the vault, at the Manager.
 * Until then, or until its readers no longer need it and it is released, its loss makes its producer run again. Rates and the lag are measured over the last complete window of a few seconds. Large outputs are
 * those the vault receives over connections of their own, above an inline transfer (64 KiB). */
struct vine_graph_durability {
	int64_t exposed;	/* outputs of completed nodes that are neither anchored nor released */
	int64_t pending;	/* outputs queued for the vault or being received */
	double output_rate;	/* outputs per second that completed and needed the vault */
	double lag;		/* mean seconds an output stayed exposed, from the mean exposed count by Little's law */
	double large_rate;	/* large outputs per second that completed and needed the vault */
	double large_capacity;	/* large outputs per second the vault anchored while others waited, or 0 if not measured */
	int64_t cut;		/* generations per anchored band of large outputs, or 0 while every output is anchored */
};

/* Create an executor whose nodes call function_name in the library library_name. Return NULL on failure. */
struct vine_graph_executor *vine_graph_executor_create(struct vine_manager *manager, const char *library_name, const char *function_name);

/* Cancel the run's tasks and undeclare every file it declared. Local files the frontend provided are left in place.
 * Deleting an executor whose run has not finished cancels the run. */
void vine_graph_executor_delete(struct vine_graph_executor *e);

/* Apply one setting. Return zero, or -1 for an unknown name or invalid value, which
 * vine_graph_executor_check_setting explains.
 * task-priority-mode     Order of ready nodes: largest-input-first (default), depth-first, or fifo.
 * max-retries            Failed attempts allowed per node for infrastructure failures, and Worker losses charged
 *                        to it across its attempts, less one (default 5, at most 62).
 * release-outputs        1 (default) prunes a node's outputs once every reader is durable (the release rule), and 0
 *                        keeps every output of the run, as for a frontend that may add readers of any node later.
 *                        Unlike a pin, it does not anchor outputs off the checkpoint cut.
 * progress-bar           Draw a progress bar on standard output: 1 (default) or 0.
 * progress-bar-update-interval-sec  Seconds between progress bar updates (default 0.1).
 * checkpoint-cut         Anchoring of large outputs: auto (default) anchors every output while the vault keeps up,
 *                        and only the outputs on the boundaries of bands of generations while large outputs complete
 *                        faster than it anchors them, with bands just wide enough for it to keep up. 0 anchors every
 *                        output, and k anchors the boundaries of bands of k generations. Small outputs are always
 *                        anchored. A lost output then recomputes at most a band, never a longer chain.
 * checkpoint-yield       Seconds a large output's checkpoint waits, once, while the node has children whose tasks run
 *                        and read it from the Worker that also sends the checkpoint (default 10, 0 disables it). An
 *                        output the frontend fetches never waits. */
int vine_graph_executor_tune(struct vine_graph_executor *e, const char *name, const char *value);

/* Return NULL if vine_graph_executor_tune would accept a setting, or a static string that explains what the setting
 * accepts. A frontend reports it, and one that also runs tasks without an executor, such as for debugging, checks
 * settings by the same rules. */
const char *vine_graph_executor_check_setting(const char *name, const char *value);

/* Add a node and return its id. */
uint64_t vine_graph_executor_add_node(struct vine_graph_executor *e);

/* Declare a file the frontend provides at source_path and return its id. With vault set, Workers fetch it from the
 * Manager's data service, which suits many small files staged for the run. The file must stay readable until the
 * run ends. Its loss fails the nodes that read it, because it cannot be recomputed. A directory without vault is
 * measured once, so its size is that of its tree. */
uint64_t vine_graph_executor_declare_file(struct vine_graph_executor *e, const char *source_path, int vault);

/* Declare a remote input that Workers fetch from its origin, a URL of http, https, ftp, or file, or a root:// URL of
 * XRootD, which uses the X509 proxy that TaskVine finds (X509_USER_PROXY), and return its id. Workers keep it while the
 * workflow runs. A node fails as INPUT when its origin fails more often than max-origin-failures (Manager tuning)
 * allows, with the origin's error. */
uint64_t vine_graph_executor_declare_url(struct vine_graph_executor *e, const char *url);


/* Undeclare a file the frontend provided, removing its vault entry and its copies on Workers. The frontend calls this
 * only when no node that may still run reads the file under any id: every reader failed, or was released and will not
 * be asked for again. Return zero, or -1 for an unknown file or a node output. */
int vine_graph_executor_undeclare_file(struct vine_graph_executor *e, uint64_t file_id);

/* Declare a file that a node not yet submitted writes at task_path in its sandbox, and return its id. */
uint64_t vine_graph_executor_add_output(struct vine_graph_executor *e, uint64_t node_id, const char *task_path);

/* Mount a file at task_path in the sandbox of a node not yet submitted. A node that reads another node's output depends
 * on that node. Return zero, or -1 for an unknown node or file, or a submitted node. */
int vine_graph_executor_add_input(struct vine_graph_executor *e, uint64_t node_id, uint64_t file_id, const char *task_path);

/* Mount count files at once, file_ids[i] at task_paths[i], as vine_graph_executor_add_input() does for each, so a
 * frontend makes one call for a node that reads many files. Return zero, or -1 at the first mount that fails, with the
 * mounts before it in place. */
int vine_graph_executor_add_inputs(struct vine_graph_executor *e, uint64_t node_id, int count, const uint64_t *file_ids, const char *const *task_paths);

/* Declare data the frontend holds in memory, such as the manifest of a node, and mount it at task_path in the sandbox
 * of a node not yet submitted. The Manager keeps a copy, sends it with each task of the node, and has the Worker remove
 * it when that task ends, so the Manager side creates no file. Each declaration has a cache name of its own, even for
 * equal data. The node owns it: it has no file id, and the executor undeclares it when it removes the node, so the
 * frontend keeps no record of it. Return zero, or -1 for an unknown or submitted node or a failed declaration. */
int vine_graph_executor_add_input_buffer(struct vine_graph_executor *e, uint64_t node_id, const char *buffer, size_t size, const char *task_path);

/* Make a node not yet submitted run a shell command line instead of the library function: its task is a standard
 * TaskVine task that runs command in its sandbox, with its mounts, and requests what its profile says. The node
 * succeeds when the command exits with status zero and every output it declares exists, and fails when it exits with
 * another status, with its standard output as the error message. Return zero, or -1 for an unknown or submitted node or
 * a NULL command. */
int vine_graph_executor_set_command(struct vine_graph_executor *e, uint64_t node_id, const char *command);

/* Make a node not yet submitted run its function as a standard TaskVine task instead of a call of the library, as for a
 * node whose profile requests GPUs, which a library call cannot hold. command starts the frontend's runner in the
 * sandbox, which runs the function as a library call does, writes its result where the library would, and exits with
 * status 1, VINE_GRAPH_RETRY_STATUS, or the status of a shell whose child a signal killed (128 + N). The frontend mounts
 * whatever the runner loads at start. Return zero, or -1 for an unknown or submitted node or a NULL command. */
int vine_graph_executor_set_standalone(struct vine_graph_executor *e, uint64_t node_id, const char *command);

/* Return the id of the profile equal to p, which is added when the executor has none, or -1 for a profile of fewer than
 * one core or of negative memory or GPUs. Equal profiles have one id, and profile 0 is the default. */
int vine_graph_executor_add_profile(struct vine_graph_executor *e, const struct vine_graph_profile *p);

/* Make a node not yet submitted request what a profile says. A function call occupies as many function slots of a
 * library instance as the profile has cores, of one slot per core of its Worker, and the library limits the call's
 * thread pools to as many threads. A call also holds the memory of its profile when the library shares its Worker
 * (TaskVine's pool mode). GPUs apply to command and standalone nodes. Every node is stopped at its time limit. A node whose profile does not fit the
 * largest connected Worker waits for a larger one (see vine_graph_executor_next_unfit()). Return zero, or -1 for an
 * unknown or submitted node or an unknown profile. */
int vine_graph_executor_set_profile(struct vine_graph_executor *e, uint64_t node_id, int profile);

/* Mount the output file_id on a node not yet submitted only to order it: the node waits for the output's producer, and
 * fails with it when the output exists nowhere, but its task never receives the file, and a restore of a completed node
 * does not need it. A frontend orders a node after outputs that it reads itself, outside the task. Return zero, or -1
 * for an unknown or submitted node, or a file that is not a node output. */
int vine_graph_executor_add_after(struct vine_graph_executor *e, uint64_t node_id, uint64_t file_id);

/* Make a node not yet submitted open: once submitted, it is linked and counted as any node, so nodes may read its
 * outputs, but it is never dispatched until vine_graph_executor_seal_node(). Until then, inputs and buffers may still
 * be mounted on it, each linked to its producer at once, which must be submitted. Such inputs may come from nodes
 * submitted after it, so the frontend keeps them from reading the open node's outputs, directly or not. Readers that
 * were linked before keep the depth they had. Return zero, or -1 for an unknown or submitted node. */
int vine_graph_executor_open_node(struct vine_graph_executor *e, uint64_t node_id);

/* Seal an open node, whose inputs are then fixed: it runs once every node whose output it reads has completed. Return
 * zero, or -1 for a node that is not open. Sealing a failed node does nothing else. Its cost is O(inputs), once. */
int vine_graph_executor_seal_node(struct vine_graph_executor *e, uint64_t node_id);

/* Fail an open node that did not finish as VINE_GRAPH_FAILURE_EXPAND with message, and the nodes that need its outputs,
 * as when the frontend cannot decide what it reads. Return zero, or -1 for a node that is not open or finished. */
int vine_graph_executor_fail_open_node(struct vine_graph_executor *e, uint64_t node_id, const char *message);

/* Submit a node whose files are mounted. It runs once every node whose output it reads has completed, and it fails at
 * once when it reads an output of a failed node that exists nowhere. Return zero, or -1 for an unknown or submitted
 * node, a node that reads an output of a node not submitted before it, or a failed run. */
int vine_graph_executor_submit_node(struct vine_graph_executor *e, uint64_t node_id);

/* Install a file at path as the vault copy of an output of a node not yet submitted, as if its task had written it and
 * the vault had checkpointed it, such as a result that a store kept from an earlier run. The path must stay until the
 * run ends. Return zero, or -1 for a file that is not an output of an unsubmitted node, or a path the vault cannot take.
 */
int vine_graph_executor_adopt_output(struct vine_graph_executor *e, uint64_t file_id, const char *path);

/* Complete a node not yet submitted without running it, once every output was adopted: it is linked as
 * vine_graph_executor_submit_node() links it, and is completed and anchored at once, so its readers, releases,
 * progress, and fetches treat it as a node whose task completed. A completed node that must recreate an output it lost
 * runs its task. Return zero, or -1 for an unknown or submitted node, an output without a vault copy, or a node that
 * reads an output of a node that did not complete. */
int vine_graph_executor_complete_node(struct vine_graph_executor *e, uint64_t node_id);

/* Pin or unpin a node output. A pinned output is never released. Pins are counted, so each pin needs one unpin.
 * Return zero, or -1 for a file that is not a node output, or an unpin without a pin. An unpin returns one instead when
 * it leaves the producer released, because it released the producer or took the last pin of an output of a released
 * producer that was not restored: vine_graph_executor_next_released() does not report the producer for it, so the
 * frontend lets the producer go as if it had been reported. Ancestors that it releases in turn are reported. */
int vine_graph_executor_pin_file(struct vine_graph_executor *e, uint64_t file_id);
int vine_graph_executor_unpin_file(struct vine_graph_executor *e, uint64_t file_id);

/* Request a readable copy of a pinned output of a submitted node at the Manager. The run then retrieves it as soon as
 * the node completes, with priority over checkpoints and beyond the vault limit, and recomputes it if it was lost.
 * Return zero, or -1 for a file that is not a pinned output of a submitted node. */
int vine_graph_executor_fetch_file(struct vine_graph_executor *e, uint64_t file_id);

/* Return the next file whose fetch made it readable since the last call, or zero when none did. Each call of
 * vine_graph_executor_fetch_file() is reported once, at once when the file is already readable, unless its producer
 * fails. Its bytes, when the Manager holds them in memory, are then available from vine_graph_executor_get_local_data(),
 * and its path from vine_graph_executor_get_local_path(), while the output stays pinned. */
uint64_t vine_graph_executor_next_fetched(struct vine_graph_executor *e);

/* Return the next file whose fetch failed because its Worker cannot send it, such as a directory that holds an entry
 * the Worker cannot read, or zero when none did. Such a fetch is not tried again: another call of
 * vine_graph_executor_fetch_file() tries again, which succeeds once the producer has run again and written a file its
 * Worker can send. */
uint64_t vine_graph_executor_next_fetch_failed(struct vine_graph_executor *e);

/* Return the next node that waits because its profile requests more cores, memory, or GPUs than the largest connected
 * Worker has, or zero when none does. A node is reported once, after two checks seconds apart found its task, a
 * recovery of its outputs included, waiting while every connected Worker was too small for it. It keeps waiting for a
 * larger Worker. Without Workers, nodes wait unreported. */
uint64_t vine_graph_executor_next_unfit(struct vine_graph_executor *e);

/* Set data and length to the bytes of a fetched node output that the Manager holds in memory, a small result that came
 * inline, and return one. Return zero, and set data to NULL, when the Manager holds no copy of the file in memory, which
 * vine_graph_executor_get_local_path() then provides. The bytes stay valid until the next call into the executor, so
 * the caller copies them at once. Reading them never writes the file. */
int vine_graph_executor_get_local_data(struct vine_graph_executor *e, uint64_t file_id, const char **data, int64_t *length);

/* Return a readable local path of a file, or NULL while none is available. A provided file is at its source path. A
 * node output is available once fetched, and its path stays valid while the output stays pinned. A fetched output that
 * the Manager holds in memory is written to its vault path first. */
const char *vine_graph_executor_get_local_path(struct vine_graph_executor *e, uint64_t file_id);

/* Advance the run and return its state. The wait ends once a node finished or a requested fetch became available, or
 * after about timeout seconds, or at a wake. It runs the Manager for that time even when nothing remains, so Workers
 * that connect and recovery tasks are handled while the run is idle. More nodes may be submitted after DONE, and the
 * run continues with the next wait. */
vine_graph_status_t vine_graph_executor_wait(struct vine_graph_executor *e, int timeout);

/* End a wait that runs in another thread soon, so that thread can apply calls queued for it. This is the only function
 * that may be called while another thread uses the executor, and only while the executor exists. */
void vine_graph_executor_wake(struct vine_graph_executor *e);

/* Hold and release the messages to Workers that the calls between them send, such as the unlinks of the outputs that
 * unpins release and removals undeclare. While held, messages only queue, and the outermost release writes each
 * Worker's queue once, so a frontend that lets go of many tasks in a row makes one write per Worker instead of one per
 * message. Holds nest, and each hold needs one release. A frontend calls vine_graph_executor_wait() only outside a
 * hold, because the Manager would send nothing while it waits. */
void vine_graph_executor_hold_sends(struct vine_graph_executor *e);
void vine_graph_executor_release_sends(struct vine_graph_executor *e);

/* Return the next node that completed or failed since the last call, or zero when none did. A completed node whose
 * outputs later cannot be restored is reported again as failed. */
uint64_t vine_graph_executor_next_finished(struct vine_graph_executor *e);
/* Return the next node whose outputs were released since the last call, or zero when none was. A release waits until
 * every child is durable, so a released node runs again only when the frontend asks for its outputs, by pinning them
 * or by submitting a node that reads them. */
uint64_t vine_graph_executor_next_released(struct vine_graph_executor *e);
/* Return the next node that the executor keeps since the last call, or zero when none. A node whose outputs were
 * released and that runs again, because the frontend asked for its outputs or for those of a descendant, is kept: it is
 * never released until the frontend lets it go with vine_graph_executor_unkeep_node(), so requests for its neighbours
 * do not recompute it again. Return of vine_graph_executor_unkeep_node(): zero, one when it released the node, which
 * vine_graph_executor_next_released() then does not report, or -1 for a node that is not kept. */
uint64_t vine_graph_executor_next_kept(struct vine_graph_executor *e);
int vine_graph_executor_unkeep_node(struct vine_graph_executor *e, uint64_t node_id);
/* Return the state of a node. */
vine_graph_node_state_t vine_graph_executor_get_node_state(const struct vine_graph_executor *e, uint64_t node_id);
/* Return the node whose own failure made this node fail, which is the node itself or one it depends on, or zero
 * when the node did not fail. */
uint64_t vine_graph_executor_get_failure_source(const struct vine_graph_executor *e, uint64_t node_id);
/* Return why the failure source of a failed node failed, or NULL when the node did not fail. */
const char *vine_graph_executor_get_node_error(const struct vine_graph_executor *e, uint64_t node_id);
/* Return the kind of failure of the failure source of a failed node, or VINE_GRAPH_FAILURE_NONE when the node did not
 * fail. */
vine_graph_failure_t vine_graph_executor_get_node_failure(const struct vine_graph_executor *e, uint64_t node_id);

/* Return the exit status of the command whose failure made a command node, or a node that depends on it, fail, or -1
 * when the failure source is not a command that exited with an error status. A command that failed as
 * VINE_GRAPH_FAILURE_RESOURCES by its status 137 reports it too. */
int vine_graph_executor_get_node_exit_code(const struct vine_graph_executor *e, uint64_t node_id);

/* Return why the run failed as a whole, or NULL. */
const char *vine_graph_executor_get_error(const struct vine_graph_executor *e);
/* Return the time from the first dispatch to the last retrieval of node tasks, in microseconds. */
uint64_t vine_graph_executor_get_makespan_us(const struct vine_graph_executor *e);
/* Return how many tasks recomputed outputs of completed nodes in this run. */
uint64_t vine_graph_executor_get_completed_recovery_tasks(const struct vine_graph_executor *e);
/* Return the workers connected now and their cores, at O(workers), or zeros for a NULL executor. */
struct vine_graph_capacity vine_graph_executor_get_capacity(struct vine_graph_executor *e);
/* Return how far durability lags behind completions, at O(1), or zeros for a NULL executor. */
struct vine_graph_durability vine_graph_executor_get_durability(const struct vine_graph_executor *e);

/* Drop a submitted node the frontend no longer needs: it will not pin, fetch, or read the node's outputs, and no node it
 * submits later reads them. The node is removed once it failed, or once it completed and its outputs were released,
 * and once every node that reads its outputs was removed, so a node is never removed before its descendants. A removed
 * node frees its task and undeclares its outputs, and its id is no longer valid. Dropping only queues the node, in
 * O(1): vine_graph_executor_wait() removes a few hundred queued nodes per step, and the frontend may remove more with
 * vine_graph_executor_remove_dropped(). Return zero, or -1 for an unknown or unsubmitted node. */
int vine_graph_executor_drop_node(struct vine_graph_executor *e, uint64_t node_id);
/* Remove dropped nodes that are removable, and the dropped parents that their removal makes removable in turn, taking
 * at most limit queued entries. Return the number of entries still queued, zero when none is. */
int vine_graph_executor_remove_dropped(struct vine_graph_executor *e, int limit);
/* Return the next node removed since the last call, or zero when none was. The frontend forgets it then. */
uint64_t vine_graph_executor_next_removed(struct vine_graph_executor *e);

/* Cancel a submitted node that has not completed or failed: its task stops, and it fails with every node that depends
 * on it, as if its function had failed. Return zero, or -1 for an unknown node or a node that already finished. */
int vine_graph_executor_cancel_node(struct vine_graph_executor *e, uint64_t node_id);

#endif // VINE_GRAPH_EXECUTOR_H
