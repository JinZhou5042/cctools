#ifndef VINE_GRAPH_EXECUTOR_H
#define VINE_GRAPH_EXECUTOR_H

/* Execute one workflow graph on a TaskVine Manager. This is the interface every workflow frontend uses, whatever its
 * language. The frontend describes nodes and the files they read and write. The executor declares those files, runs
 * the nodes, retries infrastructure failures, releases intermediate data, checkpoints it to the Manager, fetches
 * results on request, and undeclares everything when it is deleted. It never terminates the process and never
 * installs signal handlers.
 *
 * Handles. Nodes and files are identified by ids that the executor assigns, starting from one. Zero means failure.
 * Ids are valid only for the executor that returned them.
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
 * fail more often than max-retries for other reasons. Every unfinished node that depends on it then fails too, while
 * unrelated nodes continue. The run as a whole fails only when the executor itself cannot continue, such as when
 * TaskVine removes the library after it keeps failing to start on workers.
 *
 * Runner contract. Every node runs as a call of one function in one TaskVine library that the frontend installs
 * before the run and removes after it. Node tasks carry no infile, so the library calls the function without
 * arguments. Everything a node needs, including a description of what to run, is a file the frontend mounts in the
 * node's sandbox. A node succeeds when its function returns and every output file it declares exists. A function that
 * fails must exit its task process with a non-zero status. Its standard output then becomes the node's error message.
 * The executor never reads file contents, so their formats belong to the frontend and its runner.
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

/* An opaque executor. */
struct vine_graph_executor;

/* Create an executor whose nodes call function_name in the library library_name. Return NULL on failure. */
struct vine_graph_executor *vine_graph_executor_create(struct vine_manager *manager, const char *library_name, const char *function_name);

/* Cancel the run's tasks and undeclare every file it declared. Local files the frontend provided are left in place.
 * Deleting an executor whose run has not finished cancels the run. */
void vine_graph_executor_delete(struct vine_graph_executor *e);

/* Apply one setting. Return zero, or -1 for an unknown name or invalid value, which
 * vine_graph_executor_check_setting explains.
 * task-priority-mode     Order of ready nodes: largest-input-first (default), depth-first, or fifo.
 * max-retries            Failed attempts allowed per node for infrastructure failures (default 5).
 * progress-bar           Draw a progress bar on standard output: 1 (default) or 0.
 * progress-bar-update-interval-sec  Seconds between progress bar updates (default 0.1). */
int vine_graph_executor_tune(struct vine_graph_executor *e, const char *name, const char *value);

/* Return NULL if vine_graph_executor_tune would accept a setting, or a static string that explains what the setting
 * accepts. A frontend reports it, and one that also runs tasks without an executor, such as for debugging, checks
 * settings by the same rules. */
const char *vine_graph_executor_check_setting(const char *name, const char *value);

/* Add a node and return its id. */
uint64_t vine_graph_executor_add_node(struct vine_graph_executor *e);

/* Declare a file the frontend provides at source_path and return its id. With vault set, Workers fetch it from the
 * Manager's data service, which suits many small files staged for the run. The file must stay readable until the
 * run ends. Its loss fails the nodes that read it, because it cannot be recomputed. */
uint64_t vine_graph_executor_declare_file(struct vine_graph_executor *e, const char *source_path, int vault);

/* Undeclare a file the frontend provided, removing its vault entry and its copies on Workers. The frontend calls this
 * only when no node that may still run reads the file under any id: every reader failed, or was released and will not
 * be asked for again. Return zero, or -1 for an unknown file or a node output. */
int vine_graph_executor_undeclare_file(struct vine_graph_executor *e, uint64_t file_id);

/* Declare a file that a node not yet submitted writes at task_path in its sandbox, and return its id. */
uint64_t vine_graph_executor_add_output(struct vine_graph_executor *e, uint64_t node_id, const char *task_path);

/* Mount a file at task_path in the sandbox of a node not yet submitted. A node that reads another node's output depends
 * on that node. Return zero, or -1 for an unknown node or file, or a submitted node. */
int vine_graph_executor_add_input(struct vine_graph_executor *e, uint64_t node_id, uint64_t file_id, const char *task_path);

/* Submit a node whose files are mounted. It runs once every node whose output it reads has completed, and it fails at
 * once when one of them failed. Return zero, or -1 for an unknown or submitted node, a node that reads an output of a
 * node not submitted before it, or a failed run. */
int vine_graph_executor_submit_node(struct vine_graph_executor *e, uint64_t node_id);

/* Pin or unpin a node output. A pinned output is never released. Pins are counted, so each pin needs one unpin.
 * Return zero, or -1 for a file that is not a node output, or an unpin without a pin. */
int vine_graph_executor_pin_file(struct vine_graph_executor *e, uint64_t file_id);
int vine_graph_executor_unpin_file(struct vine_graph_executor *e, uint64_t file_id);

/* Request a readable copy of a pinned output of a submitted node at the Manager. The run then retrieves it as soon as
 * the node completes, with priority over checkpoints and beyond the vault limit, and recomputes it if it was lost.
 * Return zero, or -1 for a file that is not a pinned output of a submitted node. */
int vine_graph_executor_fetch_file(struct vine_graph_executor *e, uint64_t file_id);

/* Return a readable local path of a file, or NULL while none is available. A provided file is at its source path. A
 * node output is available once fetched, and its path stays valid while the output stays pinned. */
const char *vine_graph_executor_get_local_path(struct vine_graph_executor *e, uint64_t file_id);

/* Advance the run and return its state. The wait ends once a node finished or a requested fetch became available, or
 * after about timeout seconds. More nodes may be submitted after DONE, and the run continues with the next wait. */
vine_graph_status_t vine_graph_executor_wait(struct vine_graph_executor *e, int timeout);

/* End a wait that runs in another thread soon, so that thread can apply calls queued for it. This is the only function
 * that may be called while another thread uses the executor, and only while the executor exists. */
void vine_graph_executor_wake(struct vine_graph_executor *e);

/* Return the next node that completed or failed since the last call, or zero when none did. A completed node whose
 * outputs later cannot be restored is reported again as failed. */
uint64_t vine_graph_executor_next_finished(struct vine_graph_executor *e);
/* Return the next node whose outputs were released since the last call, or zero when none was. A release waits until
 * every child is durable, so a released node runs again only when the frontend asks for its outputs, by pinning them
 * or by submitting a node that reads them. */
uint64_t vine_graph_executor_next_released(struct vine_graph_executor *e);
/* Return the state of a node. */
vine_graph_node_state_t vine_graph_executor_get_node_state(const struct vine_graph_executor *e, uint64_t node_id);
/* Return the node whose own failure made this node fail, which is the node itself or one it depends on, or zero
 * when the node did not fail. */
uint64_t vine_graph_executor_get_failure_source(const struct vine_graph_executor *e, uint64_t node_id);
/* Return why the failure source of a failed node failed, or NULL when the node did not fail. */
const char *vine_graph_executor_get_node_error(const struct vine_graph_executor *e, uint64_t node_id);

/* Return why the run failed as a whole, or NULL. */
const char *vine_graph_executor_get_error(const struct vine_graph_executor *e);
/* Return the time from the first dispatch to the last retrieval of node tasks, in microseconds. */
uint64_t vine_graph_executor_get_makespan_us(const struct vine_graph_executor *e);
/* Return how many tasks recomputed outputs of completed nodes in this run. */
uint64_t vine_graph_executor_get_completed_recovery_tasks(const struct vine_graph_executor *e);

/* Cancel a submitted node that has not completed or failed: its task stops, and it fails with every node that depends
 * on it, as if its function had failed. Return zero, or -1 for an unknown node or a node that already finished. */
int vine_graph_executor_cancel_node(struct vine_graph_executor *e, uint64_t node_id);

#endif // VINE_GRAPH_EXECUTOR_H
