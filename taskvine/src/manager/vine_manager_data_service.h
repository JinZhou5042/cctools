/* Manage the Manager data port and asynchronous transfers in both directions.
 * The vault is a directory under the Manager staging directory. Each entry is named by a cache name and is the only
 * data a Worker can fetch from the data port: a symlink to a declared local file, or a retrieved checkpoint.
 * The vault table indexes these entries by cache name for queries on the Manager thread.
 * Worker requests always take priority over checkpoint receives for the shared transfer slots: receives never use
 * the slots reserved for Worker requests, and a Worker request preempts a receive when every slot is busy.
 * The Manager thread owns file lookup and completion callbacks. Executors only perform network and file I/O.
 * All entry points except executor functions run on the Manager thread. */
#ifndef VINE_MANAGER_DATA_SERVICE_H
#define VINE_MANAGER_DATA_SERVICE_H

#include <stdint.h>

struct vine_manager;
struct vine_file;
struct vine_manager_data_service;
struct link_info;

/* Result of a checkpoint request. Both refusals leave nothing behind and can be retried later. */
typedef enum {
	VINE_CHECKPOINT_ADMITTED = 1,	/* Started, already in progress, or already in the vault. */
	VINE_CHECKPOINT_BUSY = 0,	/* No free transfer slot outside the Worker reserve, or the vault is full. */
	VINE_CHECKPOINT_NO_SOURCE = -1, /* The file is not a temporary file with a ready Worker replica. */
} vine_manager_checkpoint_result_t;

/* Create a data listener and a bounded executor pool. */
struct vine_manager_data_service *vine_manager_data_service_create(const char *runtime_directory);
/* Cancel outstanding transfers and release their resources without invoking callbacks. */
void vine_manager_data_service_delete(struct vine_manager_data_service *ds);
/* Return the data port advertised to Workers. */
int vine_manager_data_service_port(struct vine_manager_data_service *ds);
/* Fill two poll entries for the listener and completion notification. */
void vine_manager_data_service_poll(struct vine_manager_data_service *ds, struct link_info *entries);
/* Accept connections and process a bounded batch of executor results. */
void vine_manager_data_service_handle(struct vine_manager *manager);

/* Request a checkpoint of a temporary file into the vault through the shared bounded pool.
 * When admitted, the Manager thread later calls complete with one when the complete file is published, or with zero
 * when the receive fails or is preempted by a Worker request. An existing vault entry completes immediately.
 * An in-progress request keeps its original callback. Removing the vault entry cancels the receive and drops
 * its callback. */
vine_manager_checkpoint_result_t vine_manager_data_service_checkpoint(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument);
/* Request a temporary file that the application needs now. It behaves as a checkpoint, except that it is admitted
 * beyond the vault limit, so only slots and a ready Worker replica can refuse it. */
vine_manager_checkpoint_result_t vine_manager_data_service_fetch(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument);

/* Declare a regular local file as a workflow-cached VINE_FILE served through the vault. Return NULL on failure.
 * The caller keeps the local source unchanged until undeclaration. */
struct vine_file *vine_manager_data_service_declare_file(struct vine_manager *manager, const char *source_path);
/* Query a readable local disk copy without starting a transfer: a vault entry or a readable VINE_FILE source. */
int vine_manager_data_service_has_local_file(struct vine_manager *manager, struct vine_file *file);
/* Cancel checkpoint retrieval and remove the file's vault entry without waiting for queued work.
 * Only a connected receive is joined. Open sends retain their descriptors. */
void vine_manager_data_service_vault_remove(struct vine_manager_data_service *ds, struct vine_file *file);
/* Return non-zero when the vault holds a complete entry for this file. Does not touch the filesystem. */
int vine_manager_data_service_vault_contains(struct vine_manager *manager, struct vine_file *file);
/* Return a newly allocated path of the file's vault entry, or NULL when the vault holds none. The entry stays until
 * the file is pruned or undeclared. */
char *vine_manager_data_service_vault_path(struct vine_manager *manager, struct vine_file *file);
/* Limit the bytes of checkpoint entries, including receives in progress. Zero means unlimited.
 * Declared local files are not counted because the vault only links to them. */
void vine_manager_data_service_vault_set_limit(struct vine_manager_data_service *ds, int64_t bytes);
/* Return the bytes of published checkpoint entries plus reservations of receives in progress. */
int64_t vine_manager_data_service_vault_get_usage(struct vine_manager_data_service *ds);

#endif
