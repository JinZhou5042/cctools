/* DataVine workflow store API. */
#ifndef VINE_DATAVINE_WORKFLOW_STORE_H
#define VINE_DATAVINE_WORKFLOW_STORE_H

#include "vine_datavine_workflow.h"

#include <stddef.h>
#include <signal.h>
#include <stdint.h>

struct vine_datavine_workflow_store;
struct vine_datavine_journal;
struct vine_datavine_workflow_runtime;

/* Maximum number of materialized physical task views retained per workflow.
 * The logical Scheduler frontier may be much larger. */
#define VINE_DATAVINE_WORKFLOW_SUBMISSION_WINDOW 4096
/* Keep one machine-wide execution wave available for physical data recovery.
 * Ordinary children stop at SUBMISSION_WINDOW. A last-replica replay may
 * exceed that bound by RECOVERY_RESERVE so consumers waiting in Worker Data
 * Agents cannot prevent their producers from entering TaskVine. */
#define VINE_DATAVINE_WORKFLOW_RECOVERY_RESERVE 2048
struct vine_datavine_data_controller;
struct vine_manager;
struct jx;

/* Internal append validator. The caller retains ownership of root. */
typedef int (*vine_datavine_workflow_data_lookup_t)(void *context,
		uint64_t data_id, int64_t *producer_task_id);
int vine_datavine_workflow_delta_validate_parsed(struct jx *root,
		uint64_t accepted_maximum_task_id,
		uint64_t accepted_maximum_data_id,
		uint64_t maximum_tasks, uint64_t maximum_edges,
		uint64_t accepted_tasks, uint64_t accepted_edges,
		vine_datavine_workflow_data_lookup_t lookup_data, void *lookup_context,
		char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1],
		struct vine_datavine_workflow_summary *summary,
		struct vine_datavine_workflow_error *error);

enum vine_datavine_workflow_state {
	VINE_DATAVINE_WORKFLOW_OPEN = 1,
	VINE_DATAVINE_WORKFLOW_SEALED = 2,
	VINE_DATAVINE_WORKFLOW_CANCELLED = 3,
	VINE_DATAVINE_WORKFLOW_RUNNING = 4,
	VINE_DATAVINE_WORKFLOW_COMPLETED = 5,
	VINE_DATAVINE_WORKFLOW_FAILED = 6,
	VINE_DATAVINE_WORKFLOW_RUNNING_OPEN = 7,
	VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT = 8,
	VINE_DATAVINE_WORKFLOW_STAGED = 9,
};

enum vine_datavine_workflow_event_type {
	VINE_DATAVINE_WORKFLOW_ACCEPTED = 1,
	VINE_DATAVINE_WORKFLOW_APPENDED = 2,
	VINE_DATAVINE_WORKFLOW_SEALED_EVENT = 3,
	VINE_DATAVINE_WORKFLOW_CANCELLED_EVENT = 4,
	VINE_DATAVINE_WORKFLOW_STARTED = 5,
	VINE_DATAVINE_WORKFLOW_COMPLETED_EVENT = 6,
	VINE_DATAVINE_WORKFLOW_FAILED_EVENT = 7,
	VINE_DATAVINE_WORKFLOW_RECOVERED_EVENT = 8,
	VINE_DATAVINE_WORKFLOW_TASK_SUBMITTED = 9,
	VINE_DATAVINE_WORKFLOW_TASK_COMPLETED = 10,
	VINE_DATAVINE_WORKFLOW_TASK_RETRY = 11,
	VINE_DATAVINE_WORKFLOW_TASK_FAILED = 12,
	VINE_DATAVINE_WORKFLOW_QUIESCENT_EVENT = 13,
	VINE_DATAVINE_WORKFLOW_RESUMED_EVENT = 14,
};

struct vine_datavine_workflow_info {
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1];
	uint64_t generation;
	uint64_t event_id;
	enum vine_datavine_workflow_state state;
	struct vine_datavine_workflow_summary summary;
};

struct vine_datavine_workflow_event {
	uint64_t event_id;
	uint64_t generation;
	enum vine_datavine_workflow_event_type type;
	char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1];
	int64_t task_id;
	uint32_t attempt;
	int32_t result;
};

struct vine_datavine_workflow_result_info {
	uint64_t data_id;
	uint64_t size;
	uint32_t attempt;
	int64_t producer_task_id;
	int32_t producer_output_index;
	int requested;
	char sha256[65];
	char codec_name[129];
	char codec_version[65];
};

struct vine_datavine_workflow_task_event_record {
	enum vine_datavine_workflow_event_type type;
	int64_t task_id;
	uint32_t attempt;
	int32_t result;
};

/* Store lifetime. */
struct vine_datavine_workflow_store *vine_datavine_workflow_store_open(
		const char *journal_path);
struct vine_datavine_journal *vine_datavine_workflow_store_journal(
		struct vine_datavine_workflow_store *store);
int vine_datavine_workflow_store_recover(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		struct vine_datavine_workflow_error *error);
void vine_datavine_workflow_store_close(
		struct vine_datavine_workflow_store *store);

/* Service-facing workflow mutations and queries. */
int vine_datavine_workflow_store_submit(
		struct vine_datavine_workflow_store *store,
		const char *json, size_t size,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error);

/* Append one immutable delta using expected_generation as a CAS. */
int vine_datavine_workflow_store_append_delta(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t expected_generation,
		const char *json, size_t size,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error);

int vine_datavine_workflow_store_seal(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t expected_generation,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error);
int vine_datavine_workflow_store_cancel(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error);
int vine_datavine_workflow_store_describe(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		struct vine_datavine_workflow_info *result);
int vine_datavine_workflow_store_frontier(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t *maximum_task_id,
		uint64_t *maximum_data_id, uint64_t *maximum_tasks,
		uint64_t *maximum_edges);

/* Return events after after_event_id, or zero when none are currently stored. */
size_t vine_datavine_workflow_store_watch(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t after_event_id,
		struct vine_datavine_workflow_event *events, size_t capacity);

/* Runtime-only workflow ownership. Returned document is caller-owned. */
int vine_datavine_workflow_store_take_runnable(
		struct vine_datavine_workflow_store *store,
		struct vine_datavine_workflow_info *result,
		char **document, size_t *document_size);
/* Transfer the already-validated parsed transaction to its first runtime
 * consumer. Recovery/reconstruction reparses the retained journal bytes. */
struct jx *vine_datavine_workflow_store_take_delta_root(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t after_generation,
		uint64_t *generation);
int vine_datavine_workflow_store_mark_quiescent(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		uint64_t expected_generation,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error);
int vine_datavine_workflow_store_finish(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, int successful,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error);

/* Result reads exist only to migrate journals written by the retired
 * payload-in-workflow-store implementation. New results belong exclusively
 * to vine_datavine_data_controller. */
int vine_datavine_workflow_store_legacy_fetch_result(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t data_id,
		char **data, size_t *size);
int vine_datavine_workflow_store_legacy_result_info(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t data_id,
		struct vine_datavine_workflow_result_info *result);
int vine_datavine_workflow_store_record_task_events(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		const struct vine_datavine_workflow_task_event_record *records,
		size_t count, struct vine_datavine_workflow_error *error);
uint32_t vine_datavine_workflow_store_task_attempts(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, int64_t task_id);
int vine_datavine_workflow_store_task_attempts_snapshot(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint32_t *attempts, size_t count);
int vine_datavine_workflow_store_completed_task_ids(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, int64_t **task_ids, size_t *count);
int vine_datavine_workflow_store_checkpoint(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id);

/* Runtime thread lifetime; the manager remains caller-owned. */
struct vine_datavine_workflow_runtime *vine_datavine_workflow_runtime_start(
		struct vine_datavine_workflow_store *store,
		struct vine_datavine_data_controller *data_controller,
		struct vine_manager *manager,
		const char *native_executor_path,
		const char *python_executor_path);
void vine_datavine_workflow_runtime_run(
		struct vine_datavine_workflow_runtime *runtime,
		volatile sig_atomic_t *external_stopping);
void vine_datavine_workflow_runtime_stop(
		struct vine_datavine_workflow_runtime *runtime);

#endif
