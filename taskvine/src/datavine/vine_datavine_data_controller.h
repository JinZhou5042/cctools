/* Concurrent DataVine data-plane owner. */
#ifndef VINE_DATAVINE_DATA_CONTROLLER_H
#define VINE_DATAVINE_DATA_CONTROLLER_H

#include <stddef.h>
#include <stdint.h>

struct vine_datavine_data_controller;
struct vine_datavine_journal;
struct vine_datavine_publish_record;
struct vine_datavine_workflow_result_info;
struct vine_task;

uint64_t vine_datavine_data_controller_loss_events(
		struct vine_datavine_data_controller *controller);

enum vine_datavine_agent_resolve_status {
	VINE_DATAVINE_AGENT_UNKNOWN = 0,
	VINE_DATAVINE_AGENT_PENDING = 1,
	VINE_DATAVINE_AGENT_AVAILABLE = 2,
	VINE_DATAVINE_AGENT_DEAD = 3,
};

struct vine_datavine_agent_replica {
	uint32_t worker_slot;
	uint64_t session_epoch;
	uint64_t object_token;
	uint32_t generation;
	char host[64];
	uint16_t port;
};

struct vine_datavine_agent_stats {
	uint64_t active_data;
	uint64_t active_replicas;
	uint64_t active_waiters;
	uint64_t active_sessions;
	uint64_t peak_data;
	uint64_t peak_replicas;
	uint64_t peak_waiters;
};

/* Cumulative diagnostics for requested-output persistence.  These counters
 * observe the existing path only; they do not alter queueing or durability. */
struct vine_datavine_agent_persistence_metrics {
	uint64_t jobs;
	uint64_t bytes;
	uint64_t failures;
	uint64_t retries;
	uint64_t peak_queue_depth;
	uint64_t enqueue_block_nanoseconds;
	uint64_t queue_wait_nanoseconds;
	uint64_t connection_wait_nanoseconds;
	uint64_t request_nanoseconds;
	uint64_t stream_nanoseconds;
	uint64_t fsync_nanoseconds;
	uint64_t close_nanoseconds;
	uint64_t rename_nanoseconds;
	uint64_t verify_nanoseconds;
	uint64_t commit_wait_nanoseconds;
	uint64_t commit_nanoseconds;
	uint64_t commit_groups;
	uint64_t background_jobs;
	uint64_t background_bytes;
	uint64_t background_peak_backlog;
};

struct vine_datavine_agent_release {
	uint64_t sequence;
	uint64_t data_id;
	uint64_t object_token;
	uint32_t generation;
};

struct vine_datavine_data_publication_metrics {
	uint64_t queue_nanoseconds;
	uint64_t commit_nanoseconds;
	uint64_t pull_nanoseconds;
	uint64_t decode_nanoseconds;
	uint64_t function_nanoseconds;
	uint64_t serialize_nanoseconds;
	uint64_t fsync_nanoseconds;
	uint64_t outputs;
	uint64_t remote_outputs;
	uint64_t durable_outputs;
	uint64_t output_bytes;
	uint64_t journal_records;
	uint64_t task_reports;
	uint64_t task_reported_read_bytes;
	uint64_t task_reported_cpu_milliseconds;
};

/* Decode Worker Data Agent task telemetry inside the Controller boundary.
 * The workflow runtime receives only numeric counters and never reads the
 * data manifest carried by a physical TaskVine completion. */
int vine_datavine_data_controller_task_metrics(
		struct vine_datavine_data_controller *controller,
		struct vine_task *completed,
		struct vine_datavine_data_publication_metrics *metrics);

struct vine_datavine_data_controller *vine_datavine_data_controller_open(
		const char *workflow_journal_path, size_t threads,
		struct vine_datavine_journal *journal);
void vine_datavine_data_controller_close(
		struct vine_datavine_data_controller *controller);

/* Configure the current, replaceable object-service location. Workflow IR
 * retains only content identity; scheduler state never retains this address. */
int vine_datavine_data_controller_configure_object_service(
		struct vine_datavine_data_controller *controller,
		const char *host, int port, const char *token);
int vine_datavine_data_controller_workflow_key(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, unsigned char key[32]);
int vine_datavine_data_controller_agent_endpoint(
		struct vine_datavine_data_controller *controller, char host[64],
		uint16_t *port);
char *vine_datavine_data_controller_object_ticket(
		struct vine_datavine_data_controller *controller,
		const char *sha256);
int vine_datavine_data_controller_put_object(
		struct vine_datavine_data_controller *controller, const char sha256[65],
		const void *data, size_t size, int *deduplicated);
int vine_datavine_data_controller_get_object(
		struct vine_datavine_data_controller *controller, const char sha256[65],
		unsigned char **data, size_t *size);
/* Create the one SharedFS result directory owned by a workflow.  Result-path
 * construction is deliberately side-effect free after this call. */
int vine_datavine_data_controller_prepare_workflow(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id);
/* Enable the optional low-priority Controller replica for every live retained
 * Worker output.  The setting is fixed before either persistence queue starts. */
int vine_datavine_data_controller_set_idata_backup(
		struct vine_datavine_data_controller *controller, int background);

/* Authenticate and serve bounded ranges from Controller-backed iData.  This
 * is a recovery source for Workers, not a scheduler-visible file mount. */
int vine_datavine_data_controller_validate_backup_ticket(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint64_t size, const unsigned char digest[32],
		const unsigned char signature[32]);
int vine_datavine_data_controller_read_backup(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint64_t offset, size_t requested, unsigned char **data, size_t *size);

/* Worker Data Agent metadata path. The workflow key authenticates
 * one HELLO and returns a compact workflow slot; all later records are fixed
 * width and carry the slot rather than a workflow string. Payload bytes never
 * enter these calls. */
int vine_datavine_data_controller_agent_hello(
		struct vine_datavine_data_controller *controller,
		const unsigned char workflow_key[32], uint32_t *worker_slot,
		uint64_t session_epoch, const char *host, uint16_t port,
		uint64_t *workflow_slot);
int vine_datavine_data_controller_agent_expect(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t *generation);
int vine_datavine_data_controller_agent_expect_result(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t generation,
		int64_t producer_task_id, int32_t producer_output_index,
		const char *codec_name, const char *codec_version);
int vine_datavine_data_controller_agent_session_lost(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint32_t worker_slot,
		uint64_t session_epoch);
int vine_datavine_data_controller_agent_publish(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint64_t size, const unsigned char digest[32], uint32_t worker_slot,
		uint64_t session_epoch, uint64_t object_token);
int vine_datavine_data_controller_agent_publish_batch(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot,
		const struct vine_datavine_publish_record *records, size_t count,
		uint32_t worker_slot, uint64_t session_epoch);
enum vine_datavine_agent_resolve_status
vine_datavine_data_controller_agent_resolve(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		struct vine_datavine_agent_replica *replicas, size_t capacity,
		size_t *count, uint64_t *size, unsigned char digest[32],
		int *persisted);
int vine_datavine_data_controller_agent_wait(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint32_t worker_slot, uint64_t session_epoch, uint64_t request_id,
		uint32_t item_index);
int vine_datavine_data_controller_agent_fault(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint32_t worker_slot, uint64_t session_epoch, uint64_t object_token);
int vine_datavine_data_controller_agent_mark_dead(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation);
int vine_datavine_data_controller_agent_output_available(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id);
int vine_datavine_data_controller_agent_set_recovery(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, int active);
int vine_datavine_data_controller_agent_take_releases(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint32_t worker_slot,
		uint64_t session_epoch, uint64_t acknowledged_sequence,
		struct vine_datavine_agent_release *releases, size_t capacity,
		size_t *count);
int vine_datavine_data_controller_agent_stats(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, struct vine_datavine_agent_stats *stats);
int vine_datavine_data_controller_agent_persistence_metrics(
		struct vine_datavine_data_controller *controller,
		struct vine_datavine_agent_persistence_metrics *metrics);
int vine_datavine_data_controller_agent_check(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot);

int vine_datavine_data_controller_fetch_result(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		char **data, size_t *size);
int vine_datavine_data_controller_result_info(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		struct vine_datavine_workflow_result_info *result);
int vine_datavine_data_controller_result_descriptors(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, const uint64_t *data_ids, size_t count,
		struct vine_datavine_workflow_result_info *results, char **paths);
/* Return the next durably admitted requested result after sequence. Sequence
 * starts at one and follows DATA_READY_BATCH journal order, so reconnecting
 * clients can resume without consulting the bounded workflow event log. */
int vine_datavine_data_controller_next_requested_result(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t after_sequence,
		uint64_t *sequence,
		struct vine_datavine_workflow_result_info *result, char **path);
void vine_datavine_data_controller_set_result_notifier(
		struct vine_datavine_data_controller *controller,
		void (*notify)(void *), void *context);
int vine_datavine_data_controller_result_active(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id);
int vine_datavine_data_controller_result_counts(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, size_t *active, size_t *peak);
int vine_datavine_data_controller_requested_results_ready(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, size_t expected);
int vine_datavine_data_controller_finish_workflow(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id);
int vine_datavine_data_controller_take_workflow_losses(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id,
		uint64_t **data_ids, size_t *count);

#endif
