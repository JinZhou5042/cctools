/* Concurrent file-backed DataVine data controller. */

#include "vine_datavine_data_controller.h"
#include "vine_datavine_ir.h"

#include "create_dir.h"
#include "domain_name_cache.h"
#include "jx.h"
#include "link.h"
#include "stringtools.h"
#include "taskvine.h"
#include "vine_datavine_journal.h"
#include "vine_datavine_object_store.h"
#include "vine_datavine_protocol.h"
#include "vine_datavine_replica_table.h"
#include "vine_datavine_workflow_store.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/crypto.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define DATA_READY_BATCH 1
#define DATA_RELEASE_BATCH 2
#define DATA_QUEUE_LIMIT 4096
#define DATA_COMMIT_COALESCE_NS 1000000L
#define DATA_BACKGROUND_DIVISOR 4U
#define CATALOG_CHUNK_SHIFT 10U
#define CATALOG_CHUNK_SIZE (1U << CATALOG_CHUNK_SHIFT)
#define CATALOG_CHUNK_MASK (CATALOG_CHUNK_SIZE - 1U)
#define CATALOG_LOSS_QUEUED 0x01U
#define CATALOG_RESULT 0x02U
#define CATALOG_METADATA 0x04U
#define CATALOG_TAG_MASK 0x07U

struct agent_result_metadata;

struct data_result {
	struct vine_datavine_workflow_result_info info;
	char *path;
};

struct requested_result_record {
	uint64_t data_id;
	uint32_t attempt;
};

struct workflow_losses {
	uint64_t *data_ids;
	size_t count;
	size_t capacity;
	size_t active_results;
	size_t active_requested_results;
	size_t peak_results;
};

struct catalog_record {
	/* calloc pointers are at least 8-byte aligned, leaving three low tag bits. */
	uintptr_t state;
};

struct catalog_chunk {
	struct catalog_record records[CATALOG_CHUNK_SIZE];
};

struct publication_output {
	struct vine_datavine_workflow_result_info info;
	char *path;
	uint64_t workflow_slot;
	uint32_t background_worker_slot;
	int background;
};

struct publication_job {
	char *workflow_id;
	struct publication_output output;
	struct publication_job *next;
	uint64_t enqueued_at_nanoseconds;
	uint64_t prepared_at_nanoseconds;
	int background;
};

struct agent_persistence_counters {
	atomic_uint_fast64_t jobs;
	atomic_uint_fast64_t bytes;
	atomic_uint_fast64_t peak_queue_depth;
	atomic_uint_fast64_t enqueue_block_nanoseconds;
	atomic_uint_fast64_t background_jobs;
	atomic_uint_fast64_t background_bytes;
	atomic_uint_fast64_t background_peak_backlog;
};

struct agent_persistence_thread_counters {
	uint64_t failures;
	uint64_t retries;
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
} __attribute__((aligned(64)));

struct result_installation {
	struct data_result **pending;
	uint64_t *data_ids;
	const char *workflow_id;
	size_t count;
};

struct agent_namespace {
	char *workflow_id;
	unsigned char workflow_key[32];
	uint64_t workflow_slot;
	struct vine_datavine_replica_table *replicas;
	struct agent_endpoint *endpoints;
	size_t endpoint_capacity;
	uint32_t next_worker_slot;
};

struct agent_result_metadata {
	struct vine_datavine_workflow_result_info info;
	int persistence_queued;
};

struct agent_endpoint {
	char host[VINE_DATAVINE_AGENT_HOST_MAX];
	uint16_t port;
	struct agent_pull_connection *pull;
	struct vine_datavine_agent_release *releases;
	size_t release_count;
	size_t release_capacity;
	uint64_t next_release_sequence;
	int background_pull_active;
};

struct agent_release_context {
	struct agent_namespace *namespace;
	int valid;
};

struct agent_pull_connection;
static int agent_pull_connection_prepare(struct agent_endpoint *endpoint,
		const char *host, uint16_t port);
static void agent_pull_connection_delete(struct agent_pull_connection *connection);

static void agent_queue_release(uint64_t released_data_id,
		uint32_t released_generation,
		const struct vine_datavine_replica_view *replica, void *argument);
static int output_path(struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t attempt,
		char path[PATH_MAX]);
static struct publication_job *agent_persistence_job_create_locked(
		struct vine_datavine_data_controller *controller,
		struct agent_namespace *namespace,
		const struct vine_datavine_publish_record *record);
static int publication_job_enqueue(struct vine_datavine_data_controller *controller,
		struct publication_job *job);
static void agent_persistence_finish(
		struct vine_datavine_data_controller *controller,
		struct publication_job *job, int successful);
static void job_delete(struct publication_job *job);
static int digest_bytes(const char encoded[65], unsigned char digest[32]);
static int requested_result_reserve(
		struct vine_datavine_data_controller *controller, size_t additional);
static int requested_result_append(
		struct vine_datavine_data_controller *controller,
		const struct vine_datavine_workflow_result_info *info);

struct vine_datavine_data_controller {
	pthread_mutex_t lock;
	pthread_mutex_t queue_lock;
	pthread_cond_t queued;
	pthread_cond_t space;
	pthread_cond_t prepared;
	char *workflow_id;
	struct catalog_chunk **catalog;
	size_t catalog_capacity;
	struct workflow_losses losses;
	struct agent_namespace *workflow;
	struct vine_datavine_object_store *object_store;
	struct vine_datavine_journal *journal;
	char *root;
	char workflow_root[PATH_MAX];
	char *object_host;
	char *object_token;
	int object_port;
	pthread_t *threads;
	size_t thread_count;
	size_t queued_count;
	struct publication_job *head;
	struct publication_job *tail;
	struct publication_job *prepared_head;
	struct publication_job *prepared_tail;
	uint64_t *backup_queue;
	size_t backup_head;
	size_t backup_tail;
	size_t backup_count;
	size_t backup_capacity;
	size_t background_active;
	size_t background_limit;
	atomic_bool background_backup;
	uint64_t foreground_until_nanoseconds;
	int commit_active;
	atomic_uint_fast64_t loss_events;
	struct agent_persistence_counters persistence_metrics;
	struct agent_persistence_thread_counters *persistence_thread_metrics;
	struct data_worker_context *thread_contexts;
	int persistence_metrics_enabled;
	int loss_recording_failed;
	int stopping;
	int lock_initialized;
	int queue_lock_initialized;
	int queued_initialized;
	int space_initialized;
	int prepared_initialized;
	struct requested_result_record *requested_results;
	size_t requested_result_count;
	size_t requested_result_capacity;
	void (*result_notify)(void *);
	void *result_notify_context;
};

struct data_worker_context {
	struct vine_datavine_data_controller *controller;
	struct agent_persistence_thread_counters *metrics;
};

static uint64_t monotonic_nanoseconds(void)
{
	struct timespec now;
	clock_gettime(CLOCK_MONOTONIC, &now);
	return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
			(uint64_t)now.tv_nsec;
}

static void persistence_counter_add(atomic_uint_fast64_t *counter,
		uint64_t value)
{
	atomic_fetch_add_explicit(counter, value, memory_order_relaxed);
}

static void persistence_counter_max(atomic_uint_fast64_t *counter,
		uint64_t value)
{
	uint_fast64_t observed = atomic_load_explicit(counter, memory_order_relaxed);
	while (observed < value && !atomic_compare_exchange_weak_explicit(counter,
			&observed, value, memory_order_relaxed, memory_order_relaxed)) {
	}
}

static void persistence_counters_initialize(
		struct agent_persistence_counters *counters)
{
#define INITIALIZE_COUNTER(name) atomic_init(&counters->name, 0)
	INITIALIZE_COUNTER(jobs);
	INITIALIZE_COUNTER(bytes);
	INITIALIZE_COUNTER(peak_queue_depth);
	INITIALIZE_COUNTER(enqueue_block_nanoseconds);
	INITIALIZE_COUNTER(background_jobs);
	INITIALIZE_COUNTER(background_bytes);
	INITIALIZE_COUNTER(background_peak_backlog);
#undef INITIALIZE_COUNTER
}

/* The background backlog stores only DataIDs.  Replica identity, size and
 * digest remain in the dense replica table, so a million delayed backups cost
 * eight bytes each rather than a million heap-allocated publication jobs. */
static int backup_queue_reserve(
		struct vine_datavine_data_controller *controller, size_t additional)
{
	if (additional <= controller->backup_capacity - controller->backup_count)
		return 1;
	if (additional > SIZE_MAX - controller->backup_count)
		return 0;
	size_t required = controller->backup_count + additional;
	if (required > SIZE_MAX / sizeof(*controller->backup_queue))
		return 0;
	size_t capacity = controller->backup_capacity
			? controller->backup_capacity : 4096;
	while (capacity < required) {
		if (capacity > SIZE_MAX / 2 / sizeof(*controller->backup_queue)) {
			capacity = required;
			break;
		}
		capacity *= 2;
	}
	uint64_t *queue = malloc(capacity * sizeof(*queue));
	if (!queue)
		return 0;
	for (size_t index = 0; index < controller->backup_count; index++)
		queue[index] = controller->backup_queue[
				(controller->backup_head + index) % controller->backup_capacity];
	free(controller->backup_queue);
	controller->backup_queue = queue;
	controller->backup_head = 0;
	controller->backup_tail = controller->backup_count;
	controller->backup_capacity = capacity;
	return 1;
}

static int backup_queue_append_locked(
		struct vine_datavine_data_controller *controller, uint64_t data_id)
{
	if (!backup_queue_reserve(controller, 1))
		return 0;
	controller->backup_queue[controller->backup_tail] = data_id;
	controller->backup_tail = (controller->backup_tail + 1) %
			controller->backup_capacity;
	controller->backup_count++;
	persistence_counter_max(
			&controller->persistence_metrics.background_peak_backlog,
			controller->backup_count);
	pthread_cond_signal(&controller->queued);
	return 1;
}

static void agent_namespace_delete(void *value)
{
	struct agent_namespace *namespace = value;
	if (!namespace)
		return;
	vine_datavine_replica_table_delete(namespace->replicas);
	for (size_t index = 0; index < namespace->endpoint_capacity; index++) {
		agent_pull_connection_delete(namespace->endpoints[index].pull);
		free(namespace->endpoints[index].releases);
	}
	free(namespace->endpoints);
	free(namespace->workflow_id);
	free(namespace);
}

static int agent_endpoint_reserve(struct agent_namespace *namespace,
		uint32_t worker_slot)
{
	if (worker_slot < namespace->endpoint_capacity)
		return 1;
	size_t capacity = namespace->endpoint_capacity ?
			namespace->endpoint_capacity : 16;
	while (capacity <= worker_slot) {
		if (capacity > SIZE_MAX / 2)
			return 0;
		capacity *= 2;
	}
	struct agent_endpoint *next = realloc(namespace->endpoints,
			capacity * sizeof(*next));
	if (!next)
		return 0;
	memset(next + namespace->endpoint_capacity, 0,
			(capacity - namespace->endpoint_capacity) * sizeof(*next));
	namespace->endpoints = next;
	namespace->endpoint_capacity = capacity;
	return 1;
}

static uint64_t workflow_slot_from_key(const unsigned char key[32])
{
	uint64_t slot = vine_datavine_get_u64(key);
	return slot ? slot : UINT64_C(1);
}

static struct agent_namespace *agent_namespace_lookup(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot)
{
	return controller && controller->workflow && workflow_slot &&
			controller->workflow->workflow_slot == workflow_slot
			? controller->workflow : 0;
}

static int workflow_matches(struct vine_datavine_data_controller *controller,
		const char *workflow_id)
{
	return controller && controller->workflow_id && workflow_id &&
			!strcmp(controller->workflow_id, workflow_id);
}

static int workflow_bind(struct vine_datavine_data_controller *controller,
		const char *workflow_id)
{
	if (!controller || !workflow_id || !workflow_id[0])
		return 0;
	if (controller->workflow_id)
		return !strcmp(controller->workflow_id, workflow_id);
	unsigned char digest[SHA256_DIGEST_LENGTH];
	char encoded[SHA256_DIGEST_LENGTH * 2 + 1];
	SHA256((const unsigned char *)workflow_id, strlen(workflow_id), digest);
	for (size_t index = 0; index < sizeof(digest); index++)
		snprintf(encoded + index * 2, 3, "%02x", digest[index]);
	if (!controller->root || snprintf(controller->workflow_root,
			sizeof(controller->workflow_root), "%s/%s", controller->root,
			encoded) >= (int)sizeof(controller->workflow_root))
		return 0;
	controller->workflow_id = strdup(workflow_id);
	return controller->workflow_id != 0;
}

static struct agent_namespace *agent_namespace_by_id(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id)
{
	return workflow_matches(controller, workflow_id)
			? controller->workflow : 0;
}

static struct agent_namespace *agent_namespace_prepare(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id)
{
	if (!workflow_bind(controller, workflow_id))
		return 0;
	if (controller->workflow)
		return controller->workflow;
	struct agent_namespace *namespace = calloc(1, sizeof(*namespace));
	if (!namespace)
		return 0;
	namespace->workflow_id = strdup(workflow_id);
	unsigned int key_size = 0;
	if (!controller->object_token || !HMAC(EVP_sha256(),
			controller->object_token, (int)strlen(controller->object_token),
			(const unsigned char *)workflow_id, strlen(workflow_id),
			namespace->workflow_key, &key_size) || key_size != 32) {
		agent_namespace_delete(namespace);
		return 0;
	}
	namespace->workflow_slot = workflow_slot_from_key(namespace->workflow_key);
	namespace->replicas = vine_datavine_replica_table_create();
	if (!namespace->workflow_id || !namespace->replicas) {
		agent_namespace_delete(namespace);
		return 0;
	}
	controller->workflow = namespace;
	return namespace;
}

int vine_datavine_data_controller_put_object(
		struct vine_datavine_data_controller *controller, const char sha256[65],
		const void *data, size_t size, int *deduplicated)
{
	return controller && vine_datavine_object_store_put(
						 controller->object_store, sha256, data, size, deduplicated);
}

int vine_datavine_data_controller_get_object(
		struct vine_datavine_data_controller *controller, const char sha256[65],
		unsigned char **data, size_t *size)
{
	return controller && vine_datavine_object_store_get(
						 controller->object_store, sha256, data, size);
}

int vine_datavine_data_controller_configure_object_service(
		struct vine_datavine_data_controller *controller,
		const char *host, int port, const char *token)
{
	if (!controller || !host || !host[0] || port < 1 || port > 65535 ||
			!token || !token[0])
		return 0;
	char *host_copy = strdup(host);
	char *token_copy = strdup(token);
	if (!host_copy || !token_copy) {
		free(host_copy);
		free(token_copy);
		return 0;
	}
	free(controller->object_host);
	free(controller->object_token);
	controller->object_host = host_copy;
	controller->object_token = token_copy;
	controller->object_port = port;
	return 1;
}

char *vine_datavine_data_controller_object_ticket(
		struct vine_datavine_data_controller *controller,
		const char *sha256)
{
	if (!controller || !sha256 || strlen(sha256) != 64 ||
			!controller->object_host || !controller->object_host[0] ||
			!controller->object_token || !controller->object_token[0] ||
			controller->object_port < 1 || controller->object_port > 65535)
		return 0;
	unsigned char message[84] = "datavine-object-v1:";
	memcpy(message + 19, sha256, 64);
	unsigned char signature[EVP_MAX_MD_SIZE];
	unsigned int signature_size = 0;
	if (!HMAC(EVP_sha256(), controller->object_token,
			(int)strlen(controller->object_token), message, 83,
			signature, &signature_size) || signature_size != 32)
		return 0;
	char encoded[65];
	static const char hexadecimal[] = "0123456789abcdef";
	for (size_t index = 0; index < 32; index++) {
		encoded[index * 2] = hexadecimal[signature[index] >> 4];
		encoded[index * 2 + 1] = hexadecimal[signature[index] & 15];
	}
	encoded[64] = 0;
	return strchr(controller->object_host, ':')
			? string_format("datavine://[%s]:%d/%s/%s",
					controller->object_host, controller->object_port,
					sha256, encoded)
			: string_format("datavine://%s:%d/%s/%s",
					controller->object_host, controller->object_port,
					sha256, encoded);
}

int vine_datavine_data_controller_agent_endpoint(
		struct vine_datavine_data_controller *controller, char host[64],
		uint16_t *port)
{
	if (!controller || !host || !port || !controller->object_host ||
			!controller->object_host[0] ||
			strlen(controller->object_host) >= 64 ||
			controller->object_port < 1 || controller->object_port > UINT16_MAX)
		return 0;
	snprintf(host, 64, "%s", controller->object_host);
	*port = (uint16_t)controller->object_port;
	return 1;
}

static void data_result_delete(void *value)
{
	struct data_result *result = value;
	if (result) {
		free(result->path);
		free(result);
	}
}

static struct catalog_record *catalog_lookup(
		struct vine_datavine_data_controller *controller, uint64_t data_id)
{
	if (!controller || !data_id)
		return 0;
	uint64_t chunk = data_id >> CATALOG_CHUNK_SHIFT;
	if (chunk >= controller->catalog_capacity || !controller->catalog[chunk])
		return 0;
	return &controller->catalog[chunk]->records[data_id & CATALOG_CHUNK_MASK];
}

static struct data_result *catalog_result(struct catalog_record *record)
{
	return record && (record->state & CATALOG_RESULT)
			? (struct data_result *)(record->state & ~(uintptr_t)CATALOG_TAG_MASK)
			: 0;
}

static struct agent_result_metadata *catalog_metadata(
		struct catalog_record *record)
{
	return record && (record->state & CATALOG_METADATA)
			? (struct agent_result_metadata *)(record->state &
					~(uintptr_t)CATALOG_TAG_MASK)
			: 0;
}

static void catalog_set_metadata(struct catalog_record *record,
		struct agent_result_metadata *metadata)
{
	record->state = (record->state & CATALOG_LOSS_QUEUED) |
			(uintptr_t)metadata | (metadata ? CATALOG_METADATA : 0);
}

static void catalog_set_result(struct catalog_record *record,
		struct data_result *result)
{
	free(catalog_metadata(record));
	record->state = (record->state & CATALOG_LOSS_QUEUED) |
			(uintptr_t)result | (result ? CATALOG_RESULT : 0);
}

static struct catalog_record *catalog_get(
		struct vine_datavine_data_controller *controller, uint64_t data_id)
{
	if (!controller || !data_id)
		return 0;
	uint64_t chunk = data_id >> CATALOG_CHUNK_SHIFT;
	if (chunk > SIZE_MAX / sizeof(*controller->catalog) - 1)
		return 0;
	if (chunk >= controller->catalog_capacity) {
		size_t capacity = controller->catalog_capacity
				? controller->catalog_capacity : 16;
		while (capacity <= chunk) {
			if (capacity > SIZE_MAX / 2)
				return 0;
			capacity *= 2;
		}
		struct catalog_chunk **next = realloc(controller->catalog,
				capacity * sizeof(*next));
		if (!next)
			return 0;
		memset(next + controller->catalog_capacity, 0,
				(capacity - controller->catalog_capacity) * sizeof(*next));
		controller->catalog = next;
		controller->catalog_capacity = capacity;
	}
	if (!controller->catalog[chunk]) {
		controller->catalog[chunk] = calloc(1, sizeof(struct catalog_chunk));
		if (!controller->catalog[chunk])
			return 0;
	}
	return &controller->catalog[chunk]->records[data_id & CATALOG_CHUNK_MASK];
}

static void catalog_delete(struct vine_datavine_data_controller *controller)
{
	if (!controller)
		return;
	for (size_t chunk = 0; chunk < controller->catalog_capacity; chunk++) {
		if (!controller->catalog[chunk])
			continue;
		for (size_t index = 0; index < CATALOG_CHUNK_SIZE; index++) {
			struct catalog_record *record =
					&controller->catalog[chunk]->records[index];
			data_result_delete(catalog_result(record));
			free(catalog_metadata(record));
		}
		free(controller->catalog[chunk]);
	}
	free(controller->catalog);
}

static struct workflow_losses *workflow_losses_get(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, int create)
{
	(void)create;
	return workflow_matches(controller, workflow_id)
			? &controller->losses : 0;
}

static int workflow_loss_append(struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	struct workflow_losses *losses = workflow_losses_get(
			controller, workflow_id, 1);
	if (!losses)
		return 0;
	struct catalog_record *record = catalog_get(controller, data_id);
	if (!record)
		return 0;
	if (record->state & CATALOG_LOSS_QUEUED)
		return 1;
	if (losses->count == losses->capacity) {
		size_t capacity = losses->capacity ? losses->capacity * 2 : 16;
		if (capacity > SIZE_MAX / sizeof(*losses->data_ids))
			return 0;
		uint64_t *data_ids = realloc(losses->data_ids,
				capacity * sizeof(*data_ids));
		if (!data_ids)
			return 0;
		losses->data_ids = data_ids;
		losses->capacity = capacity;
	}
	record->state |= CATALOG_LOSS_QUEUED;
	losses->data_ids[losses->count++] = data_id;
	atomic_fetch_add(&controller->loss_events, 1);
	return 1;
}

uint64_t vine_datavine_data_controller_loss_events(
		struct vine_datavine_data_controller *controller)
{
	return controller ? atomic_load(&controller->loss_events) : 0;
}

static struct data_result *result_lookup(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	struct catalog_record *record = workflow_matches(controller, workflow_id)
			? catalog_lookup(controller, data_id) : 0;
	return catalog_result(record);
}

static int result_insert(struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, struct data_result *result)
{
	struct catalog_record *record = workflow_matches(controller, workflow_id)
			? catalog_get(controller, data_id) : 0;
	struct workflow_losses *losses = workflow_losses_get(
			controller, workflow_id, 1);
	if (!record || catalog_result(record) || !losses)
		return 0;
	catalog_set_result(record, result);
	losses->active_results++;
	if (result->info.requested) {
		losses->active_requested_results++;
	}
	if (losses->active_results > losses->peak_results)
		losses->peak_results = losses->active_results;
	return 1;
}

static struct data_result *result_remove(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	struct catalog_record *record = workflow_matches(controller, workflow_id)
			? catalog_lookup(controller, data_id) : 0;
	struct data_result *result = catalog_result(record);
	if (result)
		record->state &= CATALOG_LOSS_QUEUED;
	if (result && controller->losses.active_results)
		controller->losses.active_results--;
	if (result && result->info.requested &&
			controller->losses.active_requested_results)
		controller->losses.active_requested_results--;
	return result;
}

int vine_datavine_data_controller_take_workflow_losses(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id,
		uint64_t **data_ids, size_t *count)
{
	if (!controller || !workflow_id || !workflow_id[0] || !data_ids || !count)
		return 0;
	*data_ids = 0;
	*count = 0;
	pthread_mutex_lock(&controller->lock);
	if (controller->loss_recording_failed) {
		pthread_mutex_unlock(&controller->lock);
		return 0;
	}
	struct workflow_losses *losses = workflow_losses_get(
			controller, workflow_id, 0);
	uint64_t *lost = losses ? losses->data_ids : 0;
	size_t lost_count = losses ? losses->count : 0;
	if (losses) {
		losses->data_ids = 0;
		losses->count = 0;
		losses->capacity = 0;
		for (size_t index = 0; index < lost_count; index++) {
			struct catalog_record *record = catalog_lookup(
					controller, lost[index]);
			if (record)
				record->state &= ~CATALOG_LOSS_QUEUED;
		}
	}
	size_t next = 0;
	struct agent_namespace *namespace = workflow_matches(controller, workflow_id)
			? controller->workflow : 0;
	for (size_t index = 0; index < lost_count; index++) {
		struct data_result *removed = result_lookup(
				controller, workflow_id, lost[index]);
		if (removed) {
			/* A durable requested result is independent of Worker replicas. */
			continue;
		}
		/* Worker-Agent intermediates never enter Manager's file/result tables.
		 * If another exact replica arrived before this drain, the loss has
		 * already healed. Otherwise return the DataID for physical replay. */
		enum vine_datavine_resolve_status status = namespace
				? vine_datavine_replica_table_resolve(namespace->replicas,
						lost[index], 0, 0, 0, 0, 0, 0, 0)
				: VINE_DATAVINE_RESOLVE_UNKNOWN;
		if (status == VINE_DATAVINE_RESOLVE_PENDING)
			lost[next++] = lost[index];
	}
	pthread_mutex_unlock(&controller->lock);
	if (!next) {
		free(lost);
		lost = 0;
	}
	*data_ids = lost;
	*count = next;
	return 1;
}

static int workflow_directory_path(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, char path[PATH_MAX])
{
	if (!workflow_matches(controller, workflow_id) ||
			snprintf(path, PATH_MAX, "%s", controller->workflow_root) >= PATH_MAX)
		return 0;
	return 1;
}

int vine_datavine_data_controller_workflow_key(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, unsigned char key[32])
{
	if (!controller || !workflow_id || !workflow_id[0] || !key)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_by_id(
			controller, workflow_id);
	int valid = namespace != 0;
	if (valid)
		memcpy(key, namespace->workflow_key, sizeof(namespace->workflow_key));
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_prepare_workflow(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id)
{
	char path[PATH_MAX];
	if (!controller || !workflow_id || !workflow_id[0])
		return 0;
	pthread_mutex_lock(&controller->lock);
	int valid = workflow_bind(controller, workflow_id) &&
			workflow_directory_path(controller, workflow_id, path) &&
			create_dir(path, 0700) &&
			agent_namespace_prepare(controller, workflow_id) != 0;
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_set_idata_backup(
		struct vine_datavine_data_controller *controller, int background)
{
	if (!controller)
		return 0;
	pthread_mutex_lock(&controller->queue_lock);
	/* One Controller owns one workflow.  Do not change policy after data has
	 * entered either persistence queue. Reapplying the same policy when a
	 * streaming workflow resumes is always idempotent. */
	int requested = !!background;
	int current = atomic_load_explicit(&controller->background_backup,
			memory_order_acquire);
	int valid = current == requested ||
			(!controller->head && !controller->prepared_head &&
			 !controller->backup_count && !controller->background_active);
	if (valid && current != requested)
		atomic_store_explicit(&controller->background_backup, !!background,
				memory_order_release);
	pthread_mutex_unlock(&controller->queue_lock);
	return valid;
}

static void backup_ticket_message(unsigned char message[64],
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint64_t size, const unsigned char digest[32])
{
	memset(message, 0, 64);
	memcpy(message, "DVB1", 4);
	vine_datavine_put_u64(message + 4, workflow_slot);
	vine_datavine_put_u64(message + 12, data_id);
	vine_datavine_put_u32(message + 20, generation);
	vine_datavine_put_u64(message + 24, size);
	memcpy(message + 32, digest, 32);
}

int vine_datavine_data_controller_validate_backup_ticket(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint64_t size, const unsigned char digest[32],
		const unsigned char signature[32])
{
	if (!controller || !workflow_slot || !data_id || !generation ||
			!digest || !signature)
		return 0;
	unsigned char key[32];
	int valid = 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	struct data_result *stored = namespace
			? result_lookup(controller, namespace->workflow_id, data_id) : 0;
	if (stored && stored->info.attempt == generation &&
			stored->info.size == size) {
		unsigned char expected_digest[32];
		valid = digest_bytes(stored->info.sha256, expected_digest) &&
				!CRYPTO_memcmp(expected_digest, digest, 32);
		if (valid)
			memcpy(key, namespace->workflow_key, sizeof(key));
	}
	pthread_mutex_unlock(&controller->lock);
	if (!valid)
		return 0;
	unsigned char message[64];
	unsigned char expected[EVP_MAX_MD_SIZE];
	unsigned int expected_size = 0;
	backup_ticket_message(message, workflow_slot, data_id, generation,
			size, digest);
	return HMAC(EVP_sha256(), key, sizeof(key), message, sizeof(message),
			expected, &expected_size) && expected_size == 32 &&
			!CRYPTO_memcmp(expected, signature, 32);
}

int vine_datavine_data_controller_read_backup(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint64_t offset, size_t requested, unsigned char **data, size_t *size)
{
	if (!controller || !workflow_slot || !data_id || !generation ||
			!requested || !data || !size)
		return 0;
	*data = 0;
	*size = 0;
	int fd = -1;
	uint64_t total = 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	struct data_result *stored = namespace
			? result_lookup(controller, namespace->workflow_id, data_id) : 0;
	if (stored && stored->info.attempt == generation && stored->path) {
		total = stored->info.size;
		fd = open(stored->path, O_RDONLY | O_CLOEXEC);
	}
	pthread_mutex_unlock(&controller->lock);
	if (fd < 0 || offset > total) {
		if (fd >= 0)
			close(fd);
		return 0;
	}
	size_t count = requested;
	if ((uint64_t)count > total - offset)
		count = (size_t)(total - offset);
	unsigned char *buffer = malloc(count ? count : 1);
	if (!buffer) {
		close(fd);
		return 0;
	}
	size_t used = 0;
	while (used < count) {
		ssize_t got = pread(fd, buffer + used, count - used,
				(off_t)(offset + used));
		if (got > 0)
			used += (size_t)got;
		else if (got < 0 && errno == EINTR)
			continue;
		else
			break;
	}
	int valid = used == count && close(fd) == 0;
	if (!valid) {
		free(buffer);
		return 0;
	}
	*data = buffer;
	*size = count;
	return 1;
}

struct agent_loss_context {
	struct vine_datavine_data_controller *controller;
	struct agent_namespace *namespace;
};

static void agent_last_replica_lost(
		uint64_t data_id, uint32_t generation, void *argument)
{
	(void)generation;
	struct agent_loss_context *context = argument;
	if (!workflow_loss_append(context->controller,
			context->namespace->workflow_id, data_id))
		context->controller->loss_recording_failed = 1;
}

int vine_datavine_data_controller_agent_hello(
		struct vine_datavine_data_controller *controller,
		const unsigned char workflow_key[32], uint32_t *worker_slot,
		uint64_t session_epoch, const char *host, uint16_t port,
		uint64_t *workflow_slot)
{
	if (!controller || !workflow_key || !worker_slot || !session_epoch ||
			!host || !host[0] || strlen(host) >= VINE_DATAVINE_AGENT_HOST_MAX ||
			!port || !workflow_slot)
		return 0;
	uint64_t slot = workflow_slot_from_key(workflow_key);
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(controller, slot);
	uint32_t assigned = *worker_slot;
	if (namespace && !assigned) {
		assigned = ++namespace->next_worker_slot;
		if (!assigned)
			assigned = ++namespace->next_worker_slot;
	} else if (namespace && assigned > namespace->next_worker_slot) {
		namespace->next_worker_slot = assigned;
	}
	int valid = namespace && assigned &&
		!CRYPTO_memcmp(namespace->workflow_key, workflow_key,
			sizeof(namespace->workflow_key)) &&
		agent_endpoint_reserve(namespace, assigned) &&
		agent_pull_connection_prepare(&namespace->endpoints[assigned], host,
				port) &&
		vine_datavine_replica_table_session_open(
			namespace->replicas, assigned, session_epoch);
	pthread_mutex_unlock(&controller->lock);
	if (valid) {
		*worker_slot = assigned;
		*workflow_slot = slot;
	}
	return valid;
}

int vine_datavine_data_controller_agent_expect(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t *generation)
{
	if (!controller || !workflow_id || !workflow_id[0] || !data_id ||
			!generation || !*generation)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_by_id(
			controller, workflow_id);
	int valid = namespace && catalog_get(controller, data_id) &&
			vine_datavine_replica_table_expect(
			namespace->replicas, data_id, generation);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_agent_expect_result(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t generation,
		int64_t producer_task_id, int32_t producer_output_index,
		const char *codec_name, const char *codec_version)
{
	if (!controller || !workflow_id || !workflow_id[0] || !data_id ||
			!generation || producer_task_id < 1 || producer_output_index < 0 ||
			!codec_name || !codec_version ||
			strlen(codec_name) >=
				sizeof(((struct vine_datavine_workflow_result_info *)0)->codec_name) ||
			strlen(codec_version) >=
				sizeof(((struct vine_datavine_workflow_result_info *)0)->codec_version))
		return 0;
	struct publication_job *persistence = 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_by_id(
			controller, workflow_id);
	struct catalog_record *record = namespace
			? catalog_get(controller, data_id) : 0;
	struct agent_result_metadata *known = catalog_metadata(record);
	int valid = record != 0;
	struct data_result *stored = valid
			? result_lookup(controller, workflow_id, data_id) : 0;
	if (stored) {
		if (!stored->info.requested && stored->info.attempt == generation) {
			valid = requested_result_reserve(controller, 1);
			if (valid) {
				stored->info.producer_task_id = producer_task_id;
				stored->info.producer_output_index = producer_output_index;
				stored->info.requested = 1;
				snprintf(stored->info.codec_name,
						sizeof(stored->info.codec_name), "%s", codec_name);
				snprintf(stored->info.codec_version,
						sizeof(stored->info.codec_version), "%s", codec_version);
				valid = requested_result_append(controller, &stored->info);
				if (valid)
					controller->losses.active_requested_results++;
				else
					stored->info.requested = 0;
			}
		} else {
			valid = stored->info.attempt == generation &&
					stored->info.producer_task_id == producer_task_id &&
					stored->info.producer_output_index == producer_output_index &&
					!strcmp(stored->info.codec_name, codec_name) &&
					!strcmp(stored->info.codec_version, codec_version);
		}
		pthread_mutex_unlock(&controller->lock);
		if (valid && controller->result_notify)
			controller->result_notify(controller->result_notify_context);
		return valid;
	}
	if (valid && known) {
		valid = known->info.producer_task_id == producer_task_id &&
				known->info.producer_output_index == producer_output_index &&
				!strcmp(known->info.codec_name, codec_name) &&
				!strcmp(known->info.codec_version, codec_version);
		/* A failed physical attempt may have registered this durable result
		 * before it ran.  No payload was admitted, so the next attempt replaces
		 * only the generation in place. */
		if (valid)
			known->info.attempt = generation;
	} else if (valid) {
		known = calloc(1, sizeof(*known));
		if (known) {
			known->info.data_id = data_id;
			known->info.attempt = generation;
			known->info.producer_task_id = producer_task_id;
			known->info.producer_output_index = producer_output_index;
			known->info.requested = 1;
			snprintf(known->info.codec_name, sizeof(known->info.codec_name),
					"%s", codec_name);
			snprintf(known->info.codec_version,
					sizeof(known->info.codec_version), "%s", codec_version);
		}
		valid = known != 0;
		if (valid)
			catalog_set_metadata(record, known);
		else
			free(known);
	}
	/* A streaming delta may request an output after its volatile replica was
	 * already published.  Promote that existing identity without waiting for a
	 * duplicate DATA_READY record from the Worker.  The ordinary pre-execution
	 * call reaches this same code with no replica and remains metadata-only. */
	if (valid && known && !known->persistence_queued &&
			!result_lookup(controller, workflow_id, data_id)) {
		struct vine_datavine_replica_view replica;
		size_t count = 0;
		uint64_t size = 0;
		unsigned char digest[32];
		int persisted = 0;
		enum vine_datavine_resolve_status status =
				vine_datavine_replica_table_resolve(namespace->replicas,
						data_id, generation, &replica, 1, &count, &size,
						digest, &persisted);
		if (status == VINE_DATAVINE_RESOLVE_AVAILABLE && count &&
				!persisted) {
			struct vine_datavine_publish_record record = {
					.data_id = data_id,
					.size = size,
					.object_token = replica.object_token,
					.generation = replica.generation,
					.requested = 1,
			};
			memcpy(record.digest, digest, sizeof(record.digest));
			persistence = agent_persistence_job_create_locked(controller,
					namespace, &record);
		}
	}
	pthread_mutex_unlock(&controller->lock);
	if (persistence && !publication_job_enqueue(controller, persistence)) {
		agent_persistence_finish(controller, persistence, 0);
		job_delete(persistence);
		valid = 0;
	}
	return valid;
}

int vine_datavine_data_controller_agent_session_lost(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint32_t worker_slot,
		uint64_t session_epoch)
{
	if (!controller || !workflow_slot || !session_epoch)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	struct agent_loss_context context = {controller, namespace};
	int valid = namespace && vine_datavine_replica_table_session_lost(
			namespace->replicas, worker_slot, session_epoch,
			agent_last_replica_lost, 0, &context);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_agent_publish(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint64_t size, const unsigned char digest[32], uint32_t worker_slot,
		uint64_t session_epoch, uint64_t object_token)
{
	if (!controller || !workflow_slot || !data_id || !generation || !digest)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	int valid = namespace && vine_datavine_replica_table_publish(
			namespace->replicas, data_id, generation, size, digest,
			worker_slot, session_epoch, object_token, 0, 0);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_agent_publish_batch(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot,
		const struct vine_datavine_publish_record *records, size_t count,
		uint32_t worker_slot, uint64_t session_epoch)
{
	if (!controller || !workflow_slot || !records || !count)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	int valid = namespace && vine_datavine_replica_table_publish_batch(
			namespace->replicas, records, count, worker_slot, session_epoch,
			0, 0);
	struct publication_job *persistence = 0;
	struct publication_job **persistence_tail = &persistence;
	uint64_t background_ids[VINE_DATAVINE_AGENT_MAX_BATCH];
	size_t background_count = 0;
	for (size_t index = 0; valid && index < count; index++) {
		/* Controller metadata is authoritative for late result promotion.  A
		 * task spec created before the append may still publish requested=0. */
		struct catalog_record *record = catalog_lookup(
				controller, records[index].data_id);
		if (!records[index].requested && !catalog_metadata(record)) {
			if (atomic_load_explicit(&controller->background_backup,
					memory_order_acquire)) {
				int queued = vine_datavine_replica_table_queue_backup(
						namespace->replicas, records[index].data_id,
						records[index].generation);
				valid = queued != 0;
				if (queued == 1)
					background_ids[background_count++] = records[index].data_id;
			}
			continue;
		}
		struct publication_job *job = agent_persistence_job_create_locked(
				controller, namespace, &records[index]);
		if (job) {
			*persistence_tail = job;
			persistence_tail = &job->next;
		} else {
			struct agent_result_metadata *metadata = catalog_metadata(record);
			valid = metadata && (metadata->persistence_queued ||
					result_lookup(controller, namespace->workflow_id,
						records[index].data_id));
		}
	}
	/* A consumer can finish and retire its input before the producing worker's
	 * deliberately asynchronous DATA_READY reaches us.  The table accepts that
	 * exact late generation as a tombstone; return a release so it cannot leak
	 * in the worker cache. */
	if (valid) {
		struct agent_release_context context = {namespace, 1};
		for (size_t index = 0; index < count; index++) {
			enum vine_datavine_resolve_status status =
					vine_datavine_replica_table_resolve(namespace->replicas,
						records[index].data_id, records[index].generation,
						0, 0, 0, 0, 0, 0);
			if (status == VINE_DATAVINE_RESOLVE_DEAD) {
				struct vine_datavine_replica_view replica = {
						.worker_slot = worker_slot,
						.session_epoch = session_epoch,
						.object_token = records[index].object_token,
						.generation = records[index].generation,
				};
				agent_queue_release(records[index].data_id,
						records[index].generation, &replica, &context);
			}
		}
		valid = context.valid;
	}
	pthread_mutex_unlock(&controller->lock);
	if (background_count) {
		pthread_mutex_lock(&controller->queue_lock);
		for (size_t index = 0; valid && index < background_count; index++)
			valid = backup_queue_append_locked(controller, background_ids[index]);
		pthread_mutex_unlock(&controller->queue_lock);
		if (!valid) {
			pthread_mutex_lock(&controller->lock);
			for (size_t index = 0; index < background_count; index++)
				vine_datavine_replica_table_clear_backup(namespace->replicas,
						background_ids[index], 0);
			pthread_mutex_unlock(&controller->lock);
		}
	}
	while (persistence) {
		struct publication_job *next = persistence->next;
		persistence->next = 0;
		if (!publication_job_enqueue(controller, persistence)) {
			agent_persistence_finish(controller, persistence, 0);
			job_delete(persistence);
			valid = 0;
		}
		persistence = next;
	}
	return valid;
}

enum vine_datavine_agent_resolve_status
vine_datavine_data_controller_agent_resolve(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		struct vine_datavine_agent_replica *replicas, size_t capacity,
		size_t *count, uint64_t *size, unsigned char digest[32],
		int *persisted)
{
	if (!controller || !workflow_slot || !data_id)
		return VINE_DATAVINE_AGENT_UNKNOWN;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	struct vine_datavine_replica_view local_views[16];
	struct vine_datavine_replica_view *views = capacity <= 16
			? local_views
			: calloc(capacity, sizeof(*views));
	enum vine_datavine_resolve_status status = namespace
			&& views
			? vine_datavine_replica_table_resolve(namespace->replicas,
				  data_id, generation, views, capacity,
				  count, size, digest, persisted)
			: VINE_DATAVINE_RESOLVE_UNKNOWN;
	/* The persistence journal is replayed before the Worker namespace exists.
	 * Rehydrate its immutable identity on first use instead of scanning every
	 * catalog entry at restart.  This makes an output consumed by a later
	 * dynamic delta follow exactly the same resolve path as a static edge. */
	struct data_result *durable = namespace
			? result_lookup(controller, namespace->workflow_id, data_id) : 0;
	if (namespace && views && durable &&
			(status == VINE_DATAVINE_RESOLVE_UNKNOWN ||
			 status == VINE_DATAVINE_RESOLVE_PENDING) &&
			(!generation || generation == durable->info.attempt)) {
		unsigned char durable_digest[32];
		if (digest_bytes(durable->info.sha256, durable_digest) &&
				vine_datavine_replica_table_restore_persisted(
					namespace->replicas, data_id, durable->info.attempt,
					durable->info.size, durable_digest))
			status = vine_datavine_replica_table_resolve(
					namespace->replicas, data_id, generation, views, capacity,
					count, size, digest, persisted);
	}
	if (namespace && status == VINE_DATAVINE_RESOLVE_AVAILABLE &&
			replicas && count) {
		for (size_t index = 0; index < *count; index++) {
			replicas[index].worker_slot = views[index].worker_slot;
			replicas[index].session_epoch = views[index].session_epoch;
			replicas[index].object_token = views[index].object_token;
			replicas[index].generation = views[index].generation;
			uint32_t worker = views[index].worker_slot;
			if (worker >= namespace->endpoint_capacity ||
					!namespace->endpoints[worker].port) {
				status = VINE_DATAVINE_RESOLVE_PENDING;
				*count = 0;
				break;
			}
			snprintf(replicas[index].host, sizeof(replicas[index].host),
					"%s", namespace->endpoints[worker].host);
			replicas[index].port = namespace->endpoints[worker].port;
		}
		if (!*count && persisted && *persisted && capacity &&
				controller->object_host && controller->object_port > 0) {
			struct data_result *backup = result_lookup(controller,
					namespace->workflow_id, data_id);
			if (!backup) {
				status = VINE_DATAVINE_RESOLVE_PENDING;
				goto resolved;
			}
			memset(&replicas[0], 0, sizeof(replicas[0]));
			replicas[0].generation = backup->info.attempt;
			snprintf(replicas[0].host, sizeof(replicas[0].host), "%s",
					controller->object_host);
			replicas[0].port = (uint16_t)controller->object_port;
			*count = 1;
		}
	}

resolved:
	if (views != local_views)
		free(views);
	pthread_mutex_unlock(&controller->lock);
	/* A resolve usually precedes a Worker-to-Worker or Controller fallback
	 * transfer.  Hold admission of new background copies briefly; in-flight
	 * copies finish, while all RPC and foreground result threads remain free. */
	pthread_mutex_lock(&controller->queue_lock);
	uint64_t foreground_until = monotonic_nanoseconds() + UINT64_C(50000000);
	if (foreground_until > controller->foreground_until_nanoseconds)
		controller->foreground_until_nanoseconds = foreground_until;
	pthread_mutex_unlock(&controller->queue_lock);
	return (enum vine_datavine_agent_resolve_status)status;
}

int vine_datavine_data_controller_agent_wait(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint32_t worker_slot, uint64_t session_epoch, uint64_t request_id,
		uint32_t item_index)
{
	if (!controller || !workflow_slot || !data_id || !request_id)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	int valid = namespace && vine_datavine_replica_table_wait(
			namespace->replicas, data_id, generation, worker_slot,
			session_epoch, request_id, item_index);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_agent_fault(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint32_t worker_slot, uint64_t session_epoch, uint64_t object_token)
{
	if (!controller || !workflow_slot || !data_id || !generation)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	struct agent_loss_context context = {controller, namespace};
	int valid = namespace && vine_datavine_replica_table_fault(
			namespace->replicas, data_id, generation, worker_slot,
			session_epoch, object_token, agent_last_replica_lost, &context);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

static void agent_queue_release(uint64_t released_data_id,
		uint32_t released_generation,
		const struct vine_datavine_replica_view *replica, void *argument)
{
	struct agent_release_context *context = argument;
	if (!context->namespace ||
			replica->worker_slot >= context->namespace->endpoint_capacity) {
		context->valid = 0;
		return;
	}
	struct agent_endpoint *endpoint =
			&context->namespace->endpoints[replica->worker_slot];
	if (endpoint->release_count == endpoint->release_capacity) {
		size_t capacity = endpoint->release_capacity ?
				endpoint->release_capacity * 2 : 256;
		void *next = realloc(endpoint->releases,
				capacity * sizeof(*endpoint->releases));
		if (!next) {
			context->valid = 0;
			return;
		}
		endpoint->releases = next;
		endpoint->release_capacity = capacity;
	}
	uint64_t sequence = ++endpoint->next_release_sequence;
	if (!sequence)
		sequence = ++endpoint->next_release_sequence;
	endpoint->releases[endpoint->release_count++] =
			(struct vine_datavine_agent_release){
					.sequence = sequence,
					.data_id = released_data_id,
					.object_token = replica->object_token,
					.generation = released_generation,
			};
}

int vine_datavine_data_controller_agent_mark_dead(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation)
{
	if (!controller || !workflow_slot || !data_id)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	struct agent_release_context context = {namespace, 1};
	enum vine_datavine_resolve_status before = namespace
			? vine_datavine_replica_table_resolve(namespace->replicas, data_id,
					generation, 0, 0, 0, 0, 0, 0)
			: VINE_DATAVINE_RESOLVE_UNKNOWN;
	int valid = namespace && vine_datavine_replica_table_mark_dead(
			namespace->replicas, data_id, generation, agent_queue_release, 0,
			&context) && context.valid;
	struct data_result *removed = valid
			? result_lookup(controller, namespace->workflow_id, data_id) : 0;
	if (removed && !removed->info.requested) {
		removed = result_remove(controller, namespace->workflow_id, data_id);
		if (removed && removed->path)
			unlink(removed->path);
		data_result_delete(removed);
	}
	if (!valid && getenv("DATAVINE_WORKFLOW_METRICS"))
		fprintf(stderr,
				"datavine controller mark_dead_failed slot=%llu data=%llu generation=%u namespace=%d before=%d release_valid=%d\n",
				(unsigned long long)workflow_slot,
				(unsigned long long)data_id, generation, namespace != 0,
				(int)before, context.valid);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_agent_output_available(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	if (!controller || !workflow_id || !workflow_id[0] || !data_id)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_by_id(
			controller, workflow_id);
	enum vine_datavine_resolve_status status = namespace
			? vine_datavine_replica_table_resolve(namespace->replicas, data_id,
					0, 0, 0, 0, 0, 0, 0)
			: VINE_DATAVINE_RESOLVE_UNKNOWN;
	pthread_mutex_unlock(&controller->lock);
	return status == VINE_DATAVINE_RESOLVE_AVAILABLE;
}

int vine_datavine_data_controller_agent_set_recovery(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, int active)
{
	if (!controller || !workflow_id || !workflow_id[0] || !data_id)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_by_id(
			controller, workflow_id);
	int valid = namespace && vine_datavine_replica_table_set_recovery(
			namespace->replicas, data_id, 0, active);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_agent_take_releases(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint32_t worker_slot,
		uint64_t session_epoch, uint64_t acknowledged_sequence,
		struct vine_datavine_agent_release *releases, size_t capacity,
		size_t *count)
{
	if (count)
		*count = 0;
	if (!controller || !workflow_slot || !session_epoch || !releases ||
			!capacity || !count)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	int valid = namespace && worker_slot < namespace->endpoint_capacity &&
			vine_datavine_replica_table_session_active(namespace->replicas,
					worker_slot, session_epoch);
	struct agent_endpoint *endpoint = valid
			? &namespace->endpoints[worker_slot] : 0;
	if (valid) {
		size_t acknowledged = 0;
		while (acknowledged < endpoint->release_count &&
				endpoint->releases[acknowledged].sequence <=
					acknowledged_sequence)
			acknowledged++;
		if (acknowledged) {
			memmove(endpoint->releases,
					endpoint->releases + acknowledged,
					(endpoint->release_count - acknowledged) *
						sizeof(*endpoint->releases));
			endpoint->release_count -= acknowledged;
		}
		*count = endpoint->release_count < capacity
				? endpoint->release_count : capacity;
		memcpy(releases, endpoint->releases,
				*count * sizeof(*releases));
	}
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

int vine_datavine_data_controller_agent_stats(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, struct vine_datavine_agent_stats *stats)
{
	if (!controller || !workflow_slot || !stats)
		return 0;
	struct vine_datavine_replica_stats internal;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	int valid = namespace && vine_datavine_replica_table_stats(
			namespace->replicas, &internal);
	pthread_mutex_unlock(&controller->lock);
	if (valid) {
		stats->active_data = internal.active_data;
		stats->active_replicas = internal.active_replicas;
		stats->active_waiters = internal.active_waiters;
		stats->active_sessions = internal.active_sessions;
		stats->peak_data = internal.peak_data;
		stats->peak_replicas = internal.peak_replicas;
		stats->peak_waiters = internal.peak_waiters;
	}
	return valid;
}

int vine_datavine_data_controller_agent_persistence_metrics(
		struct vine_datavine_data_controller *controller,
		struct vine_datavine_agent_persistence_metrics *metrics)
{
	if (!controller || !metrics)
		return 0;
	memset(metrics, 0, sizeof(*metrics));
	metrics->jobs = atomic_load_explicit(&controller->persistence_metrics.jobs,
			memory_order_relaxed);
	metrics->bytes = atomic_load_explicit(&controller->persistence_metrics.bytes,
			memory_order_relaxed);
	metrics->peak_queue_depth = atomic_load_explicit(
			&controller->persistence_metrics.peak_queue_depth,
			memory_order_relaxed);
	metrics->enqueue_block_nanoseconds = atomic_load_explicit(
			&controller->persistence_metrics.enqueue_block_nanoseconds,
			memory_order_relaxed);
	metrics->background_jobs = atomic_load_explicit(
			&controller->persistence_metrics.background_jobs,
			memory_order_relaxed);
	metrics->background_bytes = atomic_load_explicit(
			&controller->persistence_metrics.background_bytes,
			memory_order_relaxed);
	metrics->background_peak_backlog = atomic_load_explicit(
			&controller->persistence_metrics.background_peak_backlog,
			memory_order_relaxed);
	for (size_t index = 0; index < controller->thread_count; index++) {
		struct agent_persistence_thread_counters *source =
				&controller->persistence_thread_metrics[index];
		metrics->failures += source->failures;
		metrics->retries += source->retries;
		metrics->queue_wait_nanoseconds += source->queue_wait_nanoseconds;
		metrics->connection_wait_nanoseconds +=
				source->connection_wait_nanoseconds;
		metrics->request_nanoseconds += source->request_nanoseconds;
		metrics->stream_nanoseconds += source->stream_nanoseconds;
		metrics->fsync_nanoseconds += source->fsync_nanoseconds;
		metrics->close_nanoseconds += source->close_nanoseconds;
		metrics->rename_nanoseconds += source->rename_nanoseconds;
		metrics->verify_nanoseconds += source->verify_nanoseconds;
		metrics->commit_wait_nanoseconds += source->commit_wait_nanoseconds;
		metrics->commit_nanoseconds += source->commit_nanoseconds;
		metrics->commit_groups += source->commit_groups;
	}
	return 1;
}

int vine_datavine_data_controller_agent_check(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot)
{
	if (!controller || !workflow_slot)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	int valid = namespace && vine_datavine_replica_table_check(
			namespace->replicas);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

static int output_path(struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t attempt,
		char path[PATH_MAX])
{
	char directory[PATH_MAX];
	if (!workflow_directory_path(controller, workflow_id, directory))
		return 0;
	return snprintf(path, PATH_MAX, "%s/%llu.%u.data", directory, (unsigned long long)data_id, attempt) < PATH_MAX;
}

static int hash_file(const char *path, uint64_t *size, unsigned char digest[32])
{
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	EVP_MD_CTX *context = fd >= 0 ? EVP_MD_CTX_new() : 0;
	int valid = context && EVP_DigestInit_ex(context, EVP_sha256(), 0) == 1;
	uint64_t total = 0;
	char buffer[1024 * 1024];
	while (valid) {
		ssize_t count = read(fd, buffer, sizeof(buffer));
		if (count > 0) {
			valid = EVP_DigestUpdate(context, buffer, (size_t)count) == 1;
			total += (uint64_t)count;
		} else if (!count) {
			break;
		} else {
			valid = 0;
		}
	}
	unsigned int digest_size = 0;
	valid = valid && EVP_DigestFinal_ex(context, digest, &digest_size) == 1 &&
		digest_size == 32;
	if (context)
		EVP_MD_CTX_free(context);
	if (fd >= 0)
		close(fd);
	if (valid)
		*size = total;
	return valid;
}

struct agent_pull_source {
	struct agent_pull_connection *connection;
	uint64_t object_token;
};

struct agent_pull_connection {
	char host[VINE_DATAVINE_AGENT_HOST_MAX];
	uint16_t port;
	struct link *link;
	pthread_mutex_t lock;
};

static int agent_pull_connection_prepare(struct agent_endpoint *endpoint,
		const char *host, uint16_t port)
{
	if (!endpoint || !host || !host[0] || !port)
		return 0;
	struct agent_pull_connection *connection = endpoint->pull;
	if (!connection) {
		connection = calloc(1, sizeof(*connection));
		if (connection && pthread_mutex_init(&connection->lock, 0)) {
			free(connection);
			connection = 0;
		}
		if (!connection)
			return 0;
		endpoint->pull = connection;
	}
	pthread_mutex_lock(&connection->lock);
	if (connection->link && (connection->port != port ||
			strcmp(connection->host, host))) {
		link_close(connection->link);
		connection->link = 0;
	}
	snprintf(connection->host, sizeof(connection->host), "%s", host);
	connection->port = port;
	snprintf(endpoint->host, sizeof(endpoint->host), "%s", host);
	endpoint->port = port;
	pthread_mutex_unlock(&connection->lock);
	return 1;
}

static void agent_pull_connection_delete(struct agent_pull_connection *connection)
{
	if (!connection)
		return;
	if (connection->link)
		link_close(connection->link);
	pthread_mutex_destroy(&connection->lock);
	free(connection);
}

static int pull_agent_file(struct vine_datavine_data_controller *controller,
		const struct publication_output *output,
		const struct agent_pull_source *source,
		struct agent_persistence_thread_counters *metrics)
{
	char cache[128];
	if (snprintf(cache, sizeof(cache),
			"datavine-%016llx-%016llx-%08x-%016llx",
			(unsigned long long)output->workflow_slot,
			(unsigned long long)output->info.data_id, output->info.attempt,
			(unsigned long long)source->object_token) >= (int)sizeof(cache))
		return 0;
	struct agent_pull_connection *cached = source->connection;
	char line[4096] = {0};
	if (!cached)
		return 0;
	uint64_t started = controller->persistence_metrics_enabled
			? monotonic_nanoseconds() : 0;
	pthread_mutex_lock(&cached->lock);
	if (started)
		metrics->connection_wait_nanoseconds +=
				monotonic_nanoseconds() - started;
	for (int attempt = 0; cached && attempt < 2; attempt++) {
		if (attempt && controller->persistence_metrics_enabled)
			metrics->retries++;
		started = controller->persistence_metrics_enabled
				? monotonic_nanoseconds() : 0;
		time_t deadline = time(0) + 30;
		if (!cached->link) {
			char address[LINK_ADDRESS_MAX];
			if (domain_name_cache_lookup(cached->host, address)) {
				cached->link = link_connect(address, cached->port, deadline);
				if (cached->link)
					link_tune(cached->link, LINK_TUNE_INTERACTIVE);
			}
		}
		char encoded[256];
		long long length = -1;
		unsigned int mode = 0;
		int mtime = 0;
		int valid = cached->link &&
				link_printf(cached->link, deadline, "get %s\n", cache) >= 0 &&
				link_readline(cached->link, line, sizeof(line), deadline) &&
				sscanf(line, "file %255s %lld %o %d", encoded, &length, &mode,
						&mtime) == 4 && length >= 0 &&
				(uint64_t)length == output->info.size;
		char temporary[PATH_MAX];
		int fd = valid && snprintf(temporary, sizeof(temporary),
				"%s.part.%ld.%lu", output->path, (long)getpid(),
				(unsigned long)pthread_self()) < (int)sizeof(temporary)
				? open(temporary, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600)
				: -1;
		if (started)
			metrics->request_nanoseconds += monotonic_nanoseconds() - started;
		started = controller->persistence_metrics_enabled
				? monotonic_nanoseconds() : 0;
		int streamed = valid && fd >= 0 &&
				link_stream_to_fd(cached->link, fd, length, deadline) == length;
		if (started)
			metrics->stream_nanoseconds += monotonic_nanoseconds() - started;
		if (!streamed) {
			if (fd >= 0) {
				close(fd);
				unlink(temporary);
			}
			if (cached->link) {
				link_close(cached->link);
				cached->link = 0;
			}
			continue;
		}

		/* The complete response has been consumed, so another data thread may
		 * safely use this Worker connection while this file becomes durable. */
		pthread_mutex_unlock(&cached->lock);
		started = controller->persistence_metrics_enabled
				? monotonic_nanoseconds() : 0;
		valid = fsync(fd) == 0;
		if (started)
			metrics->fsync_nanoseconds += monotonic_nanoseconds() - started;
		started = controller->persistence_metrics_enabled
				? monotonic_nanoseconds() : 0;
		if (close(fd))
			valid = 0;
		if (started)
			metrics->close_nanoseconds += monotonic_nanoseconds() - started;
		if (valid) {
			started = controller->persistence_metrics_enabled
					? monotonic_nanoseconds() : 0;
			if (rename(temporary, output->path))
				valid = 0;
			if (started)
				metrics->rename_nanoseconds +=
						monotonic_nanoseconds() - started;
		}
		if (!valid)
			unlink(temporary);
		if (valid)
			return 1;

		/* A local persistence failure does not corrupt the framed Worker
		 * connection.  Keep it for the retry. */
		started = controller->persistence_metrics_enabled
				? monotonic_nanoseconds() : 0;
		pthread_mutex_lock(&cached->lock);
		if (started)
			metrics->connection_wait_nanoseconds +=
					monotonic_nanoseconds() - started;
	}
	if (getenv("DATAVINE_WORKFLOW_METRICS"))
		fprintf(stderr,
				"datavine controller pull_failed host=%s port=%u data=%llu size=%llu line=%s errno=%d\n",
				cached->host, cached->port,
				(unsigned long long)output->info.data_id,
				(unsigned long long)output->info.size, line, errno);
	pthread_mutex_unlock(&cached->lock);
	return 0;
}

static int pull_agent_output(struct vine_datavine_data_controller *controller,
		struct publication_output *output,
		struct agent_persistence_thread_counters *metrics)
{
	struct agent_pull_source sources[16];
	struct vine_datavine_replica_view replicas[16];
	size_t count = 0;
	uint64_t size = 0;
	unsigned char digest[32];
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, output->workflow_slot);
	enum vine_datavine_resolve_status status = namespace
			? vine_datavine_replica_table_resolve(namespace->replicas,
					output->info.data_id, output->info.attempt, replicas, 16,
					&count, &size, digest, 0)
			: VINE_DATAVINE_RESOLVE_UNKNOWN;
	int valid = status == VINE_DATAVINE_RESOLVE_AVAILABLE && count &&
			size == output->info.size;
	if (valid && output->background) {
		valid = 0;
		for (size_t index = 0; index < count; index++) {
			uint32_t worker = replicas[index].worker_slot;
			if (worker == output->background_worker_slot &&
					worker < namespace->endpoint_capacity &&
					namespace->endpoints[worker].port &&
					namespace->endpoints[worker].pull) {
				sources[0].connection = namespace->endpoints[worker].pull;
				sources[0].object_token = replicas[index].object_token;
				count = 1;
				valid = 1;
				break;
			}
		}
	} else {
		for (size_t index = 0; valid && index < count; index++) {
			uint32_t worker = replicas[index].worker_slot;
			valid = worker < namespace->endpoint_capacity &&
					namespace->endpoints[worker].port &&
					namespace->endpoints[worker].pull;
			if (valid) {
				sources[index].connection = namespace->endpoints[worker].pull;
				sources[index].object_token = replicas[index].object_token;
			}
		}
	}
	pthread_mutex_unlock(&controller->lock);
	for (size_t index = 0; valid && index < count; index++)
		if (pull_agent_file(controller, output, &sources[index], metrics))
			return 1;
	return 0;
}

static void digest_hex(const unsigned char digest[32], char encoded[65])
{
	for (size_t index = 0; index < 32; index++)
		snprintf(encoded + index * 2, 3, "%02x", digest[index]);
}

static struct publication_job *agent_persistence_job_create_locked(
		struct vine_datavine_data_controller *controller,
		struct agent_namespace *namespace,
		const struct vine_datavine_publish_record *record)
{
	struct catalog_record *slot = catalog_lookup(controller, record->data_id);
	struct agent_result_metadata *metadata = catalog_metadata(slot);
	if (!metadata || metadata->info.attempt != record->generation ||
			metadata->persistence_queued ||
			result_lookup(controller, namespace->workflow_id, record->data_id))
		return 0;
	struct publication_job *job = calloc(1, sizeof(*job));
	struct publication_output *output = job ? &job->output : 0;
	char path[PATH_MAX];
	int valid = job && output &&
			output_path(controller, namespace->workflow_id, record->data_id,
				record->generation, path);
	if (valid) {
		job->workflow_id = strdup(namespace->workflow_id);
		valid = job->workflow_id != 0;
	}
	if (valid) {
		output->info = metadata->info;
		output->info.size = record->size;
		digest_hex(record->digest, output->info.sha256);
		output->path = strdup(path);
		output->workflow_slot = namespace->workflow_slot;
		valid = output->path != 0;
	}
	if (!valid) {
		free(output ? output->path : 0);
		free(job ? job->workflow_id : 0);
		free(job);
		return 0;
	}
	metadata->persistence_queued = 1;
	return job;
}

static struct publication_job *background_job_create(
		struct vine_datavine_data_controller *controller, uint64_t data_id,
		int *retry)
{
	*retry = 0;
	struct publication_job *job = 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = controller->workflow;
	struct vine_datavine_replica_view replicas[16];
	size_t count = 0;
	uint64_t size = 0;
	unsigned char digest[32];
	int persisted = 0;
	struct catalog_record *record = namespace
			? catalog_lookup(controller, data_id) : 0;
	enum vine_datavine_resolve_status status = namespace &&
			!catalog_metadata(record)
			? vine_datavine_replica_table_resolve(namespace->replicas, data_id, 0,
					replicas, 16, &count, &size, digest, &persisted)
			: VINE_DATAVINE_RESOLVE_UNKNOWN;
	if (status == VINE_DATAVINE_RESOLVE_AVAILABLE && count && !persisted) {
		struct vine_datavine_replica_view *replica = 0;
		for (size_t index = 0; index < count; index++) {
			uint32_t worker = replicas[index].worker_slot;
			if (worker < namespace->endpoint_capacity &&
					namespace->endpoints[worker].port &&
					namespace->endpoints[worker].pull &&
					!namespace->endpoints[worker].background_pull_active) {
				replica = &replicas[index];
				break;
			}
		}
		if (!replica) {
			*retry = 1;
			pthread_mutex_unlock(&controller->lock);
			return 0;
		}
		job = calloc(1, sizeof(*job));
		char path[PATH_MAX];
		if (job && output_path(controller, namespace->workflow_id, data_id,
				replica->generation, path)) {
			job->workflow_id = strdup(namespace->workflow_id);
			job->output.path = strdup(path);
			job->output.workflow_slot = namespace->workflow_slot;
			job->output.info.data_id = data_id;
			job->output.info.attempt = replica->generation;
			job->output.info.size = size;
			job->output.background_worker_slot = replica->worker_slot;
			job->background = 1;
			job->output.background = 1;
			digest_hex(digest, job->output.info.sha256);
		}
		if (!job || !job->workflow_id || !job->output.path) {
			job_delete(job);
			job = 0;
			*retry = 1;
		} else {
			namespace->endpoints[replica->worker_slot].background_pull_active = 1;
		}
	} else if (status == VINE_DATAVINE_RESOLVE_PENDING && !persisted) {
		*retry = 1;
	} else if (namespace) {
		vine_datavine_replica_table_clear_backup(namespace->replicas,
				data_id, 0);
	}
	pthread_mutex_unlock(&controller->lock);
	return job;
}

static void background_job_release_worker(
		struct vine_datavine_data_controller *controller,
		const struct publication_job *job)
{
	if (!job || !job->background)
		return;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, job->output.workflow_slot);
	uint32_t worker = job->output.background_worker_slot;
	if (namespace && worker < namespace->endpoint_capacity)
		namespace->endpoints[worker].background_pull_active = 0;
	pthread_mutex_unlock(&controller->lock);
}

static int background_job_install(
		struct vine_datavine_data_controller *controller,
		struct publication_job *job)
{
	struct publication_output *output = &job->output;
	int notify = 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, output->workflow_slot);
	struct catalog_record *record = namespace
			? catalog_lookup(controller, output->info.data_id) : 0;
	struct data_result *stored = record
			? result_lookup(controller, namespace->workflow_id,
					output->info.data_id) : 0;
	enum vine_datavine_resolve_status status = namespace
			? vine_datavine_replica_table_resolve(namespace->replicas,
					output->info.data_id, output->info.attempt,
					0, 0, 0, 0, 0, 0)
			: VINE_DATAVINE_RESOLVE_UNKNOWN;
	int valid = namespace && record &&
			status != VINE_DATAVINE_RESOLVE_DEAD &&
			status != VINE_DATAVINE_RESOLVE_UNKNOWN;
	if (valid && !stored) {
		stored = calloc(1, sizeof(*stored));
		valid = stored != 0;
		if (valid) {
			stored->info = output->info;
			stored->path = strdup(output->path);
			valid = stored->path != 0;
		}
		struct agent_result_metadata *metadata = catalog_metadata(record);
		if (valid && metadata && metadata->info.attempt == output->info.attempt) {
			stored->info.producer_task_id = metadata->info.producer_task_id;
			stored->info.producer_output_index =
					metadata->info.producer_output_index;
			stored->info.requested = 1;
			snprintf(stored->info.codec_name, sizeof(stored->info.codec_name),
					"%s", metadata->info.codec_name);
			snprintf(stored->info.codec_version,
					sizeof(stored->info.codec_version), "%s",
					metadata->info.codec_version);
			valid = requested_result_reserve(controller, 1) &&
					requested_result_append(controller, &stored->info);
			notify = valid;
		}
		if (valid) {
			if (!result_insert(controller, namespace->workflow_id,
					output->info.data_id, stored))
				abort();
			stored = 0;
		}
		data_result_delete(stored);
	}
	if (valid)
		valid = vine_datavine_replica_table_set_persisted(namespace->replicas,
				output->info.data_id, output->info.attempt);
	pthread_mutex_unlock(&controller->lock);
	if (!valid)
		unlink(output->path);
	if (notify && controller->result_notify)
		controller->result_notify(controller->result_notify_context);
	return valid;
}

static int publication_job_enqueue(struct vine_datavine_data_controller *controller,
		struct publication_job *job)
{
	if (!controller || !job || !controller->thread_count)
		return 0;
	uint64_t started = controller->persistence_metrics_enabled
			? monotonic_nanoseconds() : 0;
	pthread_mutex_lock(&controller->queue_lock);
	while (!controller->stopping &&
			controller->queued_count >= DATA_QUEUE_LIMIT)
		pthread_cond_wait(&controller->space, &controller->queue_lock);
	if (controller->stopping) {
		pthread_mutex_unlock(&controller->queue_lock);
		return 0;
	}
	if (started) {
		uint64_t now = monotonic_nanoseconds();
		persistence_counter_add(
				&controller->persistence_metrics.enqueue_block_nanoseconds,
				now - started);
		job->enqueued_at_nanoseconds = now;
	}
	if (controller->tail)
		controller->tail->next = job;
	else
		controller->head = job;
	controller->tail = job;
	controller->queued_count++;
	if (controller->persistence_metrics_enabled) {
		persistence_counter_add(&controller->persistence_metrics.jobs, 1);
		persistence_counter_add(&controller->persistence_metrics.bytes,
				job->output.info.size);
		persistence_counter_max(&controller->persistence_metrics.peak_queue_depth,
				controller->queued_count);
	}
	pthread_cond_signal(&controller->queued);
	pthread_mutex_unlock(&controller->queue_lock);
	return 1;
}

static int digest_bytes(const char encoded[65], unsigned char digest[32])
{
	if (strlen(encoded) != 64)
		return 0;
	for (size_t index = 0; index < 32; index++) {
		unsigned int value = 0;
		if (sscanf(encoded + index * 2, "%2x", &value) != 1)
			return 0;
		digest[index] = (unsigned char)value;
	}
	return 1;
}

static size_t encoded_size(const char *workflow_id,
		struct publication_output *outputs, size_t count)
{
	size_t size = 4 + strlen(workflow_id);
	for (size_t index = 0; index < count; index++)
		size += 72 + strlen(outputs[index].info.codec_name) +
			strlen(outputs[index].info.codec_version);
	return size;
}

static unsigned char *encode_publication(const char *workflow_id,
		struct publication_output *outputs, size_t count, size_t *payload_size)
{
	size_t workflow_size = strlen(workflow_id);
	if (!workflow_size || workflow_size > UINT16_MAX || !count ||
			count > UINT16_MAX)
		return 0;
	*payload_size = encoded_size(workflow_id, outputs, count);
	if (*payload_size > UINT32_MAX)
		return 0;
	unsigned char *payload = calloc(1, *payload_size);
	if (!payload)
		return 0;
	vine_datavine_put_u16(payload, (uint16_t)workflow_size);
	vine_datavine_put_u16(payload + 2, (uint16_t)count);
	memcpy(payload + 4, workflow_id, workflow_size);
	size_t offset = 4 + workflow_size;
	for (size_t index = 0; index < count; index++) {
		struct vine_datavine_workflow_result_info *info = &outputs[index].info;
		size_t codec_name_size = strlen(info->codec_name);
		size_t codec_version_size = strlen(info->codec_version);
		unsigned char digest[32];
		if (codec_name_size > UINT16_MAX || codec_version_size > UINT16_MAX ||
				!digest_bytes(info->sha256, digest)) {
			free(payload);
			return 0;
		}
		vine_datavine_put_u64(payload + offset, info->data_id);
		vine_datavine_put_u32(payload + offset + 8, info->attempt);
		vine_datavine_put_u64(payload + offset + 12, info->size);
		vine_datavine_put_u64(payload + offset + 20,
				(uint64_t)info->producer_task_id);
		vine_datavine_put_u32(payload + offset + 28,
				(uint32_t)info->producer_output_index);
		vine_datavine_put_u32(payload + offset + 32,
				(uint32_t)info->requested);
		memcpy(payload + offset + 36, digest, 32);
		vine_datavine_put_u16(payload + offset + 68,
				(uint16_t)codec_name_size);
		vine_datavine_put_u16(payload + offset + 70,
				(uint16_t)codec_version_size);
		offset += 72;
		memcpy(payload + offset, info->codec_name, codec_name_size);
		offset += codec_name_size;
		memcpy(payload + offset, info->codec_version, codec_version_size);
		offset += codec_version_size;
	}
	return payload;
}

static void installation_delete(struct result_installation *installation)
{
	if (!installation)
		return;
	for (size_t index = 0; index < installation->count; index++)
		data_result_delete(installation->pending ? installation->pending[index] : 0);
	free(installation->pending);
	free(installation->data_ids);
	memset(installation, 0, sizeof(*installation));
}

static int installation_prepare(struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct publication_output *outputs,
		size_t count, struct result_installation *installation)
{
	memset(installation, 0, sizeof(*installation));
	installation->pending = calloc(count, sizeof(*installation->pending));
	installation->data_ids = calloc(count, sizeof(*installation->data_ids));
	installation->workflow_id = workflow_id;
	installation->count = count;
	int valid = installation->pending && installation->data_ids;
	for (size_t index = 0; valid && index < count; index++) {
		installation->data_ids[index] = outputs[index].info.data_id;
		struct data_result *known = result_lookup(controller, workflow_id, installation->data_ids[index]);
		if (known) {
			valid = known->info.attempt <= outputs[index].info.attempt;
			if (!valid)
				break;
			if (known->info.attempt == outputs[index].info.attempt) {
				valid = !strcmp(known->info.sha256,
						outputs[index].info.sha256);
				continue;
			}
		}
		installation->pending[index] = calloc(
				1, sizeof(*installation->pending[index]));
		if (installation->pending[index]) {
			installation->pending[index]->info = outputs[index].info;
			installation->pending[index]->path = strdup(outputs[index].path);
		}
		valid = installation->pending[index] &&
			installation->pending[index]->path;
	}
	if (!valid)
		installation_delete(installation);
	return valid;
}

static int requested_result_reserve(
		struct vine_datavine_data_controller *controller, size_t additional)
{
	if (!additional)
		return 1;
	if (additional > SIZE_MAX - controller->requested_result_count)
		return 0;
	size_t required = controller->requested_result_count + additional;
	if (required > SIZE_MAX / sizeof(*controller->requested_results))
		return 0;
	if (required > controller->requested_result_capacity) {
		size_t capacity = controller->requested_result_capacity
				? controller->requested_result_capacity : 1024;
		while (capacity < required) {
			if (capacity > SIZE_MAX / 2 /
					sizeof(*controller->requested_results)) {
				capacity = required;
				break;
			}
			capacity *= 2;
		}
		struct requested_result_record *records = realloc(
				controller->requested_results, capacity * sizeof(*records));
		if (!records)
			return 0;
		controller->requested_results = records;
		controller->requested_result_capacity = capacity;
	}
	return 1;
}

static int requested_result_append(struct vine_datavine_data_controller *controller,
		const struct vine_datavine_workflow_result_info *info)
{
	if (!info->requested)
		return 1;
	if (controller->requested_result_count ==
			controller->requested_result_capacity)
		return 0;
	struct requested_result_record *record =
			&controller->requested_results[controller->requested_result_count++];
	record->data_id = info->data_id;
	record->attempt = info->attempt;
	return 1;
}

static int installation_apply(struct vine_datavine_data_controller *controller,
		struct result_installation *installation)
{
	for (size_t index = 0; index < installation->count; index++) {
		struct data_result *known = result_lookup(controller, installation->workflow_id, installation->data_ids[index]);
		if (known && installation->pending[index] &&
				known->info.attempt < installation->pending[index]->info.attempt) {
			known = result_remove(controller, installation->workflow_id, installation->data_ids[index]);
			if (known->path)
				unlink(known->path);
			data_result_delete(known);
		}
		if (!result_lookup(controller, installation->workflow_id, installation->data_ids[index])) {
			if (installation->pending[index] &&
					!requested_result_append(controller,
						&installation->pending[index]->info))
				return 0;
			if (!result_insert(controller, installation->workflow_id, installation->data_ids[index], installation->pending[index]))
				abort();
			installation->pending[index] = 0;
		}
	}
	return 1;
}

static int install_results(struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct publication_output *outputs,
		size_t count, int replay)
{
	struct result_installation installation;
	int valid = installation_prepare(controller, workflow_id, outputs, count, &installation);
	size_t requested_count = 0;
	for (size_t index = 0; valid && index < installation.count; index++)
		requested_count += installation.pending[index] &&
				installation.pending[index]->info.requested;
	valid = valid && requested_result_reserve(controller, requested_count);
	if (valid && !replay) {
		size_t payload_size = 0;
		unsigned char *payload = encode_publication(
				workflow_id, outputs, count, &payload_size);
		valid = payload && vine_datavine_journal_commit(controller->journal,
						   DATA_READY_BATCH,
						   payload,
						   payload_size);
		free(payload);
	}
	if (valid)
		valid = installation_apply(controller, &installation);
	installation_delete(&installation);
	if (valid && !replay && controller->result_notify)
		controller->result_notify(controller->result_notify_context);
	return valid;
}

static int replay_record(void *context, uint16_t opcode,
		const unsigned char *payload, size_t payload_size)
{
	struct vine_datavine_data_controller *controller = context;
	if (opcode >= 100)
		return 1;
	if (payload_size < 4)
		return 0;
	uint16_t workflow_size = vine_datavine_get_u16(payload);
	uint16_t count = vine_datavine_get_u16(payload + 2);
	if (!workflow_size || !count || 4U + workflow_size > payload_size)
		return 0;
	if (opcode == DATA_RELEASE_BATCH) {
		if (payload_size != 4U + workflow_size + (size_t)count * 8U)
			return 0;
		char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
		if (workflow_size > VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX)
			return 0;
		memcpy(workflow_id, payload + 4, workflow_size);
		workflow_id[workflow_size] = 0;
		if (!workflow_bind(controller, workflow_id))
			return 0;
		for (uint16_t index = 0; index < count; index++) {
			uint64_t data_id = vine_datavine_get_u64(
					payload + 4 + workflow_size + (size_t)index * 8);
			struct data_result *removed = result_remove(
					controller, workflow_id, data_id);
			if (removed)
				unlink(removed->path);
			data_result_delete(removed);
		}
		return 1;
	}
	if (opcode != DATA_READY_BATCH)
		return 0;
	char *workflow_id = malloc((size_t)workflow_size + 1);
	struct publication_output *outputs = calloc(count, sizeof(*outputs));
	if (!workflow_id || !outputs) {
		free(workflow_id);
		free(outputs);
		return 0;
	}
	memcpy(workflow_id, payload + 4, workflow_size);
	workflow_id[workflow_size] = 0;
	if (!workflow_bind(controller, workflow_id)) {
		free(workflow_id);
		free(outputs);
		return 0;
	}
	size_t offset = 4 + workflow_size;
	int valid = 1;
	for (uint16_t index = 0; valid && index < count; index++) {
		if (offset > payload_size || payload_size - offset < 72) {
			valid = 0;
			break;
		}
		struct vine_datavine_workflow_result_info *info = &outputs[index].info;
		info->data_id = vine_datavine_get_u64(payload + offset);
		info->attempt = vine_datavine_get_u32(payload + offset + 8);
		info->size = vine_datavine_get_u64(payload + offset + 12);
		info->producer_task_id =
				(int64_t)vine_datavine_get_u64(payload + offset + 20);
		info->producer_output_index =
				(int32_t)vine_datavine_get_u32(payload + offset + 28);
		info->requested = (int)vine_datavine_get_u32(payload + offset + 32);
		digest_hex(payload + offset + 36, info->sha256);
		uint16_t name_size = vine_datavine_get_u16(payload + offset + 68);
		uint16_t version_size = vine_datavine_get_u16(payload + offset + 70);
		offset += 72;
		if (!name_size || !version_size || name_size >= sizeof(info->codec_name) ||
				version_size >= sizeof(info->codec_version) ||
				name_size + version_size > payload_size - offset) {
			valid = 0;
			break;
		}
		memcpy(info->codec_name, payload + offset, name_size);
		info->codec_name[name_size] = 0;
		offset += name_size;
		memcpy(info->codec_version, payload + offset, version_size);
		info->codec_version[version_size] = 0;
		offset += version_size;
		char path[PATH_MAX];
		outputs[index].path = output_path(controller, workflow_id, info->data_id, info->attempt, path)
							  ? strdup(path)
							  : 0;
		valid = outputs[index].path != 0;
	}
	valid = valid && offset == payload_size &&
		install_results(controller, workflow_id, outputs, count, 1);
	for (uint16_t index = 0; index < count; index++)
		free(outputs[index].path);
	free(outputs);
	free(workflow_id);
	return valid;
}

static int validate_catalog(struct vine_datavine_data_controller *controller)
{
	for (size_t chunk = 0; chunk < controller->catalog_capacity; chunk++) {
		if (!controller->catalog[chunk])
			continue;
		for (size_t index = 0; index < CATALOG_CHUNK_SIZE; index++) {
			struct data_result *result = catalog_result(
					&controller->catalog[chunk]->records[index]);
			if (!result)
				continue;
			uint64_t size = 0;
			unsigned char digest[32];
			char encoded[65];
			if (!hash_file(result->path, &size, digest))
				return 0;
			digest_hex(digest, encoded);
			if (size != result->info.size ||
					strcmp(encoded, result->info.sha256))
				return 0;
		}
	}
	return 1;
}

static int prepare_job(struct vine_datavine_data_controller *controller,
		struct publication_job *job,
		struct agent_persistence_thread_counters *metrics)
{
	struct publication_output *output = &job->output;
	int valid = pull_agent_output(controller, output, metrics);
	if (valid) {
		uint64_t started = controller->persistence_metrics_enabled
				? monotonic_nanoseconds() : 0;
		uint64_t size = 0;
		unsigned char digest[32];
		unsigned char expected[32];
		valid = digest_bytes(output->info.sha256, expected) &&
			hash_file(output->path, &size, digest) &&
			size == output->info.size && !memcmp(digest, expected, 32);
		if (started)
			metrics->verify_nanoseconds += monotonic_nanoseconds() - started;
	}
	return valid;
}

static void agent_persistence_finish(
		struct vine_datavine_data_controller *controller,
		struct publication_job *job, int successful)
{
	struct publication_output *output = &job->output;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, output->workflow_slot);
	struct catalog_record *record = namespace
			? catalog_lookup(controller, output->info.data_id) : 0;
	struct agent_result_metadata *metadata = catalog_metadata(record);
	if (successful && namespace) {
		vine_datavine_replica_table_set_persisted(namespace->replicas,
				output->info.data_id, output->info.attempt);
		if (metadata && metadata->info.attempt == output->info.attempt) {
			free(metadata);
			catalog_set_metadata(record, 0);
		}
	} else if (metadata && metadata->info.attempt == output->info.attempt) {
		metadata->persistence_queued = 0;
	}
	pthread_mutex_unlock(&controller->lock);
}

static void commit_job_group(struct vine_datavine_data_controller *controller,
		struct publication_job *jobs,
		struct agent_persistence_thread_counters *metrics)
{
	uint64_t commit_started = controller->persistence_metrics_enabled
			? monotonic_nanoseconds() : 0;
	size_t count = 0;
	for (struct publication_job *job = jobs; job; job = job->next) {
		count++;
		if (commit_started && job->prepared_at_nanoseconds)
			metrics->commit_wait_nanoseconds +=
					commit_started - job->prepared_at_nanoseconds;
	}
	struct publication_job **accepted = calloc(count, sizeof(*accepted));
	struct result_installation *installations = calloc(count, sizeof(*installations));
	unsigned char **payloads = calloc(count, sizeof(*payloads));
	size_t *payload_sizes = calloc(count, sizeof(*payload_sizes));
	int *success = calloc(count, sizeof(*success));
	int allocated = accepted && installations && payloads && payload_sizes && success;
	size_t accepted_count = 0;

	pthread_mutex_lock(&controller->lock);
	for (struct publication_job *job = jobs; allocated && job; job = job->next) {
		struct result_installation *installation =
				&installations[accepted_count];
		if (!installation_prepare(controller, job->workflow_id,
				&job->output, 1, installation))
			continue;
		payloads[accepted_count] = encode_publication(job->workflow_id,
				&job->output,
				1,
				&payload_sizes[accepted_count]);
		if (!payloads[accepted_count]) {
			installation_delete(installation);
			continue;
		}
		accepted[accepted_count++] = job;
	}
	int committed = allocated && accepted_count;
	size_t requested_count = 0;
	for (size_t index = 0; index < accepted_count; index++)
		requested_count += installations[index].pending[0] &&
				installations[index].pending[0]->info.requested;
	committed = committed && requested_result_reserve(controller,
			requested_count);
	for (size_t index = 0; committed && index < accepted_count; index++)
		committed = vine_datavine_journal_enqueue(controller->journal,
				DATA_READY_BATCH,
				payloads[index],
				payload_sizes[index]);
	if (committed) {
		for (size_t index = 0; index < accepted_count; index++) {
			if (installation_apply(controller, &installations[index]))
				success[index] = 1;
			else
				committed = 0;
		}
	}
	pthread_mutex_unlock(&controller->lock);
	if (committed && controller->result_notify)
		controller->result_notify(controller->result_notify_context);

	for (size_t index = 0; index < accepted_count; index++) {
		installation_delete(&installations[index]);
		free(payloads[index]);
	}
	for (struct publication_job *job = jobs; job; job = job->next) {
		int job_success = 0;
		for (size_t index = 0; index < accepted_count; index++) {
			if (accepted[index] == job) {
				job_success = success[index];
				break;
			}
		}
		agent_persistence_finish(controller, job, job_success);
	}
	free(success);
	free(payload_sizes);
	free(payloads);
	free(installations);
	free(accepted);
	if (commit_started) {
		metrics->commit_nanoseconds += monotonic_nanoseconds() - commit_started;
		metrics->commit_groups++;
	}
}

static void job_delete(struct publication_job *job)
{
	if (!job)
		return;
	free(job->output.path);
	free(job->workflow_id);
	free(job);
}

static void *data_worker(void *argument)
{
	struct data_worker_context *context = argument;
	struct vine_datavine_data_controller *controller = context->controller;
	struct agent_persistence_thread_counters *metrics = context->metrics;
	for (;;) {
		pthread_mutex_lock(&controller->queue_lock);
		while (!controller->stopping && !controller->head &&
				(!atomic_load_explicit(&controller->background_backup,
						memory_order_acquire) ||
				 !controller->backup_count ||
				 controller->background_active >= controller->background_limit))
			pthread_cond_wait(&controller->queued, &controller->queue_lock);
		if (controller->stopping && !controller->head) {
			pthread_mutex_unlock(&controller->queue_lock);
			break;
		}
		if (!controller->head &&
				monotonic_nanoseconds() < controller->foreground_until_nanoseconds) {
			pthread_mutex_unlock(&controller->queue_lock);
			struct timespec delay = {.tv_nsec = 10000000L};
			nanosleep(&delay, 0);
			continue;
		}
		struct publication_job *job = 0;
		uint64_t background_data_id = 0;
		if (controller->head) {
			job = controller->head;
			controller->head = job->next;
			if (controller->tail == job)
				controller->tail = 0;
			controller->queued_count--;
		} else {
			background_data_id =
					controller->backup_queue[controller->backup_head];
			controller->backup_head = (controller->backup_head + 1) %
					controller->backup_capacity;
			controller->backup_count--;
			controller->background_active++;
		}
		if (controller->persistence_metrics_enabled &&
				job && job->enqueued_at_nanoseconds)
			metrics->queue_wait_nanoseconds +=
					monotonic_nanoseconds() - job->enqueued_at_nanoseconds;
		pthread_cond_broadcast(&controller->space);
		pthread_mutex_unlock(&controller->queue_lock);
		if (!job) {
			int retry = 0;
			job = background_job_create(controller, background_data_id, &retry);
			if (!job) {
				pthread_mutex_lock(&controller->queue_lock);
				if (controller->background_active)
					controller->background_active--;
				if (retry)
					backup_queue_append_locked(controller, background_data_id);
				pthread_cond_broadcast(&controller->queued);
				pthread_mutex_unlock(&controller->queue_lock);
				if (retry) {
					struct timespec delay = {.tv_nsec = 10000000L};
					nanosleep(&delay, 0);
				}
				continue;
			}
		}
		int prepared = 0;
		unsigned int attempts = job->background ? 1 : 8;
		for (unsigned int attempt = 0; !prepared && attempt < attempts; attempt++) {
			prepared = prepare_job(controller, job, metrics);
			if (!prepared && attempt + 1 < attempts) {
				struct timespec delay = {
					.tv_nsec = (long)(10000000U << (attempt < 4 ? attempt : 4)),
				};
				nanosleep(&delay, 0);
			}
		}
		if (!prepared) {
			if (controller->persistence_metrics_enabled)
				metrics->failures++;
			if (job->background) {
				background_job_release_worker(controller, job);
				pthread_mutex_lock(&controller->queue_lock);
				if (controller->background_active)
					controller->background_active--;
				backup_queue_append_locked(controller,
						job->output.info.data_id);
				pthread_cond_broadcast(&controller->queued);
				pthread_mutex_unlock(&controller->queue_lock);
				struct timespec delay = {.tv_nsec = 100000000L};
				nanosleep(&delay, 0);
			} else {
				agent_persistence_finish(controller, job, 0);
			}
			job_delete(job);
			continue;
		}
		if (job->background) {
			int installed = background_job_install(controller, job);
			if (installed) {
				persistence_counter_add(
						&controller->persistence_metrics.background_jobs, 1);
				persistence_counter_add(
						&controller->persistence_metrics.background_bytes,
						job->output.info.size);
			} else if (controller->persistence_metrics_enabled) {
				metrics->failures++;
			}
			background_job_release_worker(controller, job);
			pthread_mutex_lock(&controller->queue_lock);
			if (controller->background_active)
				controller->background_active--;
			if (!installed)
				backup_queue_append_locked(controller,
						job->output.info.data_id);
			pthread_cond_broadcast(&controller->queued);
			pthread_mutex_unlock(&controller->queue_lock);
			job_delete(job);
			continue;
		}

		pthread_mutex_lock(&controller->queue_lock);
		if (controller->persistence_metrics_enabled)
			job->prepared_at_nanoseconds = monotonic_nanoseconds();
		job->next = 0;
		if (controller->prepared_tail)
			controller->prepared_tail->next = job;
		else
			controller->prepared_head = job;
		controller->prepared_tail = job;
		pthread_cond_signal(&controller->prepared);
		if (controller->commit_active) {
			pthread_mutex_unlock(&controller->queue_lock);
			continue;
		}
		controller->commit_active = 1;
		for (;;) {
			if (!controller->stopping) {
				struct timespec deadline;
				clock_gettime(CLOCK_REALTIME, &deadline);
				deadline.tv_nsec += DATA_COMMIT_COALESCE_NS;
				if (deadline.tv_nsec >= INT64_C(1000000000)) {
					deadline.tv_sec++;
					deadline.tv_nsec -= INT64_C(1000000000);
				}
				while (!controller->stopping &&
						pthread_cond_timedwait(&controller->prepared,
								&controller->queue_lock,
								&deadline) != ETIMEDOUT) {
				}
			}
			struct publication_job *batch = controller->prepared_head;
			controller->prepared_head = 0;
			controller->prepared_tail = 0;
			pthread_mutex_unlock(&controller->queue_lock);
			commit_job_group(controller, batch, metrics);
			while (batch) {
				struct publication_job *next = batch->next;
				job_delete(batch);
				batch = next;
			}
			pthread_mutex_lock(&controller->queue_lock);
			if (!controller->prepared_head) {
				controller->commit_active = 0;
				pthread_mutex_unlock(&controller->queue_lock);
				break;
			}
		}
	}
	return 0;
}

struct vine_datavine_data_controller *vine_datavine_data_controller_open(
		const char *workflow_journal_path, size_t threads,
		struct vine_datavine_journal *journal)
{
	if (!workflow_journal_path || !workflow_journal_path[0] || !journal)
		return 0;
	struct vine_datavine_data_controller *controller =
			calloc(1, sizeof(*controller));
	if (!controller)
		return 0;
	atomic_init(&controller->loss_events, 0);
	atomic_init(&controller->background_backup, 0);
	persistence_counters_initialize(&controller->persistence_metrics);
	const char *persistence_diagnostics =
			getenv("DATAVINE_PERSISTENCE_DIAGNOSTICS");
	controller->persistence_metrics_enabled = persistence_diagnostics &&
			strcmp(persistence_diagnostics, "0");
	size_t root_size = strlen(workflow_journal_path) + 6;
	controller->root = malloc(root_size);
	if (controller->root)
		snprintf(controller->root, root_size, "%s.data", workflow_journal_path);
	int valid = controller->root && create_dir(controller->root, 0700);
	if (!pthread_mutex_init(&controller->lock, 0))
		controller->lock_initialized = 1;
	if (controller->lock_initialized &&
			!pthread_mutex_init(&controller->queue_lock, 0))
		controller->queue_lock_initialized = 1;
	if (controller->queue_lock_initialized &&
			!pthread_cond_init(&controller->queued, 0))
		controller->queued_initialized = 1;
	if (controller->queued_initialized &&
			!pthread_cond_init(&controller->space, 0))
		controller->space_initialized = 1;
	if (controller->space_initialized &&
			!pthread_cond_init(&controller->prepared, 0))
		controller->prepared_initialized = 1;
	valid = valid && controller->prepared_initialized;
	controller->journal = journal;
	controller->background_limit = threads / DATA_BACKGROUND_DIVISOR;
	if (!controller->background_limit && threads)
		controller->background_limit = 1;
	controller->object_store = valid
						   ? vine_datavine_object_store_open(workflow_journal_path)
						   : 0;
	controller->threads = threads ? calloc(threads, sizeof(*controller->threads)) : 0;
	if (threads && threads <=
			SIZE_MAX / sizeof(*controller->persistence_thread_metrics)) {
		void *metrics = 0;
		if (!posix_memalign(&metrics, 64,
				threads * sizeof(*controller->persistence_thread_metrics))) {
			memset(metrics, 0,
					threads * sizeof(*controller->persistence_thread_metrics));
			controller->persistence_thread_metrics = metrics;
		}
	}
	controller->thread_contexts = threads
			? calloc(threads, sizeof(*controller->thread_contexts)) : 0;
	valid = valid && controller->journal && controller->object_store &&
		(!threads || (controller->threads &&
				controller->persistence_thread_metrics &&
				controller->thread_contexts)) &&
		vine_datavine_journal_replay(controller->journal, replay_record, controller) &&
		validate_catalog(controller);
	if (!valid) {
		vine_datavine_data_controller_close(controller);
		return 0;
	}
	for (size_t index = 0; index < threads; index++) {
		controller->thread_contexts[index].controller = controller;
		controller->thread_contexts[index].metrics =
				&controller->persistence_thread_metrics[index];
		if (pthread_create(&controller->threads[index], 0, data_worker,
				&controller->thread_contexts[index])) {
			vine_datavine_data_controller_close(controller);
			return 0;
		}
		controller->thread_count++;
	}
	return controller;
}

void vine_datavine_data_controller_close(
		struct vine_datavine_data_controller *controller)
{
	if (!controller)
		return;
	if (controller->queue_lock_initialized) {
		pthread_mutex_lock(&controller->queue_lock);
		controller->stopping = 1;
		if (controller->queued_initialized)
			pthread_cond_broadcast(&controller->queued);
		if (controller->space_initialized)
			pthread_cond_broadcast(&controller->space);
		if (controller->prepared_initialized)
			pthread_cond_broadcast(&controller->prepared);
		pthread_mutex_unlock(&controller->queue_lock);
	}
	for (size_t index = 0; index < controller->thread_count; index++)
		pthread_join(controller->threads[index], 0);
	while (controller->head) {
		struct publication_job *next = controller->head->next;
		job_delete(controller->head);
		controller->head = next;
	}
	vine_datavine_object_store_close(controller->object_store);
	catalog_delete(controller);
	free(controller->losses.data_ids);
	free(controller->requested_results);
	free(controller->backup_queue);
	agent_namespace_delete(controller->workflow);
	free(controller->workflow_id);
	free(controller->threads);
	free(controller->persistence_thread_metrics);
	free(controller->thread_contexts);
	free(controller->root);
	free(controller->object_host);
	free(controller->object_token);
	if (controller->prepared_initialized)
		pthread_cond_destroy(&controller->prepared);
	if (controller->space_initialized)
		pthread_cond_destroy(&controller->space);
	if (controller->queued_initialized)
		pthread_cond_destroy(&controller->queued);
	if (controller->queue_lock_initialized)
		pthread_mutex_destroy(&controller->queue_lock);
	if (controller->lock_initialized)
		pthread_mutex_destroy(&controller->lock);
	free(controller);
}

int vine_datavine_data_controller_task_metrics(
		struct vine_datavine_data_controller *controller,
		struct vine_task *completed,
		struct vine_datavine_data_publication_metrics *metrics)
{
	if (!controller || !completed || !metrics)
		return 0;
	memset(metrics, 0, sizeof(*metrics));
	const char *manifest = vine_task_get_stdout(completed);
	if (!manifest || strncmp(manifest,
			VINE_DATAVINE_OUTPUT_MANIFEST_MAGIC "\n", 5))
		return 0;
	const char *timing = strstr(manifest, "\nM ");
	const char *report = timing ? strstr(timing + 1, "\nR ") : 0;
	unsigned long long pull = 0;
	unsigned long long decode = 0;
	unsigned long long function = 0;
	unsigned long long serialize = 0;
	unsigned long long fsync = 0;
	unsigned long long read_bytes = 0;
	unsigned long long cpu_milliseconds = 0;
	if (!timing || !report || sscanf(timing + 1,
			"M %llu %llu %llu %llu %llu", &pull, &decode, &function,
			&serialize, &fsync) != 5 ||
			sscanf(report + 1, "R %llu %llu", &read_bytes,
					&cpu_milliseconds) != 2)
		return 0;
	metrics->pull_nanoseconds = (uint64_t)pull;
	metrics->decode_nanoseconds = (uint64_t)decode;
	metrics->function_nanoseconds = (uint64_t)function;
	metrics->serialize_nanoseconds = (uint64_t)serialize;
	metrics->fsync_nanoseconds = (uint64_t)fsync;
	metrics->task_reports = 1;
	metrics->task_reported_read_bytes = (uint64_t)read_bytes;
	metrics->task_reported_cpu_milliseconds = (uint64_t)cpu_milliseconds;
	return 1;
}

int vine_datavine_data_controller_fetch_result(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		char **data, size_t *size)
{
	if (!controller || !workflow_id || !data || !size || !data_id)
		return 0;
	*data = 0;
	*size = 0;
	pthread_mutex_lock(&controller->lock);
	struct data_result *result = result_lookup(controller, workflow_id, data_id);
	char *path = result ? strdup(result->path) : 0;
	uint64_t expected_size = result ? result->info.size : 0;
	pthread_mutex_unlock(&controller->lock);
	if (!path || expected_size > SIZE_MAX) {
		free(path);
		return 0;
	}
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	free(path);
	char *buffer = fd >= 0 ? malloc((size_t)expected_size + 1) : 0;
	size_t used = 0;
	while (buffer && used < (size_t)expected_size) {
		ssize_t count = read(fd, buffer + used, (size_t)expected_size - used);
		if (count > 0)
			used += (size_t)count;
		else {
			free(buffer);
			buffer = 0;
		}
	}
	if (fd >= 0)
		close(fd);
	if (!buffer)
		return 0;
	buffer[used] = 0;
	*data = buffer;
	*size = used;
	return 1;
}

int vine_datavine_data_controller_result_info(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		struct vine_datavine_workflow_result_info *result)
{
	if (!controller || !workflow_id || !data_id || !result)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct data_result *stored = result_lookup(controller, workflow_id, data_id);
	if (stored)
		*result = stored->info;
	pthread_mutex_unlock(&controller->lock);
	return stored != 0;
}

int vine_datavine_data_controller_result_descriptors(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, const uint64_t *data_ids, size_t count,
		struct vine_datavine_workflow_result_info *results, char **paths)
{
	if (!controller || !workflow_id || !data_ids || !count || !results || !paths)
		return 0;
	memset(paths, 0, count * sizeof(*paths));
	int valid = 1;
	pthread_mutex_lock(&controller->lock);
	for (size_t index = 0; valid && index < count; index++) {
		struct data_result *stored = data_ids[index]
								 ? result_lookup(controller, workflow_id, data_ids[index])
								 : 0;
		valid = stored && stored->path;
		if (valid) {
			results[index] = stored->info;
			paths[index] = strdup(stored->path);
			valid = paths[index] != 0;
		}
	}
	pthread_mutex_unlock(&controller->lock);
	if (!valid) {
		for (size_t index = 0; index < count; index++) {
			free(paths[index]);
			paths[index] = 0;
		}
	}
	return valid;
}

int vine_datavine_data_controller_next_requested_result(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t after_sequence,
		uint64_t *sequence,
		struct vine_datavine_workflow_result_info *result, char **path)
{
	if (!controller || !workflow_id || !sequence || !result || !path)
		return -1;
	*path = 0;
	*sequence = after_sequence;
	int found = 0;
	pthread_mutex_lock(&controller->lock);
	if (controller->workflow_id && !strcmp(controller->workflow_id, workflow_id)) {
		size_t index = (size_t)after_sequence;
		if ((uint64_t)index != after_sequence)
			index = controller->requested_result_count;
		while (index < controller->requested_result_count) {
			struct requested_result_record *record =
					&controller->requested_results[index++];
			struct data_result *stored = result_lookup(controller,
					workflow_id, record->data_id);
			if (!stored || stored->info.attempt != record->attempt ||
					!stored->info.requested || !stored->path)
				continue;
			*result = stored->info;
			*path = strdup(stored->path);
			*sequence = (uint64_t)index;
			found = *path ? 1 : -1;
			break;
		}
		if (!found)
			*sequence = (uint64_t)index;
	}
	pthread_mutex_unlock(&controller->lock);
	return found;
}

void vine_datavine_data_controller_set_result_notifier(
		struct vine_datavine_data_controller *controller,
		void (*notify)(void *), void *context)
{
	if (!controller)
		return;
	pthread_mutex_lock(&controller->lock);
	controller->result_notify = notify;
	controller->result_notify_context = context;
	pthread_mutex_unlock(&controller->lock);
}

int vine_datavine_data_controller_result_active(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	if (!controller || !workflow_id || !data_id)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct data_result *result = result_lookup(controller, workflow_id, data_id);
	int active = result != 0;
	pthread_mutex_unlock(&controller->lock);
	return active;
}

int vine_datavine_data_controller_result_counts(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, size_t *active, size_t *peak)
{
	if (!controller || !workflow_id || !active || !peak)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct workflow_losses *losses = workflow_losses_get(
			controller, workflow_id, 0);
	*active = losses ? losses->active_results : 0;
	*peak = losses ? losses->peak_results : 0;
	pthread_mutex_unlock(&controller->lock);
	return 1;
}

int vine_datavine_data_controller_requested_results_ready(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, size_t expected)
{
	if (!controller || !workflow_id)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct workflow_losses *losses = workflow_losses_get(
			controller, workflow_id, 0);
	size_t active = losses ? losses->active_requested_results : 0;
	pthread_mutex_unlock(&controller->lock);
	return active == expected;
}

int vine_datavine_data_controller_finish_workflow(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id)
{
	if (!controller || !workflow_id || !workflow_id[0])
		return 0;
	pthread_mutex_lock(&controller->lock);
	size_t capacity = workflow_matches(controller, workflow_id)
			? controller->losses.active_results : 0;
	uint64_t *drop = capacity ? malloc(capacity * sizeof(*drop)) : 0;
	uint64_t *durable_drop = capacity
						 ? malloc(capacity * sizeof(*durable_drop))
						 : 0;
	size_t count = 0;
	size_t durable_count = 0;
	int valid = !capacity || (drop && durable_drop);
	for (size_t chunk = 0; valid && chunk < controller->catalog_capacity;
			chunk++) {
		if (!controller->catalog[chunk])
			continue;
		for (size_t index = 0; index < CATALOG_CHUNK_SIZE; index++) {
			struct data_result *result = catalog_result(
					&controller->catalog[chunk]->records[index]);
			if (result) {
				if (valid && !result->info.requested) {
					drop[count++] = result->info.data_id;
					durable_drop[durable_count++] = result->info.data_id;
				}
			}
		}
	}
	if (valid && durable_count) {
		size_t workflow_size = strlen(workflow_id);
		size_t payload_size = 4 + workflow_size + durable_count * 8;
		unsigned char *payload = payload_size <= UINT32_MAX
							 ? malloc(payload_size)
							 : 0;
		valid = payload && workflow_size <= UINT16_MAX &&
			durable_count <= UINT16_MAX;
		if (valid) {
			vine_datavine_put_u16(payload, (uint16_t)workflow_size);
			vine_datavine_put_u16(payload + 2, (uint16_t)durable_count);
			memcpy(payload + 4, workflow_id, workflow_size);
			for (size_t index = 0; index < durable_count; index++)
				vine_datavine_put_u64(payload + 4 + workflow_size + index * 8,
						durable_drop[index]);
			valid = vine_datavine_journal_commit(controller->journal,
					DATA_RELEASE_BATCH,
					payload,
					payload_size);
		}
		free(payload);
	}
	if (valid) {
		for (size_t index = 0; index < count; index++) {
			struct data_result *removed = result_remove(
					controller, workflow_id, drop[index]);
			if (removed && removed->path)
				unlink(removed->path);
			data_result_delete(removed);
		}
	}
	free(durable_drop);
	free(drop);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}
