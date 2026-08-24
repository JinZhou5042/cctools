/* Concurrent file-backed DataVine data controller. */

#include "vine_datavine_data_controller.h"
#include "vine_datavine_ir.h"

#include "create_dir.h"
#include "hash_table.h"
#include "itable.h"
#include "jx.h"
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

struct data_result {
	struct vine_datavine_workflow_result_info info;
	char *path;
	struct vine_file *remote_file;
	struct workflow_losses *losses;
	int durable;
	int lost;
};

struct workflow_losses {
	uint64_t *data_ids;
	size_t count;
	size_t capacity;
	size_t active_results;
	size_t peak_results;
};

struct publication_output {
	struct vine_datavine_workflow_result_info info;
	char *path;
	struct vine_file *remote_file;
	int remote_only;
	int direct_durable;
	char *plain_data;
	size_t plain_size;
};

struct vine_datavine_data_publication {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	int done;
	int successful;
	struct vine_datavine_data_publication_metrics metrics;
};

struct publication_job {
	char *workflow_id;
	int64_t task_id;
	uint32_t attempt;
	struct publication_output *outputs;
	size_t count;
	struct vine_datavine_data_publication *publication;
	struct timespec queued_at;
	uint64_t pull_nanoseconds;
	uint64_t decode_nanoseconds;
	uint64_t function_nanoseconds;
	uint64_t serialize_nanoseconds;
	uint64_t fsync_nanoseconds;
	uint64_t task_reports;
	uint64_t task_reported_read_bytes;
	uint64_t task_reported_cpu_milliseconds;
	struct vine_datavine_data_publication_metrics metrics;
	struct timespec commit_started;
	struct publication_output *durable;
	size_t durable_count;
	int lost;
	struct publication_job *next;
};

struct result_installation {
	struct data_result **pending;
	uint64_t *data_ids;
	const char *workflow_id;
	size_t count;
};

struct persistence_notice {
	uint64_t size;
	char sha256[65];
};

struct agent_namespace {
	char *workflow_id;
	unsigned char workflow_key[32];
	uint64_t workflow_slot;
	struct vine_datavine_replica_table *replicas;
	struct agent_endpoint *endpoints;
	size_t endpoint_capacity;
	uint32_t next_worker_slot;
	struct itable *result_metadata;
};

struct agent_result_metadata {
	struct vine_datavine_workflow_result_info info;
};

struct agent_endpoint {
	char host[VINE_DATAVINE_AGENT_HOST_MAX];
	uint16_t port;
	struct vine_datavine_agent_release *releases;
	size_t release_count;
	size_t release_capacity;
	uint64_t next_release_sequence;
};

struct agent_release_context {
	struct agent_namespace *namespace;
	int valid;
};

static void agent_queue_release(uint64_t released_data_id,
		uint32_t released_generation,
		const struct vine_datavine_replica_view *replica, void *argument);

struct vine_datavine_data_controller {
	pthread_mutex_t lock;
	pthread_mutex_t queue_lock;
	pthread_cond_t queued;
	pthread_cond_t space;
	pthread_cond_t prepared;
	struct hash_table *result_namespaces;
	struct hash_table *result_files;
	struct hash_table *workflow_losses;
	struct hash_table *object_files;
	struct hash_table *persistence_notices;
	struct hash_table *agent_workflows;
	struct itable *agent_slots;
	struct vine_datavine_object_store *object_store;
	struct vine_datavine_journal *journal;
	char *root;
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
	int commit_active;
	int loss_recording_failed;
	int stopping;
	int lock_initialized;
	int queue_lock_initialized;
	int queued_initialized;
	int space_initialized;
	int prepared_initialized;
};

static void agent_namespace_delete(void *value)
{
	struct agent_namespace *namespace = value;
	if (!namespace)
		return;
	vine_datavine_replica_table_delete(namespace->replicas);
	if (namespace->result_metadata) {
		uint64_t data_id;
		void *metadata;
		int iterator;
		ITABLE_ITERATE(namespace->result_metadata, iterator, data_id, metadata)
		{
			free(metadata);
		}
		itable_delete(namespace->result_metadata);
	}
	for (size_t index = 0; index < namespace->endpoint_capacity; index++)
		free(namespace->endpoints[index].releases);
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
	return controller && controller->agent_slots && workflow_slot
			   ? itable_lookup(controller->agent_slots, workflow_slot)
			   : 0;
}

static struct agent_namespace *agent_namespace_prepare(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id)
{
	struct agent_namespace *namespace = hash_table_lookup(
			controller->agent_workflows, workflow_id);
	if (namespace)
		return namespace;
	namespace = calloc(1, sizeof(*namespace));
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
	namespace->result_metadata = itable_create(0);
	struct agent_namespace *collision = itable_lookup(
			controller->agent_slots, namespace->workflow_slot);
	if (!namespace->workflow_id || !namespace->replicas ||
			!namespace->result_metadata ||
			collision ||
			!hash_table_insert(controller->agent_workflows, workflow_id,
				namespace) ||
			(!collision && !itable_insert(controller->agent_slots,
				namespace->workflow_slot, namespace))) {
		if (hash_table_lookup(controller->agent_workflows, workflow_id) ==
				namespace)
			hash_table_remove(controller->agent_workflows, workflow_id);
		agent_namespace_delete(namespace);
		return 0;
	}
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

int vine_datavine_data_controller_object_path(
		struct vine_datavine_data_controller *controller, const char sha256[65],
		char *path, size_t path_size)
{
	return controller && vine_datavine_object_store_path(
						 controller->object_store, sha256, path, path_size, 0);
}

int vine_datavine_data_controller_object_metrics(
		struct vine_datavine_data_controller *controller,
		struct vine_datavine_object_store_metrics *metrics)
{
	return controller && vine_datavine_object_store_metrics(
						 controller->object_store, metrics);
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

const char *vine_datavine_data_controller_object_root(
		struct vine_datavine_data_controller *controller)
{
	return controller
				   ? vine_datavine_object_store_root(controller->object_store)
				   : 0;
}

char *vine_datavine_data_controller_persistence_context(
		struct vine_datavine_data_controller *controller)
{
	if (!controller || !controller->object_host || !controller->object_token ||
			controller->object_port < 1)
		return 0;
	size_t token_size = strlen(controller->object_token);
	char *token_hex = malloc(token_size * 2 + 1);
	if (!token_hex)
		return 0;
	for (size_t index = 0; index < token_size; index++)
		snprintf(token_hex + index * 2, 3, "%02x", (unsigned char)controller->object_token[index]);
	char *context = string_format("%s\n%d\n%s\n%s", controller->object_host, controller->object_port, token_hex, controller->root);
	free(token_hex);
	return context;
}

char *vine_datavine_data_controller_object_ticket(
		struct vine_datavine_data_controller *controller,
		const char *sha256)
{
	if (!controller || !sha256 || strlen(sha256) != 64)
		return 0;
	char path[PATH_MAX];
	if (!vine_datavine_data_controller_object_path(
				controller, sha256, path, sizeof(path)))
		return 0;
	return string_format("datavine-file://%s", path);
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

struct vine_file *vine_datavine_data_controller_resolve_object(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *sha256)
{
	if (!controller || !manager || !sha256)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct vine_file *file = hash_table_lookup(controller->object_files, sha256);
	if (!file) {
		char *uri = vine_datavine_data_controller_object_ticket(controller, sha256);
		char cached_name[96];
		int named = snprintf(cached_name, sizeof(cached_name), "datavine-sha256-%s", sha256) < (int)sizeof(cached_name);
		file = uri && named ? vine_declare_url_cached(manager, uri, cached_name, VINE_CACHE_LEVEL_WORKER, 0) : 0;
		free(uri);
		if (file && !hash_table_insert(controller->object_files, sha256, file))
			file = 0;
	}
	pthread_mutex_unlock(&controller->lock);
	return file;
}

static uint64_t elapsed_nanoseconds(
		const struct timespec *started, const struct timespec *finished)
{
	return (uint64_t)((finished->tv_sec - started->tv_sec) * INT64_C(1000000000) +
			  finished->tv_nsec - started->tv_nsec);
}

static void data_result_delete(void *value)
{
	struct data_result *result = value;
	if (result) {
		free(result->path);
		free(result);
	}
}

static void result_namespace_delete(void *value)
{
	struct itable *results = value;
	if (results) {
		itable_clear(results, data_result_delete);
		itable_delete(results);
	}
}

static void workflow_losses_delete(void *value)
{
	struct workflow_losses *losses = value;
	if (losses) {
		free(losses->data_ids);
		free(losses);
	}
}

static struct workflow_losses *workflow_losses_get(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, int create)
{
	struct workflow_losses *losses = hash_table_lookup(
			controller->workflow_losses, workflow_id);
	if (!losses && create) {
		losses = calloc(1, sizeof(*losses));
		if (losses && !hash_table_insert(controller->workflow_losses,
					  workflow_id, losses)) {
			free(losses);
			losses = 0;
		}
	}
	return losses;
}

static int workflow_loss_append(struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	struct workflow_losses *losses = workflow_losses_get(
			controller, workflow_id, 1);
	if (!losses)
		return 0;
	for (size_t index = 0; index < losses->count; index++) {
		if (losses->data_ids[index] == data_id)
			return 1;
	}
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
	losses->data_ids[losses->count++] = data_id;
	return 1;
}

static struct itable *result_namespace(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, int create)
{
	struct itable *results = hash_table_lookup(
			controller->result_namespaces, workflow_id);
	if (!results && create) {
		results = itable_create(0);
		if (results && !hash_table_insert(controller->result_namespaces,
						   workflow_id,
						   results)) {
			itable_delete(results);
			results = 0;
		}
	}
	return results;
}

static struct data_result *result_lookup(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	struct itable *results = result_namespace(controller, workflow_id, 0);
	return results ? itable_lookup(results, data_id) : 0;
}

static int result_insert(struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, struct data_result *result)
{
	struct itable *results = result_namespace(controller, workflow_id, 1);
	struct workflow_losses *losses = workflow_losses_get(
			controller, workflow_id, 1);
	if (!results || !losses || !itable_insert(results, data_id, result))
		return 0;
	result->losses = losses;
	const char *cached_name = !result->durable && result->remote_file
						  ? vine_file_cached_name(result->remote_file)
						  : 0;
	if (cached_name && !hash_table_insert(controller->result_files,
					   cached_name,
					   result)) {
		itable_remove(results, data_id);
		result->losses = 0;
		return 0;
	}
	losses->active_results++;
	if (losses->active_results > losses->peak_results)
		losses->peak_results = losses->active_results;
	return 1;
}

static struct data_result *result_remove(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	struct itable *results = result_namespace(controller, workflow_id, 0);
	struct data_result *result = results ? itable_remove(results, data_id) : 0;
	const char *cached_name = result && !result->durable && result->remote_file
						  ? vine_file_cached_name(result->remote_file)
						  : 0;
	if (cached_name)
		hash_table_remove(controller->result_files, cached_name);
	if (result && result->losses && result->losses->active_results)
		result->losses->active_results--;
	return result;
}

int vine_datavine_data_controller_last_replica_lost(
		struct vine_datavine_data_controller *controller,
		const char *cached_name)
{
	if (!controller || !cached_name)
		return 0;
	int matched = 0;
	pthread_mutex_lock(&controller->lock);
	struct data_result *result = hash_table_lookup(
			controller->result_files, cached_name);
	if (result && !result->durable && !result->lost && result->losses) {
		struct workflow_losses *losses = result->losses;
		if (losses->count == losses->capacity) {
			size_t capacity = losses->capacity ? losses->capacity * 2 : 16;
			uint64_t *data_ids = realloc(losses->data_ids,
					capacity * sizeof(*data_ids));
			if (!data_ids) {
				controller->loss_recording_failed = 1;
				pthread_mutex_unlock(&controller->lock);
				return 1;
			}
			losses->data_ids = data_ids;
			losses->capacity = capacity;
		}
		losses->data_ids[losses->count++] = result->info.data_id;
		result->lost = 1;
		matched = 1;
	}
	pthread_mutex_unlock(&controller->lock);
	pthread_mutex_lock(&controller->queue_lock);
	for (struct publication_job *job = controller->head; job; job = job->next) {
		for (size_t index = 0; index < job->count; index++) {
			struct vine_file *file = job->outputs[index].remote_file;
			const char *name = file ? vine_file_cached_name(file) : 0;
			if (name && !strcmp(name, cached_name)) {
				job->lost = 1;
				matched = 1;
				break;
			}
		}
	}
	if (matched)
		pthread_cond_broadcast(&controller->queued);
	pthread_mutex_unlock(&controller->queue_lock);
	return matched;
}

int vine_datavine_data_controller_take_workflow_losses(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id,
		struct itable *files,
		uint64_t **data_ids, size_t *count)
{
	if (!controller || !manager || !workflow_id || !workflow_id[0] ||
			!files || !data_ids || !count)
		return 0;
	*data_ids = 0;
	*count = 0;
	pthread_mutex_lock(&controller->lock);
	if (controller->loss_recording_failed) {
		pthread_mutex_unlock(&controller->lock);
		return 0;
	}
	struct workflow_losses *losses = hash_table_lookup(
			controller->workflow_losses, workflow_id);
	uint64_t *lost = losses ? losses->data_ids : 0;
	size_t lost_count = losses ? losses->count : 0;
	if (losses) {
		losses->data_ids = 0;
		losses->count = 0;
		losses->capacity = 0;
	}
	size_t next = 0;
	for (size_t index = 0; index < lost_count; index++) {
		struct data_result *removed = result_lookup(
				controller, workflow_id, lost[index]);
		if (!removed || !removed->lost)
			continue;
		removed = result_remove(controller, workflow_id, lost[index]);
		if (removed && removed->remote_file) {
			if (itable_lookup(files, lost[index]) == removed->remote_file)
				itable_remove(files, lost[index]);
			vine_undeclare_file_no_loss_event(manager, removed->remote_file);
		}
		data_result_delete(removed);
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
	unsigned char digest[SHA256_DIGEST_LENGTH];
	char encoded[SHA256_DIGEST_LENGTH * 2 + 1];
	SHA256((const unsigned char *)workflow_id, strlen(workflow_id), digest);
	for (size_t index = 0; index < sizeof(digest); index++)
		snprintf(encoded + index * 2, 3, "%02x", digest[index]);
	if (snprintf(path, PATH_MAX, "%s/%s", controller->root, encoded) >=
			PATH_MAX)
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
	struct agent_namespace *namespace = hash_table_lookup(
			controller->agent_workflows, workflow_id);
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
	if (!controller || !workflow_id || !workflow_id[0] ||
			!workflow_directory_path(controller, workflow_id, path) ||
			!create_dir(path, 0700))
		return 0;
	pthread_mutex_lock(&controller->lock);
	int valid = agent_namespace_prepare(controller, workflow_id) != 0;
	pthread_mutex_unlock(&controller->lock);
	return valid;
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
		vine_datavine_replica_table_session_open(
			namespace->replicas, assigned, session_epoch);
	if (valid) {
		snprintf(namespace->endpoints[assigned].host,
				sizeof(namespace->endpoints[assigned].host), "%s", host);
		namespace->endpoints[assigned].port = port;
	}
	pthread_mutex_unlock(&controller->lock);
	if (valid) {
		*worker_slot = assigned;
		*workflow_slot = slot;
	}
	return valid;
}

int vine_datavine_data_controller_agent_expect(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t *generation,
		int requested)
{
	if (!controller || !workflow_id || !workflow_id[0] || !data_id ||
			!generation || !*generation)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = hash_table_lookup(
			controller->agent_workflows, workflow_id);
	int valid = namespace && vine_datavine_replica_table_expect(
			namespace->replicas, data_id, generation, requested);
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
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = hash_table_lookup(
			controller->agent_workflows, workflow_id);
	struct agent_result_metadata *known = namespace
			? itable_lookup(namespace->result_metadata, data_id) : 0;
	int valid = namespace != 0;
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
		valid = known && itable_insert(namespace->result_metadata,
				data_id, known);
		if (!valid)
			free(known);
	}
	pthread_mutex_unlock(&controller->lock);
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
		uint64_t session_epoch, uint64_t object_token, int requested)
{
	if (!controller || !workflow_slot || !data_id || !generation || !digest)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	int valid = namespace && vine_datavine_replica_table_publish(
			namespace->replicas, data_id, generation, size, digest,
			worker_slot, session_epoch, object_token, requested, 0, 0);
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
	}
	if (views != local_views)
		free(views);
	pthread_mutex_unlock(&controller->lock);
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
	if (!valid && getenv("DATAVINE_WORKFLOW_METRICS"))
		fprintf(stderr,
				"datavine controller mark_dead_failed slot=%llu data=%llu generation=%u namespace=%d before=%d release_valid=%d\n",
				(unsigned long long)workflow_slot,
				(unsigned long long)data_id, generation, namespace != 0,
				(int)before, context.valid);
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

int vine_datavine_data_controller_output_path(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t attempt,
		char *path, size_t path_size)
{
	char resolved[PATH_MAX];
	if (!controller || !workflow_id || !data_id || !attempt || !path ||
			!output_path(controller, workflow_id, data_id, attempt, resolved) ||
			strlen(resolved) + 1 > path_size)
		return 0;
	memcpy(path, resolved, strlen(resolved) + 1);
	return 1;
}

int vine_datavine_data_controller_result_persisted(
		struct vine_datavine_data_controller *controller,
		const char *path, uint64_t size, const char sha256[65])
{
	if (!controller || !path || !sha256 || strlen(sha256) != 64 ||
			strncmp(path, controller->root, strlen(controller->root)) ||
			path[strlen(controller->root)] != '/')
		return 0;
	struct persistence_notice *notice = calloc(1, sizeof(*notice));
	if (!notice)
		return 0;
	notice->size = size;
	memcpy(notice->sha256, sha256, 65);
	pthread_mutex_lock(&controller->lock);
	struct persistence_notice *old = hash_table_remove(
			controller->persistence_notices, path);
	int inserted = hash_table_insert(controller->persistence_notices, path, notice);
	pthread_mutex_unlock(&controller->lock);
	free(old);
	if (!inserted) {
		free(notice);
		return 0;
	}
	pthread_mutex_lock(&controller->queue_lock);
	pthread_cond_broadcast(&controller->queued);
	pthread_mutex_unlock(&controller->queue_lock);
	return 1;
}

static int output_path_in_directory(const char *directory, uint64_t data_id,
		uint32_t attempt, char path[PATH_MAX])
{
	return snprintf(path, PATH_MAX, "%s/%llu.%u.data", directory, (unsigned long long)data_id, attempt) < PATH_MAX;
}

static int write_all(int fd, const char *data, size_t size)
{
	while (size) {
		ssize_t written = write(fd, data, size);
		if (written > 0) {
			data += written;
			size -= (size_t)written;
		} else {
			return 0;
		}
	}
	return 1;
}

static int write_plain_output(struct publication_output *output)
{
	if (!output->plain_data)
		return 1;
	int fd = open(output->path,
			O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
			0600);
	int valid = fd >= 0 &&
			write_all(fd, output->plain_data, output->plain_size) &&
			fsync(fd) == 0;
	if (fd >= 0 && close(fd))
		valid = 0;
	return valid;
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

static void digest_hex(const unsigned char digest[32], char encoded[65])
{
	for (size_t index = 0; index < 32; index++)
		snprintf(encoded + index * 2, 3, "%02x", digest[index]);
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
				if (!valid || known->durable)
					continue;
			}
		}
		installation->pending[index] = calloc(
				1, sizeof(*installation->pending[index]));
		if (installation->pending[index]) {
			installation->pending[index]->info = outputs[index].info;
			installation->pending[index]->path = strdup(outputs[index].path);
			installation->pending[index]->remote_file = outputs[index].remote_file;
			installation->pending[index]->durable = 1;
		}
		valid = installation->pending[index] &&
			installation->pending[index]->path;
	}
	if (!valid)
		installation_delete(installation);
	return valid;
}

static void installation_apply(struct vine_datavine_data_controller *controller,
		struct result_installation *installation)
{
	for (size_t index = 0; index < installation->count; index++) {
		struct data_result *known = result_lookup(controller, installation->workflow_id, installation->data_ids[index]);
		if (known && installation->pending[index] &&
				(known->info.attempt < installation->pending[index]->info.attempt ||
						!known->durable)) {
			known = result_remove(controller, installation->workflow_id, installation->data_ids[index]);
			if (known->path)
				unlink(known->path);
			data_result_delete(known);
		}
		if (!result_lookup(controller, installation->workflow_id, installation->data_ids[index])) {
			if (!result_insert(controller, installation->workflow_id, installation->data_ids[index], installation->pending[index]))
				abort();
			installation->pending[index] = 0;
		}
	}
}

static int install_results(struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct publication_output *outputs,
		size_t count, int replay)
{
	struct result_installation installation;
	int valid = installation_prepare(controller, workflow_id, outputs, count, &installation);
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
		installation_apply(controller, &installation);
	installation_delete(&installation);
	return valid;
}

int vine_datavine_data_controller_agent_persisted(
		struct vine_datavine_data_controller *controller,
		uint64_t workflow_slot, uint64_t data_id, uint32_t generation,
		uint64_t size, const unsigned char digest[32])
{
	if (!controller || !workflow_slot || !data_id || !generation || !digest)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct agent_namespace *namespace = agent_namespace_lookup(
			controller, workflow_slot);
	struct agent_result_metadata *metadata = namespace
			? itable_lookup(namespace->result_metadata, data_id) : 0;
	struct vine_datavine_replica_view replica;
	size_t replica_count = 0;
	uint64_t expected_size = 0;
	unsigned char expected_digest[32];
	enum vine_datavine_resolve_status status = namespace
			? vine_datavine_replica_table_resolve(namespace->replicas, data_id,
					generation, &replica, 1, &replica_count, &expected_size,
					expected_digest, 0)
			: VINE_DATAVINE_RESOLVE_UNKNOWN;
	char path[PATH_MAX];
	struct stat info;
	int valid = metadata && metadata->info.attempt == generation &&
			status == VINE_DATAVINE_RESOLVE_AVAILABLE && replica_count &&
			expected_size == size && !memcmp(expected_digest, digest, 32) &&
			output_path(controller, namespace->workflow_id, data_id,
					generation, path) && !stat(path, &info) &&
			S_ISREG(info.st_mode) && (uint64_t)info.st_size == size;
	struct publication_output output;
	memset(&output, 0, sizeof(output));
	if (valid) {
		output.info = metadata->info;
		output.info.size = size;
		digest_hex(digest, output.info.sha256);
		output.path = path;
		valid = install_results(controller, namespace->workflow_id,
				&output, 1, 0) &&
			vine_datavine_replica_table_set_persisted(
					namespace->replicas, data_id, generation);
	}
	pthread_mutex_unlock(&controller->lock);
	return valid;
}

static int install_soft_results(struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct publication_output *outputs,
		size_t count)
{
	int valid = 1;
	for (size_t index = 0; valid && index < count; index++) {
		uint64_t data_id = outputs[index].info.data_id;
		struct data_result *known = result_lookup(controller, workflow_id, data_id);
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
		struct data_result *result = calloc(1, sizeof(*result));
		if (result) {
			result->info = outputs[index].info;
			result->remote_file = outputs[index].remote_file;
		}
		valid = result != 0;
		if (valid && known) {
			known = result_remove(controller, workflow_id, data_id);
			data_result_delete(known);
		}
		if (valid && !result_insert(controller, workflow_id, data_id, result))
			abort();
		if (!valid)
			data_result_delete(result);
	}
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
	char *workflow_id;
	struct itable *results;
	int namespace_iterator;
	HASH_TABLE_ITERATE(controller->result_namespaces,
			namespace_iterator,
			workflow_id,
			results)
	{
		UINT64_T data_id;
		struct data_result *result;
		int result_iterator;
		ITABLE_ITERATE(results, result_iterator, data_id, result)
		{
			uint64_t size = 0;
			unsigned char digest[32];
			char encoded[65];
			if (!hash_file(result->path, &size, digest))
				return 0;
			digest_hex(digest, encoded);
			if (size != result->info.size || strcmp(encoded, result->info.sha256))
				return 0;
		}
	}
	return 1;
}

static void publication_finish(
		struct vine_datavine_data_publication *publication, int successful,
		const struct vine_datavine_data_publication_metrics *metrics)
{
	pthread_mutex_lock(&publication->lock);
	if (metrics)
		publication->metrics = *metrics;
	publication->successful = successful;
	publication->done = 1;
	pthread_cond_broadcast(&publication->changed);
	pthread_mutex_unlock(&publication->lock);
}

static int prepare_job(struct vine_datavine_data_controller *controller,
		struct publication_job *job)
{
	if (job->lost)
		return 0;
	job->durable = calloc(job->count, sizeof(*job->durable));
	struct publication_output *soft = calloc(job->count, sizeof(*soft));
	size_t soft_count = 0;
	int valid = job->durable && soft;
	for (size_t index = 0; valid && index < job->count; index++) {
		if (job->outputs[index].remote_file)
			soft[soft_count++] = job->outputs[index];
		if (!job->outputs[index].remote_only ||
				job->outputs[index].direct_durable)
			job->durable[job->durable_count++] = job->outputs[index];
	}
	job->metrics.outputs = job->count;
	job->metrics.remote_outputs = soft_count;
	job->metrics.durable_outputs = job->durable_count;
	if (soft_count) {
		pthread_mutex_lock(&controller->lock);
		valid = install_soft_results(controller, job->workflow_id, soft, soft_count);
		pthread_mutex_unlock(&controller->lock);
	}
	for (size_t index = 0; valid && index < job->durable_count; index++) {
		struct publication_output *output = &job->durable[index];
		if (output->direct_durable) {
			pthread_mutex_lock(&controller->lock);
			struct persistence_notice *notice = hash_table_remove(
					controller->persistence_notices, output->path);
			pthread_mutex_unlock(&controller->lock);
			valid = notice && notice->size == output->info.size &&
				!strcmp(notice->sha256, output->info.sha256);
			free(notice);
		} else {
			unsigned char digest[32];
			valid = write_plain_output(output) &&
				hash_file(output->path, &output->info.size, digest);
			if (valid)
				digest_hex(digest, output->info.sha256);
		}
	}
	for (size_t index = 0; index < soft_count; index++)
		job->metrics.output_bytes += soft[index].info.size;
	for (size_t index = 0; index < job->durable_count; index++)
		job->metrics.output_bytes += job->durable[index].info.size;
	free(soft);
	return valid;
}

static int batch_has_duplicate_results(struct publication_job *jobs)
{
	struct hash_table *namespaces = hash_table_create(0, 0);
	int duplicate = namespaces == 0;
	for (struct publication_job *job = jobs; !duplicate && job; job = job->next) {
		struct itable *seen = hash_table_lookup(namespaces, job->workflow_id);
		if (!seen) {
			seen = itable_create(0);
			if (!seen || !hash_table_insert(namespaces, job->workflow_id, seen)) {
				itable_delete(seen);
				duplicate = 1;
				break;
			}
		}
		for (size_t index = 0; index < job->durable_count; index++) {
			uint64_t data_id = job->durable[index].info.data_id;
			duplicate = itable_lookup(seen, data_id) != 0;
			if (!duplicate && !itable_insert(seen, data_id, seen))
				abort();
			if (duplicate)
				break;
		}
	}
	if (namespaces) {
		char *workflow_id;
		struct itable *seen;
		int iterator;
		HASH_TABLE_ITERATE(namespaces, iterator, workflow_id, seen)
		itable_delete(seen);
		hash_table_delete(namespaces);
	}
	return duplicate;
}

static void finish_job(struct publication_job *job, int successful)
{
	struct timespec finished;
	clock_gettime(CLOCK_MONOTONIC, &finished);
	job->metrics.commit_nanoseconds = elapsed_nanoseconds(
			&job->commit_started, &finished);
	publication_finish(job->publication, successful, &job->metrics);
}

static void commit_job_group(struct vine_datavine_data_controller *controller,
		struct publication_job *jobs)
{
	size_t count = 0;
	for (struct publication_job *job = jobs; job; job = job->next)
		count++;
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
		if (!installation_prepare(controller, job->workflow_id, job->durable, job->durable_count, installation))
			continue;
		payloads[accepted_count] = encode_publication(job->workflow_id,
				job->durable,
				job->durable_count,
				&payload_sizes[accepted_count]);
		if (!payloads[accepted_count]) {
			installation_delete(installation);
			continue;
		}
		accepted[accepted_count++] = job;
	}
	int committed = allocated && accepted_count;
	for (size_t index = 0; committed && index < accepted_count; index++)
		committed = vine_datavine_journal_enqueue(controller->journal,
				DATA_READY_BATCH,
				payloads[index],
				payload_sizes[index]);
	if (committed) {
		accepted[0]->metrics.journal_records += accepted_count;
		for (size_t index = 0; index < accepted_count; index++) {
			installation_apply(controller, &installations[index]);
			success[index] = 1;
		}
	}
	pthread_mutex_unlock(&controller->lock);

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
		finish_job(job, job_success);
	}
	free(success);
	free(payload_sizes);
	free(payloads);
	free(installations);
	free(accepted);
}

static void commit_prepared_jobs(struct vine_datavine_data_controller *controller,
		struct publication_job *jobs)
{
	if (!batch_has_duplicate_results(jobs)) {
		commit_job_group(controller, jobs);
		return;
	}
	while (jobs) {
		struct publication_job *next = jobs->next;
		jobs->next = 0;
		commit_job_group(controller, jobs);
		jobs->next = next;
		jobs = next;
	}
}

static void job_delete(struct publication_job *job)
{
	if (!job)
		return;
	for (size_t index = 0; index < job->count; index++) {
		free(job->outputs[index].path);
		free(job->outputs[index].plain_data);
	}
	free(job->durable);
	free(job->outputs);
	free(job->workflow_id);
	free(job);
}

static int job_persistence_ready(struct vine_datavine_data_controller *controller,
		const struct publication_job *job)
{
	if (job->lost)
		return 1;
	int ready = 1;
	pthread_mutex_lock(&controller->lock);
	for (size_t index = 0; index < job->count; index++) {
		if (job->outputs[index].direct_durable &&
				!hash_table_lookup(controller->persistence_notices,
						job->outputs[index].path)) {
			ready = 0;
			break;
		}
	}
	pthread_mutex_unlock(&controller->lock);
	return ready;
}

static void *data_worker(void *argument)
{
	struct vine_datavine_data_controller *controller = argument;
	for (;;) {
		pthread_mutex_lock(&controller->queue_lock);
		while (!controller->stopping && !controller->head)
			pthread_cond_wait(&controller->queued, &controller->queue_lock);
		if (controller->stopping && !controller->head) {
			pthread_mutex_unlock(&controller->queue_lock);
			break;
		}
		struct publication_job **ready_link = &controller->head;
		if (!controller->stopping) {
			while (*ready_link &&
					!job_persistence_ready(controller, *ready_link))
				ready_link = &(*ready_link)->next;
		}
		if (!*ready_link) {
			pthread_cond_wait(&controller->queued, &controller->queue_lock);
			pthread_mutex_unlock(&controller->queue_lock);
			continue;
		}
		struct publication_job *job = *ready_link;
		*ready_link = job->next;
		if (controller->tail == job) {
			controller->tail = 0;
			for (struct publication_job *tail = controller->head; tail;
					tail = tail->next)
				controller->tail = tail;
		}
		controller->queued_count--;
		pthread_cond_broadcast(&controller->space);
		pthread_mutex_unlock(&controller->queue_lock);
		clock_gettime(CLOCK_MONOTONIC, &job->commit_started);
		job->metrics.queue_nanoseconds = elapsed_nanoseconds(
				&job->queued_at, &job->commit_started);
		job->metrics.pull_nanoseconds = job->pull_nanoseconds;
		job->metrics.decode_nanoseconds = job->decode_nanoseconds;
		job->metrics.function_nanoseconds = job->function_nanoseconds;
		job->metrics.serialize_nanoseconds = job->serialize_nanoseconds;
		job->metrics.fsync_nanoseconds = job->fsync_nanoseconds;
		job->metrics.task_reports = job->task_reports;
		job->metrics.task_reported_read_bytes = job->task_reported_read_bytes;
		job->metrics.task_reported_cpu_milliseconds =
				job->task_reported_cpu_milliseconds;
		if (!prepare_job(controller, job)) {
			finish_job(job, 0);
			job_delete(job);
			continue;
		}
		if (!job->durable_count) {
			finish_job(job, 1);
			job_delete(job);
			continue;
		}

		pthread_mutex_lock(&controller->queue_lock);
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
			commit_prepared_jobs(controller, batch);
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
	controller->result_namespaces = hash_table_create(0, 0);
	controller->result_files = hash_table_create(0, 0);
	controller->workflow_losses = hash_table_create(0, 0);
	controller->object_files = hash_table_create(0, 0);
	controller->persistence_notices = hash_table_create(0, 0);
	controller->agent_workflows = hash_table_create(0, 0);
	controller->agent_slots = itable_create(0);
	controller->journal = journal;
	controller->object_store = valid
						   ? vine_datavine_object_store_open(workflow_journal_path)
						   : 0;
	controller->threads = threads ? calloc(threads, sizeof(*controller->threads)) : 0;
	valid = valid && controller->result_namespaces && controller->result_files &&
		controller->workflow_losses && controller->object_files &&
		controller->persistence_notices &&
		controller->agent_workflows && controller->agent_slots &&
		controller->journal && controller->object_store &&
		(!threads || controller->threads) &&
		vine_datavine_journal_replay(controller->journal, replay_record, controller) &&
		validate_catalog(controller);
	if (!valid) {
		vine_datavine_data_controller_close(controller);
		return 0;
	}
	for (size_t index = 0; index < threads; index++) {
		if (pthread_create(&controller->threads[index], 0, data_worker, controller)) {
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
		publication_finish(controller->head->publication, 0, 0);
		job_delete(controller->head);
		controller->head = next;
	}
	vine_datavine_object_store_close(controller->object_store);
	if (controller->result_files)
		hash_table_delete(controller->result_files);
	if (controller->result_namespaces) {
		hash_table_clear(controller->result_namespaces, result_namespace_delete);
		hash_table_delete(controller->result_namespaces);
	}
	if (controller->workflow_losses) {
		hash_table_clear(controller->workflow_losses, workflow_losses_delete);
		hash_table_delete(controller->workflow_losses);
	}
	if (controller->object_files)
		hash_table_delete(controller->object_files);
	if (controller->persistence_notices) {
		char *path;
		void *notice;
		int iterator;
		HASH_TABLE_ITERATE(controller->persistence_notices, iterator, path, notice)
		{
			free(notice);
		}
		hash_table_delete(controller->persistence_notices);
	}
	if (controller->agent_slots)
		itable_delete(controller->agent_slots);
	if (controller->agent_workflows) {
		hash_table_clear(controller->agent_workflows, agent_namespace_delete);
		hash_table_delete(controller->agent_workflows);
	}
	free(controller->threads);
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

static int retained_output(struct itable *consumers, struct itable *requested,
		uint64_t data_id, int retain_all)
{
	return retain_all || itable_lookup(consumers, data_id) ||
		   itable_lookup(requested, data_id);
}

int vine_datavine_data_controller_bind_outputs(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id,
		struct vine_task *physical, struct jx *task, struct jx *executor,
		struct itable *files,
		struct itable *consumers,
		struct itable *requested, uint32_t attempt, int retain_all)
{
	struct jx *output_files = jx_lookup(executor, "output_files");
	struct jx *output_ids = vine_datavine_ir_task_outputs(task);
	const char *version = jx_lookup_string(executor, "version");
	int worker_local = !strcmp(jx_lookup_string(executor, "kind"), "python") &&
			!jx_lookup(executor, "environment") &&
			(!strcmp(version, VINE_DATAVINE_PYTHON_CALLABLE_VERSION) ||
			 !strcmp(version, VINE_DATAVINE_PYTHON_SOURCE_VERSION));
	if (worker_local && !strcmp(version, VINE_DATAVINE_PYTHON_SOURCE_VERSION)) {
		for (int index = 0;
				output_files && index < jx_array_length(output_files);
				index++) {
			char expected[64];
			snprintf(expected, sizeof(expected),
					"datavine-python-output-%d", index);
			if (strcmp(jx_array_index(output_files, index)->u.string_value,
					expected))
				worker_local = 0;
		}
	}
	if (!output_files)
		return jx_array_length(output_ids) == 1;
	if (jx_array_length(output_files) != jx_array_length(output_ids))
		return 0;
	char directory[PATH_MAX];
	int directory_ready = 0;
	for (int index = 0; index < jx_array_length(output_files); index++) {
		uint64_t data_id = (uint64_t)jx_array_index(
				output_ids, index)
						   ->u.integer_value;
		if (!retained_output(consumers, requested, data_id, retain_all))
			continue;
		char path[PATH_MAX];
		int remote_only = worker_local;
		if (!remote_only && !directory_ready) {
			directory_ready = workflow_directory_path(controller, workflow_id, directory);
			if (!directory_ready)
				return 0;
		}
		struct vine_file *file = remote_only
							 ? vine_declare_temp(manager)
							 : (output_path_in_directory(directory, data_id, attempt, path)
											   ? vine_declare_file(manager, path, VINE_CACHE_LEVEL_WORKFLOW, 0)
											   : 0);
		const char *remote_name = jx_array_index(
				output_files, index)
							  ->u.string_value;
		struct vine_file *previous = itable_remove(files, data_id);
		if (previous)
			vine_undeclare_file_no_loss_event(manager, previous);
		if (!file || !vine_task_add_output(physical, file, remote_name, 0) ||
				!itable_insert(files, data_id, file))
			return 0;
	}
	return 1;
}

static struct vine_datavine_data_publication *publication_create(void)
{
	struct vine_datavine_data_publication *publication =
			calloc(1, sizeof(*publication));
	if (!publication)
		return 0;
	if (pthread_mutex_init(&publication->lock, 0)) {
		free(publication);
		return 0;
	}
	if (pthread_cond_init(&publication->changed, 0)) {
		pthread_mutex_destroy(&publication->lock);
		free(publication);
		return 0;
	}
	return publication;
}

static int fill_output_metadata(struct publication_output *output,
		struct jx *data_record, struct jx *default_codec,
		struct itable *requested, uint64_t data_id,
		uint32_t attempt, const char *path)
{
	struct jx *codec = data_record
					   ? vine_datavine_ir_data_codec(data_record, default_codec)
					   : 0;
	const char *codec_name = codec ? jx_lookup_string(codec, "name") : 0;
	const char *codec_version = codec ? jx_lookup_string(codec, "version") : 0;
	if (!data_record || !vine_datavine_ir_data_is_output(data_record) ||
			!codec_name || !codec_version ||
			strlen(codec_name) >= sizeof(output->info.codec_name) ||
			strlen(codec_version) >= sizeof(output->info.codec_version))
		return 0;
	output->info.data_id = data_id;
	output->info.attempt = attempt;
	output->info.producer_task_id = vine_datavine_ir_data_producer(data_record);
	output->info.producer_output_index =
			vine_datavine_ir_data_output_index(data_record);
	output->info.requested = itable_lookup(requested, data_id) != 0;
	snprintf(output->info.codec_name, sizeof(output->info.codec_name), "%s", codec_name);
	snprintf(output->info.codec_version,
			sizeof(output->info.codec_version),
			"%s",
			codec_version);
	output->path = path ? strdup(path) : 0;
	return !path || output->path;
}

static int parse_output_manifest(const char *manifest,
		struct publication_output *outputs, size_t count,
		uint64_t *pull_nanoseconds, uint64_t *decode_nanoseconds,
		uint64_t *function_nanoseconds,
		uint64_t *serialize_nanoseconds,
		uint64_t *fsync_nanoseconds, uint64_t *task_reports,
		uint64_t *task_reported_read_bytes,
		uint64_t *task_reported_cpu_milliseconds)
{
	if (!manifest || strncmp(manifest, VINE_DATAVINE_OUTPUT_MANIFEST_MAGIC "\n", 5))
		return 0;
	char *end = 0;
	unsigned long declared = strtoul(manifest + 5, &end, 10);
	if (!end || *end++ != '\n' || declared != count)
		return 0;
	for (size_t index = 0; index < count; index++) {
		unsigned long long size = strtoull(end, &end, 10);
		if (!end || *end++ != ' ')
			return 0;
		if (strlen(end) < 65 || end[64] != '\n')
			return 0;
		memcpy(outputs[index].info.sha256, end, 64);
		outputs[index].info.sha256[64] = 0;
		unsigned char digest[32];
		if (!digest_bytes(outputs[index].info.sha256, digest))
			return 0;
		outputs[index].info.size = (uint64_t)size;
		end += 65;
	}
	if (!*end)
		return 1;
	unsigned long long decode_ns = 0;
	unsigned long long pull_ns = 0;
	unsigned long long function_ns = 0;
	unsigned long long serialize_ns = 0;
	unsigned long long fsync_ns = 0;
	int consumed = 0;
	char trailing = 0;
	int fields = sscanf(end, "M %llu %llu %llu %llu %llu\n%n", &pull_ns,
			&decode_ns, &function_ns, &serialize_ns, &fsync_ns, &consumed);
	if (fields != 5)
		return 0;
	end += consumed;
	if (*end) {
		unsigned long long read_bytes = 0;
		unsigned long long cpu_ms = 0;
		fields = sscanf(end, "R %llu %llu\n%c", &read_bytes, &cpu_ms,
				&trailing);
		if (fields != 2)
			return 0;
		*task_reports = 1;
		*task_reported_read_bytes = (uint64_t)read_bytes;
		*task_reported_cpu_milliseconds = (uint64_t)cpu_ms;
	}
	*pull_nanoseconds = (uint64_t)pull_ns;
	*decode_nanoseconds = (uint64_t)decode_ns;
	*function_nanoseconds = (uint64_t)function_ns;
	*serialize_nanoseconds = (uint64_t)serialize_ns;
	*fsync_nanoseconds = (uint64_t)fsync_ns;
	return 1;
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

struct vine_datavine_data_publication *
vine_datavine_data_controller_publish_async(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct jx *task, struct jx *executor,
		struct jx *default_codec, struct itable *data,
		struct itable *files, struct itable *consumers, struct itable *requested,
		uint32_t attempt, int retain_all, struct vine_task *completed)
{
	if (!controller || !workflow_id || !task || !data || !files || !consumers ||
			!requested || !attempt)
		return 0;
	struct vine_datavine_data_publication *publication = publication_create();
	struct publication_job *job = calloc(1, sizeof(*job));
	struct jx *output_ids = vine_datavine_ir_task_outputs(task);
	struct jx *output_files = jx_lookup(executor, "output_files");
	const char *version = jx_lookup_string(executor, "version");
	int worker_local = output_files &&
			   !strcmp(jx_lookup_string(executor, "kind"), "python") &&
			   !jx_lookup(executor, "environment") &&
			   (!strcmp(version, VINE_DATAVINE_PYTHON_CALLABLE_VERSION) ||
				!strcmp(version, VINE_DATAVINE_PYTHON_SOURCE_VERSION));
	if (worker_local && !strcmp(version, VINE_DATAVINE_PYTHON_SOURCE_VERSION)) {
		for (int index = 0; index < jx_array_length(output_files); index++) {
			char expected[64];
			snprintf(expected, sizeof(expected),
					"datavine-python-output-%d", index);
			if (strcmp(jx_array_index(output_files, index)->u.string_value,
					expected))
				worker_local = 0;
		}
	}
	const char *plain_output = output_files ? 0 : vine_task_get_stdout(completed);
	size_t plain_output_size = plain_output ? strlen(plain_output) : 0;
	int total = jx_array_length(output_ids);
	if (!publication || !job || total < 1 ||
			(output_files && jx_array_length(output_files) != total)) {
		vine_datavine_data_publication_delete(publication);
		free(job);
		return 0;
	}
	job->workflow_id = strdup(workflow_id);
	job->task_id = (int64_t)vine_datavine_ir_task_id(task);
	job->attempt = attempt;
	job->outputs = calloc((size_t)total, sizeof(*job->outputs));
	job->publication = publication;
	struct publication_output *manifest_outputs = worker_local
									  ? calloc((size_t)total, sizeof(*manifest_outputs))
									  : 0;
	int valid = job->workflow_id && job->outputs &&
			(!worker_local || (manifest_outputs && parse_output_manifest(
									   vine_task_get_stdout(completed), manifest_outputs,
									   (size_t)total, &job->pull_nanoseconds,
									   &job->decode_nanoseconds,
									   &job->function_nanoseconds,
									   &job->serialize_nanoseconds,
									   &job->fsync_nanoseconds,
									   &job->task_reports,
									   &job->task_reported_read_bytes,
									   &job->task_reported_cpu_milliseconds)));
	char directory[PATH_MAX];
	int directory_ready = 0;
	for (int index = 0; valid && index < total; index++) {
		uint64_t data_id = (uint64_t)jx_array_index(
				output_ids, index)
						   ->u.integer_value;
		if (!retained_output(consumers, requested, data_id, retain_all))
			continue;
		char path[PATH_MAX];
		struct publication_output *output = &job->outputs[job->count++];
		int durable_worker_output = worker_local &&
						itable_lookup(requested, data_id) != 0;
		if ((!worker_local || durable_worker_output) && !directory_ready)
			directory_ready = workflow_directory_path(controller, workflow_id, directory);
		valid = ((!worker_local || durable_worker_output)
							? directory_ready && output_path_in_directory(
												 directory, data_id, attempt, path)
							: 1) &&
			fill_output_metadata(output, itable_lookup(data, data_id), default_codec, requested, data_id, attempt, worker_local && !durable_worker_output ? 0 : path);
		if (valid && worker_local) {
			output->info.size = manifest_outputs[index].info.size;
			memcpy(output->info.sha256, manifest_outputs[index].info.sha256, sizeof(output->info.sha256));
			output->direct_durable = durable_worker_output;
			output->remote_file = itable_lookup(files, data_id);
			valid = output->remote_file != 0;
			output->remote_only = !durable_worker_output;
		}
		if (valid && !output_files) {
			if (total != 1 || (!plain_output && plain_output_size)) {
				valid = 0;
				break;
			}
			output->plain_data = malloc(plain_output_size + 1);
			if (!output->plain_data) {
				valid = 0;
				break;
			}
			memcpy(output->plain_data,
					plain_output ? plain_output : "",
					plain_output_size);
			output->plain_size = plain_output_size;
		}
	}
	free(manifest_outputs);
	if (!valid) {
		job_delete(job);
		vine_datavine_data_publication_delete(publication);
		return 0;
	}
	if (!job->count) {
		publication_finish(publication, 1, 0);
		job_delete(job);
		return publication;
	}
	if (!controller->thread_count) {
		clock_gettime(CLOCK_MONOTONIC, &job->commit_started);
		job->metrics.pull_nanoseconds = job->pull_nanoseconds;
		job->metrics.decode_nanoseconds = job->decode_nanoseconds;
		job->metrics.function_nanoseconds = job->function_nanoseconds;
		job->metrics.serialize_nanoseconds = job->serialize_nanoseconds;
		job->metrics.fsync_nanoseconds = job->fsync_nanoseconds;
		job->metrics.task_reports = job->task_reports;
		job->metrics.task_reported_read_bytes = job->task_reported_read_bytes;
		job->metrics.task_reported_cpu_milliseconds =
				job->task_reported_cpu_milliseconds;
		if (!prepare_job(controller, job))
			publication_finish(publication, 0, 0);
		else if (!job->durable_count)
			finish_job(job, 1);
		else
			commit_prepared_jobs(controller, job);
		job_delete(job);
		return publication;
	}
	pthread_mutex_lock(&controller->queue_lock);
	while (!controller->stopping &&
			controller->queued_count >= DATA_QUEUE_LIMIT)
		pthread_cond_wait(&controller->space, &controller->queue_lock);
	if (controller->stopping) {
		pthread_mutex_unlock(&controller->queue_lock);
		job_delete(job);
		vine_datavine_data_publication_delete(publication);
		return 0;
	}
	if (controller->tail)
		controller->tail->next = job;
	else
		controller->head = job;
	controller->tail = job;
	clock_gettime(CLOCK_MONOTONIC, &job->queued_at);
	controller->queued_count++;
	pthread_cond_signal(&controller->queued);
	pthread_mutex_unlock(&controller->queue_lock);
	return publication;
}

int vine_datavine_data_publication_ready(
		struct vine_datavine_data_publication *publication)
{
	if (!publication)
		return 0;
	pthread_mutex_lock(&publication->lock);
	int ready = publication->done;
	pthread_mutex_unlock(&publication->lock);
	return ready;
}

int vine_datavine_data_publication_wait(
		struct vine_datavine_data_publication *publication)
{
	if (!publication)
		return 0;
	pthread_mutex_lock(&publication->lock);
	while (!publication->done)
		pthread_cond_wait(&publication->changed, &publication->lock);
	int successful = publication->successful;
	pthread_mutex_unlock(&publication->lock);
	return successful;
}

int vine_datavine_data_publication_get_metrics(
		struct vine_datavine_data_publication *publication,
		struct vine_datavine_data_publication_metrics *metrics)
{
	if (!publication || !metrics)
		return 0;
	pthread_mutex_lock(&publication->lock);
	int ready = publication->done;
	if (ready)
		*metrics = publication->metrics;
	pthread_mutex_unlock(&publication->lock);
	return ready;
}

void vine_datavine_data_publication_delete(
		struct vine_datavine_data_publication *publication)
{
	if (!publication)
		return;
	pthread_mutex_destroy(&publication->lock);
	pthread_cond_destroy(&publication->changed);
	free(publication);
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
	char *path = result && result->durable ? strdup(result->path) : 0;
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
	if (stored && stored->durable)
		*result = stored->info;
	pthread_mutex_unlock(&controller->lock);
	return stored && stored->durable;
}

int vine_datavine_data_controller_result_path(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		char *path, size_t path_size)
{
	if (!controller || !workflow_id || !data_id || !path || !path_size)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct data_result *stored = result_lookup(controller, workflow_id, data_id);
	int valid = stored && stored->durable && stored->path &&
			strlen(stored->path) + 1 <= path_size;
	if (valid)
		memcpy(path, stored->path, strlen(stored->path) + 1);
	pthread_mutex_unlock(&controller->lock);
	return valid;
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
		valid = stored && stored->durable && stored->path;
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

int vine_datavine_data_controller_result_active(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	if (!controller || !workflow_id || !data_id)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct data_result *result = result_lookup(controller, workflow_id, data_id);
	int active = result && !result->lost;
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
	struct workflow_losses *losses = hash_table_lookup(
			controller->workflow_losses, workflow_id);
	*active = losses ? losses->active_results : 0;
	*peak = losses ? losses->peak_results : 0;
	pthread_mutex_unlock(&controller->lock);
	return 1;
}

struct vine_file *vine_datavine_data_controller_restore_file(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id,
		uint64_t data_id)
{
	if (!controller || !manager || !workflow_id || !data_id)
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct data_result *stored = result_lookup(controller, workflow_id, data_id);
	if (stored && stored->lost)
		stored = 0;
	struct vine_file *remote_file = stored ? stored->remote_file : 0;
	char *path = stored && stored->durable ? strdup(stored->path) : 0;
	pthread_mutex_unlock(&controller->lock);
	struct vine_file *file = remote_file
						 ? remote_file
				 : path
						 ? vine_declare_file(manager, path, VINE_CACHE_LEVEL_WORKFLOW, 0)
						 : 0;
	free(path);
	return file;
}

int vine_datavine_data_controller_release_results(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id,
		struct itable *files,
		const uint64_t *data_ids, size_t count)
{
	if (!controller || !manager || !workflow_id || !workflow_id[0] ||
			!files || (count && !data_ids) || count > UINT16_MAX)
		return 0;
	if (!count)
		return 1;
	uint64_t *drop = malloc(count * sizeof(*drop));
	uint64_t *durable = malloc(count * sizeof(*durable));
	if (!drop || !durable) {
		free(drop);
		free(durable);
		return 0;
	}
	pthread_mutex_lock(&controller->lock);
	size_t drop_count = 0;
	size_t durable_count = 0;
	for (size_t index = 0; index < count; index++) {
		struct data_result *result = result_lookup(controller, workflow_id, data_ids[index]);
		if (!result || result->info.requested)
			continue;
		drop[drop_count++] = data_ids[index];
		if (result->durable)
			durable[durable_count++] = data_ids[index];
	}
	int valid = 1;
	if (durable_count) {
		size_t workflow_size = strlen(workflow_id);
		size_t payload_size = 4 + workflow_size + durable_count * 8;
		unsigned char *payload = payload_size <= UINT32_MAX
							 ? malloc(payload_size)
							 : 0;
		valid = payload && workflow_size <= UINT16_MAX;
		if (valid) {
			vine_datavine_put_u16(payload, (uint16_t)workflow_size);
			vine_datavine_put_u16(payload + 2, (uint16_t)durable_count);
			memcpy(payload + 4, workflow_id, workflow_size);
			for (size_t index = 0; index < durable_count; index++)
				vine_datavine_put_u64(payload + 4 + workflow_size + index * 8,
						durable[index]);
			valid = vine_datavine_journal_commit(controller->journal,
					DATA_RELEASE_BATCH,
					payload,
					payload_size);
		}
		free(payload);
	}
	for (size_t index = 0; valid && index < drop_count; index++) {
		struct data_result *removed = result_remove(controller, workflow_id, drop[index]);
		if (removed && removed->remote_file) {
			if (itable_lookup(files, drop[index]) == removed->remote_file)
				itable_remove(files, drop[index]);
			vine_undeclare_file_no_loss_event(manager, removed->remote_file);
		}
		if (removed && removed->path)
			unlink(removed->path);
		data_result_delete(removed);
	}
	pthread_mutex_unlock(&controller->lock);
	free(durable);
	free(drop);
	return valid;
}

int vine_datavine_data_controller_finish_workflow(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id)
{
	if (!controller || !manager || !workflow_id || !workflow_id[0])
		return 0;
	pthread_mutex_lock(&controller->lock);
	struct itable *results = result_namespace(controller, workflow_id, 0);
	size_t capacity = results ? (size_t)itable_size(results) : 0;
	uint64_t *drop = capacity ? malloc(capacity * sizeof(*drop)) : 0;
	uint64_t *durable_drop = capacity
						 ? malloc(capacity * sizeof(*durable_drop))
						 : 0;
	UINT64_T data_id;
	struct data_result *result;
	int iterator;
	size_t count = 0;
	size_t durable_count = 0;
	int valid = !capacity || (drop && durable_drop);
	if (results) {
		ITABLE_ITERATE(results, iterator, data_id, result)
		{
			if (result->remote_file) {
				const char *cached_name = !result->durable
									  ? vine_file_cached_name(result->remote_file)
									  : 0;
				if (cached_name)
					hash_table_remove(controller->result_files,
							cached_name);
				vine_undeclare_file_no_loss_event(manager, result->remote_file);
				result->remote_file = 0;
			}
			if (valid && !result->info.requested) {
				drop[count++] = result->info.data_id;
				if (result->durable)
					durable_drop[durable_count++] = result->info.data_id;
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
