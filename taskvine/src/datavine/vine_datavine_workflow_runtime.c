/* DataVine native command-workflow execution owner. */

#include "vine_datavine_workflow_store.h"

#include "b64.h"
#include "buffer.h"
#include "itable.h"
#include "jx.h"
#include "jx_parse.h"
#include "taskvine.h"
#include "vine_datavine_data_controller.h"
#include "vine_datavine_ir.h"
#include "vine_datavine_parametric.h"
#include "vine_datavine_scheduler.h"
#include "vine_datavine_protocol.h"

#include <limits.h>
#include <pthread.h>
#include <openssl/sha.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define DATAVINE_WORKFLOW_EVENT_BATCH 128
#define DATAVINE_WORKFLOW_CHECKPOINT_COMPLETIONS 4096
#define DATAVINE_WORKFLOW_INFRASTRUCTURE_ATTEMPTS 64
#define DATAVINE_WORKFLOW_RUNTIME_LANES 2
#define DATAVINE_WORKFLOW_RECOVERY_CACHE_RESULTS 16384

static uint64_t elapsed_nanoseconds(
		const struct timespec *started, const struct timespec *finished)
{
	return (uint64_t)((finished->tv_sec - started->tv_sec) * INT64_C(1000000000) +
			  finished->tv_nsec - started->tv_nsec);
}

struct retained_root {
	struct jx *root;
	struct retained_root *next;
};

struct physical_attempt {
	int64_t logical_id;
	uint32_t attempt;
	int recovery;
	struct parametric_task_view *parametric_view;
};

struct parametric_task_view {
	struct jx *task;
	struct jx *owned_data[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS + 1];
	size_t owned_data_count;
	struct itable *data;
	struct itable *consumers;
	struct itable *requested;
};

struct task_defaults_range {
	uint64_t first_task_id;
	uint64_t last_task_id;
	struct jx *defaults;
	struct jx *default_codec;
};

struct task_defaults_index {
	struct task_defaults_range *ranges;
	size_t count;
	size_t capacity;
};

struct completion_node {
	struct vine_task *task;
	struct completion_node *next;
};

struct pending_publication {
	struct vine_datavine_data_publication *publication;
	struct jx *task;
	int64_t logical_id;
	uint32_t attempt;
	uint32_t maximum_attempts;
	int32_t task_result;
	int logical_finished;
	struct pending_publication *next;
};

struct publication_stage_totals {
	uint64_t prepare_nanoseconds;
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

struct recovery_cache_entry {
	uint64_t data_id;
	uintptr_t token;
};

struct recovery_result_cache {
	struct itable *members;
	struct recovery_cache_entry *entries;
	size_t limit;
	size_t head;
	size_t count;
	size_t peak;
	uint64_t evictions;
	uintptr_t next_token;
};

struct execution_mailbox {
	pthread_mutex_t lock;
	struct completion_node *head;
	struct completion_node *tail;
};

struct execution_resources {
	struct execution_mailbox mailbox;
	int mailbox_initialized;
	struct retained_root *roots;
	struct task_defaults_index task_defaults;
	struct itable *data;
	struct itable *tasks;
	struct itable *files;
	struct itable *consumers;
	struct itable *pending_consumers;
	struct itable *requested;
	struct itable *physical_to_logical;
	/* One immutable ticket per inline/object DataID. */
	struct itable *origin_tickets;
	struct vine_datavine_scheduler *scheduler;
	buffer_t recovered_tasks;
	uint32_t *attempts;
	uint32_t *recovery_marks;
	uint64_t *recovery_queue;
	uint8_t *recovery_pending;
	uint8_t *recovery_active;
	uint64_t *recovery_active_tasks;
	size_t recovery_active_count;
	size_t recovery_head;
	size_t recovery_tail;
	uint32_t recovery_epoch;
	struct recovery_result_cache recovery_cache;
	uint64_t maximum_task_id;
	struct vine_datavine_parametric *parametric;
	struct jx *initial_root;
	uint8_t *parametric_remaining;
};

static int retained_root_add(struct retained_root **roots, struct jx *root)
{
	struct retained_root *retained = root ? calloc(1, sizeof(*retained)) : 0;
	if (!retained) {
		if (root)
			jx_delete(root);
		return 0;
	}
	retained->root = root;
	retained->next = *roots;
	*roots = retained;
	return 1;
}

static void retained_roots_delete(struct retained_root *roots)
{
	while (roots) {
		struct retained_root *next = roots->next;
		jx_delete(roots->root);
		free(roots);
		roots = next;
	}
}

static void parametric_task_view_delete(struct parametric_task_view *view)
{
	if (!view)
		return;
	if (view->task)
		jx_delete(view->task);
	for (size_t index = 0; index < view->owned_data_count; index++)
		jx_delete(view->owned_data[index]);
	if (view->requested)
		itable_delete(view->requested);
	if (view->consumers)
		itable_delete(view->consumers);
	if (view->data)
		itable_delete(view->data);
	free(view);
}

static void physical_attempt_delete(struct physical_attempt *attempt)
{
	if (!attempt)
		return;
	parametric_task_view_delete(attempt->parametric_view);
	free(attempt);
}

static struct jx *parametric_codec(void)
{
	return jx_objectv("name", jx_string("bytes"), "version", jx_string("1"), NULL);
}

static struct jx *parametric_source_record(uint64_t data_id, char *uri)
{
	struct jx *record = uri
			? jx_objectv("data_id", jx_integer((jx_int_t)data_id),
					  "codec", parametric_codec(), "origin",
					  jx_objectv("kind", jx_string("uri"), "uri",
							jx_string(uri), NULL), NULL)
			: 0;
	free(uri);
	return record;
}

static struct jx *parametric_output_record(uint64_t data_id,
		uint64_t task_id)
{
	return jx_objectv("data_id", jx_integer((jx_int_t)data_id),
			"codec", parametric_codec(), "origin",
			jx_objectv("kind", jx_string("output"), "task_id",
					jx_integer((jx_int_t)task_id), "output_index",
					jx_integer(0), NULL), NULL);
}

static struct jx *parametric_generated_record(uint64_t data_id,
		uint64_t producer)
{
	return jx_arrayv(jx_integer((jx_int_t)data_id),
			jx_integer((jx_int_t)producer), jx_integer(0), NULL);
}

static struct parametric_task_view *parametric_task_view_create(
		const struct vine_datavine_parametric *family, struct jx *root,
		uint64_t task_id)
{
	uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
	size_t input_count = 0;
	uint64_t output_data_id = 0;
	enum vine_datavine_parametric_stage stage;
	if (!vine_datavine_parametric_task(family, task_id, &stage, inputs,
			VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS, &input_count,
			&output_data_id))
		return 0;
	struct parametric_task_view *view = calloc(1, sizeof(*view));
	if (!view)
		return 0;
	view->data = itable_create(0);
	view->consumers = itable_create(0);
	view->requested = itable_create(0);
	struct jx *input_array = jx_array(0);
	struct jx *output_array = jx_array(0);
	struct jx *output_files = jx_arrayv(
			jx_string("datavine-python-output-0"), NULL);
	struct jx *executor = jx_objectv("kind", jx_string("python"),
			"version", jx_string(VINE_DATAVINE_PYTHON_SOURCE_VERSION),
			"payload_ref", jx_integer((jx_int_t)stage), "output_files",
			output_files, NULL);
	view->task = jx_objectv("task_id", jx_integer((jx_int_t)task_id),
			"executor", executor, "inputs", input_array,
			"output_data_ids", output_array, "resources",
			jx_objectv("cores", jx_integer(1), NULL), "retry",
			jx_objectv("maximum_attempts", jx_integer(1), NULL), NULL);
	int valid = view->data && view->consumers && view->requested &&
		view->task && input_array && output_array;
	struct jx *payloads = root ? jx_lookup(root, "data") : 0;
	struct jx *payload = payloads ? jx_array_index(payloads, (int)stage - 1) : 0;
	valid = valid && payload &&
		itable_insert(view->data, (uint64_t)stage, payload);
	for (size_t index = 0; valid && index < input_count; index++) {
		jx_array_append(input_array, jx_integer((jx_int_t)inputs[index]));
		uint64_t producer = 0;
		struct jx *record = 0;
		if (vine_datavine_parametric_output_producer(family, inputs[index],
				&producer)) {
			record = parametric_generated_record(inputs[index], producer);
		} else {
			record = parametric_source_record(inputs[index],
					vine_datavine_parametric_source_uri(family, inputs[index]));
			valid = valid && itable_insert(view->consumers, inputs[index],
					(void *)(uintptr_t)1);
		}
		if (record)
			view->owned_data[view->owned_data_count++] = record;
		valid = valid && record && itable_insert(view->data, inputs[index], record);
	}
	struct jx *output_record = valid
			? parametric_output_record(output_data_id, task_id) : 0;
	if (output_record)
		view->owned_data[view->owned_data_count++] = output_record;
	uint32_t consumers = vine_datavine_parametric_output_consumers(
			family, output_data_id);
	valid = valid && output_record &&
		itable_insert(view->data, output_data_id, output_record);
	if (valid && consumers)
		valid = itable_insert(view->consumers, output_data_id,
				(void *)(uintptr_t)consumers);
	if (valid && vine_datavine_parametric_requested(family, output_data_id))
		valid = itable_insert(view->requested, output_data_id, output_record);
	if (valid)
		jx_array_append(output_array, jx_integer((jx_int_t)output_data_id));
	if (!valid) {
		parametric_task_view_delete(view);
		return 0;
	}
	return view;
}

struct vine_datavine_workflow_runtime {
	struct vine_datavine_workflow_store *store;
	struct vine_datavine_data_controller *data_controller;
	struct vine_manager *manager;
	pthread_t lanes[DATAVINE_WORKFLOW_RUNTIME_LANES];
	size_t lane_count;
	pthread_mutex_t manager_lock;
	pthread_mutex_t routing_lock;
	struct itable *completion_owners;
	atomic_int stopping;
	atomic_int lanes_running;
	atomic_int workflows_running;
	atomic_uint_fast64_t worker_connections;
	atomic_uint_fast64_t worker_losses;
	volatile sig_atomic_t *external_stopping;
};

static int runtime_stopping(struct vine_datavine_workflow_runtime *runtime)
{
	return atomic_load(&runtime->stopping) ||
		   (runtime->external_stopping && *runtime->external_stopping);
}

static void manager_lane_lock(struct vine_datavine_workflow_runtime *runtime)
{
	pthread_mutex_lock(&runtime->manager_lock);
}

static struct vine_task *mailbox_take(struct execution_mailbox *mailbox)
{
	if (!mailbox)
		return 0;
	pthread_mutex_lock(&mailbox->lock);
	struct completion_node *node = mailbox->head;
	if (node) {
		mailbox->head = node->next;
		if (!mailbox->head)
			mailbox->tail = 0;
	}
	pthread_mutex_unlock(&mailbox->lock);
	struct vine_task *task = node ? node->task : 0;
	free(node);
	return task;
}

static void mailbox_delete(struct execution_mailbox *mailbox)
{
	struct vine_task *task;
	while ((task = mailbox_take(mailbox)))
		vine_task_delete(task);
	pthread_mutex_destroy(&mailbox->lock);
}

static int mailbox_put(struct execution_mailbox *mailbox,
		struct vine_task *task)
{
	struct completion_node *node = malloc(sizeof(*node));
	if (!node)
		return 0;
	node->task = task;
	node->next = 0;
	pthread_mutex_lock(&mailbox->lock);
	if (mailbox->tail)
		mailbox->tail->next = node;
	else
		mailbox->head = node;
	mailbox->tail = node;
	pthread_mutex_unlock(&mailbox->lock);
	return 1;
}

static struct vine_task *runtime_wait(
		struct vine_datavine_workflow_runtime *runtime,
		struct execution_mailbox *mailbox, int timeout_seconds)
{
	struct timespec started;
	clock_gettime(CLOCK_MONOTONIC, &started);
	for (;;) {
		struct vine_task *task = mailbox_take(mailbox);
		if (task)
			return task;
		if (!timeout_seconds)
			return 0;
		struct timespec now;
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec - started.tv_sec >= timeout_seconds)
			return 0;
		if (runtime_stopping(runtime))
			return 0;
		usleep(1000);
	}
}

static void cancel_running_tasks(struct vine_datavine_workflow_runtime *runtime,
		struct execution_mailbox *mailbox,
		struct itable *physical_to_logical, uint64_t running)
{
	uint64_t physical_id;
	void *logical_value;
	int iterator;
	ITABLE_ITERATE(physical_to_logical, iterator, physical_id, logical_value)
	{
		manager_lane_lock(runtime);
		vine_cancel_by_task_id(runtime->manager, (int)physical_id);
		pthread_mutex_unlock(&runtime->manager_lock);
	}
	while (running) {
		struct vine_task *cancelled = runtime_wait(runtime, mailbox, 1);
		if (!cancelled && runtime_stopping(runtime)) {
			ITABLE_ITERATE(physical_to_logical, iterator, physical_id, logical_value)
			{
				pthread_mutex_lock(&runtime->routing_lock);
				itable_remove(runtime->completion_owners, physical_id);
				pthread_mutex_unlock(&runtime->routing_lock);
				physical_attempt_delete(logical_value);
			}
			return;
		}
		if (!cancelled)
			continue;
		pthread_mutex_lock(&runtime->routing_lock);
		itable_remove(runtime->completion_owners,
				(uint64_t)vine_task_get_id(cancelled));
		pthread_mutex_unlock(&runtime->routing_lock);
		physical_attempt_delete(itable_remove(physical_to_logical,
				(uint64_t)vine_task_get_id(cancelled)));
		vine_task_delete(cancelled);
		running--;
	}
}

/* Submit exactly one logical task as exactly one TaskVine task.  The caller
 * owns manager_lock so no other workflow lane can interleave Manager calls. */
static int runtime_submit_locked(struct vine_datavine_workflow_runtime *runtime,
		struct vine_task *task, struct execution_mailbox *mailbox)
{
	int task_id = vine_submit(runtime->manager, task);
	if (task_id > 0) {
		pthread_mutex_lock(&runtime->routing_lock);
		if (!itable_insert(runtime->completion_owners, (uint64_t)task_id, mailbox)) {
			/* The Manager already accepted the task. Returning a rejection would
			 * orphan its completion, so fail-stop and let journal recovery retry. */
			abort();
		}
		pthread_mutex_unlock(&runtime->routing_lock);
	}
	return task_id;
}

static void put_little_u64(unsigned char *buffer, uint64_t value)
{
	for (int i = 0; i < 8; i++)
		buffer[i] = (unsigned char)(value >> (8 * i));
}

static uint64_t get_little_u64(const unsigned char *buffer)
{
	uint64_t value = 0;
	for (int i = 0; i < 8; i++)
		value |= (uint64_t)buffer[i] << (8 * i);
	return value;
}

static struct vine_task *create_native_ticket(int64_t task_id,
		uint64_t generation, uint64_t configuration)
{
	if (task_id < 1 || !generation)
		return 0;
	unsigned char ticket[32] = {'D', 'V', 'T', '1'};
	vine_datavine_put_u64(ticket + 8, (uint64_t)task_id);
	vine_datavine_put_u64(ticket + 16, generation);
	vine_datavine_put_u64(ticket + 24, configuration);
	char tag[32];
	int length = snprintf(tag, sizeof(tag), "%lld", (long long)task_id);
	if (length < 1 || (size_t)length >= sizeof(tag))
		return 0;
	struct vine_task *task = vine_task_create("execute_datavine_task_ticket");
	if (!task)
		return 0;
	vine_task_set_library_required(task, "datavine-native-v1");
	vine_task_set_function_input(task, (const char *)ticket, sizeof(ticket));
	vine_task_set_tag(task, tag);
	vine_task_set_category(task, "datavine-compute");
	vine_task_set_cores(task, 1);
	vine_task_set_retries(task, 0);
	return task;
}

static struct vine_task *create_python_ticket(struct jx *task,
		struct jx *executor, struct jx *resources, struct itable *data,
		struct itable *consumers,
		struct itable *requested, int retain_all,
		struct vine_datavine_data_controller *data_controller,
		const char *workflow_id, uint32_t attempt)
{
	uint64_t payload_id = (uint64_t)jx_lookup_integer(executor, "payload_ref");
	if (!payload_id)
		return 0;
	uint64_t wall_seconds = 0;
	struct jx *wall_time = resources
						   ? jx_lookup(resources, "wall_time_seconds")
						   : 0;
	if (wall_time)
		wall_seconds = (uint64_t)wall_time->u.integer_value;
	unsigned char source_ticket[24] = VINE_DATAVINE_PYTHON_TICKET_MAGIC;
	unsigned char *ticket = source_ticket;
	size_t ticket_size = sizeof(source_ticket);
	const char *function_name = "datavine_python_source";
	vine_datavine_put_u64(ticket + 8, payload_id);
	vine_datavine_put_u64(ticket + 16, wall_seconds);
	const char *version = jx_lookup_string(executor, "version");
	if (!strcmp(version, VINE_DATAVINE_PYTHON_SOURCE_VERSION)) {
		struct jx *outputs = vine_datavine_ir_task_outputs(task);
		struct jx *output_files = jx_lookup(executor, "output_files");
		size_t output_count = (size_t)jx_array_length(outputs);
		int indexed_outputs = output_files &&
			jx_array_length(output_files) == (int)output_count;
		for (size_t index = 0; indexed_outputs && index < output_count; index++) {
			char expected[64];
			snprintf(expected, sizeof(expected), "datavine-python-output-%zu", index);
			indexed_outputs = !strcmp(
				jx_array_index(output_files, (int)index)->u.string_value,
				expected);
		}
		/* The extended source ticket enables the same Worker-local output and
		 * direct-durability path as callable-v1. Keep the original 24-byte
		 * ticket for custom output names so existing source executors remain
		 * wire compatible. */
		if (indexed_outputs) {
			const size_t fixed_size = 64;
			if (!output_count || output_count > UINT32_MAX ||
					output_count > (SIZE_MAX - fixed_size) / 10)
				return 0;
			ticket_size = fixed_size + output_count * 10;
			ticket = calloc(1, ticket_size);
			if (!ticket)
				return 0;
			memcpy(ticket, VINE_DATAVINE_PYTHON_TICKET_MAGIC, 4);
			vine_datavine_put_u64(ticket + 8, payload_id);
			vine_datavine_put_u64(ticket + 16, wall_seconds);
			vine_datavine_put_u32(ticket + 24, (uint32_t)output_count);
			vine_datavine_put_u32(ticket + 28, attempt);
			if (!vine_datavine_data_controller_workflow_key(
					data_controller, workflow_id, ticket + 32)) {
				free(ticket);
				return 0;
			}
			for (size_t index = 0; index < output_count; index++) {
				uint64_t data_id = (uint64_t)jx_array_index(outputs, (int)index)
							   ->u.integer_value;
				ticket[fixed_size + index] = retain_all ||
					itable_lookup(consumers, data_id) ||
					itable_lookup(requested, data_id);
				/* Runtime v2 persistence is owned by the Worker Data Agent. */
				ticket[fixed_size + output_count + index] = 0;
				vine_datavine_put_u64(
					ticket + fixed_size + output_count * 2 + index * 8,
					data_id);
			}
		}
	} else if (!strcmp(version, VINE_DATAVINE_PYTHON_CALLABLE_VERSION)) {
		uint64_t function_ref = (uint64_t)jx_lookup_integer(executor, "function_ref");
		const char *digest = jx_lookup_string(executor, "function_digest");
		struct jx *payload_record = itable_lookup(data, payload_id);
		struct jx *function_record = itable_lookup(data, function_ref);
		struct jx *payload_origin = payload_record ? jx_lookup(payload_record, "origin") : 0;
		struct jx *function_origin = function_record ? jx_lookup(function_record, "origin") : 0;
		const char *invocation_digest = payload_origin ? jx_lookup_string(payload_origin, "sha256") : 0;
		const char *stored_function_digest = function_origin ? jx_lookup_string(function_origin, "sha256") : 0;
		if (!function_ref || !digest || strlen(digest) != 64 ||
				!payload_origin || !function_origin ||
				strcmp(jx_lookup_string(payload_origin, "kind"), "object") ||
				strcmp(jx_lookup_string(function_origin, "kind"), "object") ||
				!invocation_digest || !stored_function_digest ||
				strcmp(digest, stored_function_digest))
			return 0;
		struct jx *outputs = vine_datavine_ir_task_outputs(task);
		size_t output_count = (size_t)jx_array_length(outputs);
		const size_t fixed_size = 120;
		if (!output_count || output_count > UINT32_MAX ||
				output_count > (SIZE_MAX - fixed_size) / 10)
			return 0;
		ticket_size = fixed_size + output_count * 10;
		ticket = calloc(1, ticket_size);
		if (!ticket)
			return 0;
		memcpy(ticket, VINE_DATAVINE_PYTHON_TICKET_MAGIC, 4);
		vine_datavine_put_u64(ticket + 8, wall_seconds);
		vine_datavine_put_u32(ticket + 16, (uint32_t)output_count);
		vine_datavine_put_u32(ticket + 20, attempt);
		if (!vine_datavine_data_controller_workflow_key(data_controller,
					workflow_id,
					ticket + 24)) {
			free(ticket);
			return 0;
		}
		for (size_t index = 0; index < 32; index++) {
			unsigned int value = 0;
			if (sscanf(invocation_digest + index * 2, "%2x", &value) != 1) {
				free(ticket);
				return 0;
			}
			ticket[56 + index] = (unsigned char)value;
		}
		for (size_t index = 0; index < 32; index++) {
			unsigned int value = 0;
			if (sscanf(digest + index * 2, "%2x", &value) != 1) {
				free(ticket);
				return 0;
			}
			ticket[88 + index] = (unsigned char)value;
		}
		for (size_t index = 0; index < output_count; index++) {
			uint64_t data_id = (uint64_t)jx_array_index(outputs, (int)index)
							   ->u.integer_value;
			ticket[fixed_size + index] = retain_all ||
							 itable_lookup(consumers, data_id) || itable_lookup(requested, data_id);
			ticket[fixed_size + output_count + index] = 0;
			vine_datavine_put_u64(ticket + fixed_size + output_count * 2 +
								  index * 8,
					data_id);
		}
		function_name = "datavine_python_callable";
	}
	struct vine_task *physical = vine_task_create(function_name);
	if (!physical) {
		if (ticket != source_ticket)
			free(ticket);
		return 0;
	}
	vine_task_set_library_required(physical, "datavine-python-v1");
	vine_task_set_function_input(physical, (const char *)ticket, ticket_size);
	if (ticket != source_ticket)
		free(ticket);
	return physical;
}

static int shell_word(buffer_t *command, const char *value)
{
	if (buffer_putliteral(command, "'") < 0)
		return 0;
	for (const char *cursor = value; *cursor; cursor++) {
		if (*cursor == '\'') {
			if (buffer_putliteral(command, "'\\''") < 0)
				return 0;
		} else if (buffer_putlstring(command, cursor, 1) < 0) {
			return 0;
		}
	}
	return buffer_putliteral(command, "'") >= 0;
}

static struct jx *data_record(struct itable *data, uint64_t data_id)
{
	return itable_lookup(data, data_id);
}

static int prepare_origin(struct vine_datavine_data_controller *data_controller,
		struct vine_manager *manager, uint64_t data_id, struct jx *record,
		struct jx *default_codec, struct itable *files)
{
	if (itable_lookup(files, data_id))
		return 1;
	if (vine_datavine_ir_data_is_output(record))
		return 1;
	struct jx *origin = vine_datavine_ir_data_origin(record);
	const char *kind = jx_lookup_string(origin, "kind");
	struct jx *codec = vine_datavine_ir_data_codec(record, default_codec);
	const char *codec_name = codec ? jx_lookup_string(codec, "name") : 0;
	if (!strcmp(kind, "object") && codec_name &&
			(!strcmp(codec_name, "python/callable") ||
					!strcmp(codec_name, "python/invocation")))
		return 1;
	struct vine_file *file = 0;
	if (!strcmp(kind, "inline")) {
		buffer_t decoded;
		buffer_init(&decoded);
		if (b64_decode(jx_lookup_string(origin, "base64"), &decoded) == 0) {
			size_t size = 0;
			const char *bytes = buffer_tolstring(&decoded, &size);
			file = vine_declare_buffer(manager, bytes, size, VINE_CACHE_LEVEL_WORKFLOW, 0);
		}
		buffer_free(&decoded);
	} else if (!strcmp(kind, "uri")) {
		file = vine_declare_url(manager, jx_lookup_string(origin, "uri"), VINE_CACHE_LEVEL_WORKFLOW, 0);
	} else if (!strcmp(kind, "object")) {
		file = vine_datavine_data_controller_resolve_object(
				data_controller, manager, jx_lookup_string(origin, "sha256"));
	}
	return file && itable_insert(files, data_id, file);
}

static int __attribute__((unused)) prepare_origins(
		struct vine_datavine_data_controller *data_controller,
		struct vine_manager *manager, struct itable *data,
		struct jx *default_codec, struct itable *files)
{
	UINT64_T data_id;
	void *value;
	int iterator;
	ITABLE_ITERATE(data, iterator, data_id, value)
	{
		if (!prepare_origin(data_controller, manager, data_id, value, default_codec, files))
			return 0;
	}
	return 1;
}

static int __attribute__((unused)) prepare_delta_inputs(struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct jx *root, struct itable *data,
		struct itable *files)
{
	struct jx *record;
	struct jx *data_defaults = jx_lookup(root, "data_defaults");
	struct jx *default_codec = data_defaults
						   ? jx_lookup(data_defaults, "codec")
						   : 0;
	void *iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "data"), &iterator))) {
		uint64_t data_id = vine_datavine_ir_data_id(record);
		struct jx *origin = vine_datavine_ir_data_origin(record);
		if (!origin)
			continue;
		const char *kind = jx_lookup_string(origin, "kind");
		if (!strcmp(kind, "inline") || !strcmp(kind, "uri") ||
				!strcmp(kind, "object")) {
			manager_lane_lock(runtime);
			int prepared = prepare_origin(runtime->data_controller,
					runtime->manager,
					data_id,
					record,
					default_codec,
					files);
			pthread_mutex_unlock(&runtime->manager_lock);
			if (!prepared)
				return 0;
		}
	}
	iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "tasks"), &iterator))) {
		struct jx *input;
		void *input_iterator = 0;
		while ((input = jx_iterate_array(vine_datavine_ir_task_inputs(record),
					&input_iterator))) {
			uint64_t data_id = vine_datavine_ir_input_data_id(input);
			if (itable_lookup(files, data_id))
				continue;
			struct jx *data_item = data_record(data, data_id);
			if (!data_item || !vine_datavine_ir_data_is_output(data_item))
				continue;
			manager_lane_lock(runtime);
			struct vine_file *file =
					vine_datavine_data_controller_restore_file(
							runtime->data_controller, runtime->manager, workflow_id, data_id);
			if (!file) {
				char *bytes = 0;
				size_t size = 0;
				if (vine_datavine_workflow_store_legacy_fetch_result(runtime->store,
							workflow_id,
							data_id,
							&bytes,
							&size)) {
					file = vine_declare_buffer(runtime->manager, bytes, size, VINE_CACHE_LEVEL_WORKFLOW, 0);
					free(bytes);
				}
			}
			int inserted = file && itable_insert(files, data_id, file);
			pthread_mutex_unlock(&runtime->manager_lock);
			if (!inserted)
				continue;
		}
	}
	return 1;
}

static int __attribute__((unused)) restore_outputs(struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct itable *data, struct itable *files,
		struct itable *consumers)
{
	UINT64_T data_id;
	void *value;
	int iterator;
	int valid = 1;
	ITABLE_ITERATE(data, iterator, data_id, value)
	{
		if (itable_lookup(files, data_id))
			continue;
		if (!vine_datavine_ir_data_is_output(value))
			continue;
		struct vine_datavine_workflow_result_info result_info;
		if (!vine_datavine_data_controller_result_active(
					runtime->data_controller, workflow_id, data_id) &&
				!vine_datavine_workflow_store_legacy_result_info(runtime->store,
						workflow_id,
						data_id,
						&result_info))
			continue;
		if (itable_lookup(consumers, data_id)) {
			struct vine_file *file =
					vine_datavine_data_controller_restore_file(
							runtime->data_controller,
							runtime->manager,
							workflow_id,
							data_id);
			if (!file) {
				char *bytes = 0;
				size_t size = 0;
				if (!vine_datavine_workflow_store_legacy_fetch_result(runtime->store,
							workflow_id,
							data_id,
							&bytes,
							&size)) {
					valid = 0;
					break;
				}
				file = vine_declare_buffer(runtime->manager, bytes, size, VINE_CACHE_LEVEL_WORKFLOW, 0);
				free(bytes);
			}
			if (!file || !itable_insert(files, data_id, file)) {
				valid = 0;
				break;
			}
		}
	}
	return valid;
}

static int output_available(struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, uint64_t data_id)
{
	struct vine_datavine_workflow_result_info info;
	return vine_datavine_data_controller_result_active(
				   runtime->data_controller, workflow_id, data_id) ||
		   vine_datavine_workflow_store_legacy_result_info(
				   runtime->store, workflow_id, data_id, &info);
}

static int requested_results_ready(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct itable *requested)
{
	uint64_t data_id;
	void *value;
	int iterator;
	ITABLE_ITERATE(requested, iterator, data_id, value)
	{
		if (!vine_datavine_data_controller_result_active(
				runtime->data_controller, workflow_id, data_id))
			return 0;
	}
	return 1;
}

static int pending_consumer_increment(struct itable *pending, uint64_t data_id)
{
	uintptr_t count = (uintptr_t)itable_lookup(pending, data_id);
	return count < UINTPTR_MAX &&
		   itable_insert(pending, data_id, (void *)(count + 1));
}

static int pending_consumer_decrement(struct itable *pending, uint64_t data_id)
{
	uintptr_t count = (uintptr_t)itable_lookup(pending, data_id);
	if (!count)
		return 0;
	if (count == 1) {
		itable_remove(pending, data_id);
		return 1;
	}
	return itable_insert(pending, data_id, (void *)(count - 1));
}

static int adjust_task_pending_inputs(
		struct jx *task, struct itable *pending, int increment)
{
	struct jx *input;
	void *iterator = 0;
	while ((input = jx_iterate_array(vine_datavine_ir_task_inputs(task), &iterator))) {
		uint64_t data_id = vine_datavine_ir_input_data_id(input);
		int adjusted;
		if (increment)
			adjusted = pending_consumer_increment(pending, data_id);
		else
			adjusted = pending_consumer_decrement(pending, data_id);
		if (!adjusted)
			return 0;
	}
	return 1;
}

static int output_producer(struct itable *data, uint64_t data_id,
		uint64_t *task_id)
{
	struct jx *record = data_record(data, data_id);
	if (!record || !vine_datavine_ir_data_is_output(record))
		return 0;
	int64_t producer = vine_datavine_ir_data_producer(record);
	if (producer < 1)
		return 0;
	*task_id = (uint64_t)producer;
	return 1;
}

static int restore_completed_tasks(struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, buffer_t *completed_tasks)
{
	int64_t *task_ids = 0;
	size_t count = 0;
	if (!vine_datavine_workflow_store_completed_task_ids(runtime->store,
				workflow_id,
				&task_ids,
				&count))
		return 0;
	for (size_t index = 0; index < count; index++) {
		unsigned char encoded[8];
		put_little_u64(encoded, (uint64_t)task_ids[index]);
		if (buffer_putlstring(completed_tasks, (const char *)encoded, sizeof(encoded)) < 0) {
			free(task_ids);
			return 0;
		}
	}
	free(task_ids);
	return 1;
}

static int prepare_recovered_tasks(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct itable *data, struct itable *tasks,
		struct itable *pending_consumers, struct itable *requested,
		uint64_t maximum_task_id, buffer_t *completed_tasks,
		uint64_t **invalidated_tasks, size_t *invalidated_count)
{
	*invalidated_tasks = 0;
	*invalidated_count = 0;
	size_t size = 0;
	const unsigned char *encoded = (const unsigned char *)buffer_tolstring(
			completed_tasks, &size);
	if (size % 8)
		return 0;
	unsigned char *completed = calloc((size_t)maximum_task_id + 1, 1);
	unsigned char *queued = calloc((size_t)maximum_task_id + 1, 1);
	uint64_t *queue = calloc((size_t)maximum_task_id + 1, sizeof(*queue));
	int valid = completed && queued && queue;
	for (size_t offset = 0; valid && offset < size; offset += 8) {
		uint64_t task_id = get_little_u64(encoded + offset);
		valid = task_id <= maximum_task_id && itable_lookup(tasks, task_id) &&
			!completed[task_id];
		if (valid)
			completed[task_id] = 1;
	}
	uint64_t task_id;
	void *task_value;
	int task_iterator;
	ITABLE_ITERATE(tasks, task_iterator, task_id, task_value)
	{
		if (valid && !completed[task_id])
			valid = adjust_task_pending_inputs(task_value, pending_consumers, 1);
	}
	size_t head = 0;
	size_t tail = 0;
	ITABLE_ITERATE(tasks, task_iterator, task_id, task_value)
	{
		if (!valid || !completed[task_id])
			continue;
		struct jx *outputs = vine_datavine_ir_task_outputs(task_value);
		for (int index = 0; index < jx_array_length(outputs); index++) {
			uint64_t data_id = (uint64_t)jx_array_index(outputs, index)->u.integer_value;
			if ((itable_lookup(pending_consumers, data_id) ||
						itable_lookup(requested, data_id)) &&
					!output_available(runtime, workflow_id, data_id)) {
				queue[tail++] = task_id;
				queued[task_id] = 1;
				break;
			}
		}
	}
	while (valid && head < tail) {
		task_id = queue[head++];
		if (!completed[task_id])
			continue;
		completed[task_id] = 0;
		struct jx *task = itable_lookup(tasks, task_id);
		valid = task && adjust_task_pending_inputs(task, pending_consumers, 1);
		struct jx *input;
		void *iterator = 0;
		while (valid && (input = jx_iterate_array(vine_datavine_ir_task_inputs(task),
						 &iterator))) {
			uint64_t data_id = vine_datavine_ir_input_data_id(input);
			uint64_t producer = 0;
			if (output_producer(data, data_id, &producer) &&
					completed[producer] && !queued[producer] &&
					!output_available(runtime, workflow_id, data_id)) {
				queue[tail++] = producer;
				queued[producer] = 1;
			}
		}
	}
	buffer_rewind(completed_tasks, 0);
	ITABLE_ITERATE(tasks, task_iterator, task_id, task_value)
	{
		if (valid && completed[task_id]) {
			unsigned char bytes[8];
			put_little_u64(bytes, task_id);
			valid = buffer_putlstring(completed_tasks,
						(const char *)bytes,
						sizeof(bytes)) >= 0;
		}
	}
	if (valid && tail) {
		*invalidated_tasks = queue;
		*invalidated_count = tail;
		queue = 0;
	}
	free(queue);
	free(queued);
	free(completed);
	return valid;
}

static int parametric_task_inputs(
		const struct vine_datavine_parametric *family, uint64_t task_id,
		uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS],
		size_t *input_count, uint64_t *output)
{
	enum vine_datavine_parametric_stage stage;
	return vine_datavine_parametric_task(family, task_id, &stage, inputs,
			VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS, input_count, output);
}

static int parametric_output_needed(struct execution_resources *resources,
		uint64_t producer, uint64_t data_id)
{
	return (producer <= resources->maximum_task_id &&
		resources->parametric_remaining[producer]) ||
		vine_datavine_parametric_requested(resources->parametric, data_id);
}

static int prepare_recovered_parametric(
		struct vine_datavine_workflow_runtime *runtime, const char *workflow_id,
		struct execution_resources *resources, uint64_t **invalidated_tasks,
		size_t *invalidated_count)
{
	*invalidated_tasks = 0;
	*invalidated_count = 0;
	size_t encoded_size = 0;
	const unsigned char *encoded = (const unsigned char *)buffer_tolstring(
			&resources->recovered_tasks, &encoded_size);
	if (encoded_size % 8)
		return 0;
	unsigned char *completed = calloc((size_t)resources->maximum_task_id + 1, 1);
	unsigned char *queued = calloc((size_t)resources->maximum_task_id + 1, 1);
	uint64_t *queue = malloc(((size_t)resources->maximum_task_id + 1) *
			sizeof(*queue));
	int valid = completed && queued && queue;
	for (size_t offset = 0; valid && offset < encoded_size; offset += 8) {
		uint64_t task_id = get_little_u64(encoded + offset);
		valid = task_id > 0 && task_id <= resources->maximum_task_id &&
				!completed[task_id];
		if (valid)
			completed[task_id] = 1;
	}
	uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
	for (uint64_t task_id = 1; valid && task_id <= resources->maximum_task_id;
			task_id++) {
		if (!completed[task_id])
			continue;
		size_t input_count = 0;
		uint64_t output = 0;
		valid = parametric_task_inputs(resources->parametric, task_id, inputs,
				&input_count, &output);
		for (size_t index = 0; valid && index < input_count; index++) {
			uint64_t producer = 0;
			if (!vine_datavine_parametric_output_producer(resources->parametric,
					inputs[index], &producer))
				continue;
			valid = resources->parametric_remaining[producer] > 0;
			if (valid)
				resources->parametric_remaining[producer]--;
		}
	}
	size_t head = 0;
	size_t tail = 0;
	for (uint64_t task_id = 1; valid && task_id <= resources->maximum_task_id;
			task_id++) {
		if (!completed[task_id])
			continue;
		size_t input_count = 0;
		uint64_t output = 0;
		valid = parametric_task_inputs(resources->parametric, task_id, inputs,
				&input_count, &output);
		if (valid && parametric_output_needed(resources, task_id, output) &&
				!output_available(runtime, workflow_id, output)) {
			queue[tail++] = task_id;
			queued[task_id] = 1;
		}
	}
	while (valid && head < tail) {
		uint64_t task_id = queue[head++];
		size_t input_count = 0;
		uint64_t output = 0;
		valid = parametric_task_inputs(resources->parametric, task_id, inputs,
				&input_count, &output);
		for (size_t index = 0; valid && index < input_count; index++) {
			uint64_t producer = 0;
			if (vine_datavine_parametric_output_producer(resources->parametric,
					inputs[index], &producer) && completed[producer] &&
					!queued[producer] &&
					!output_available(runtime, workflow_id, inputs[index])) {
				queue[tail++] = producer;
				queued[producer] = 1;
			}
		}
	}
	if (valid && tail) {
		*invalidated_tasks = queue;
		*invalidated_count = tail;
		queue = 0;
	}
	free(queue);
	free(queued);
	free(completed);
	return valid;
}

static int apply_parametric_losses(
		struct vine_datavine_workflow_runtime *runtime, const char *workflow_id,
		const uint64_t *lost_data_ids, size_t lost_count,
		struct execution_resources *resources, size_t *invalidated_first,
		size_t *invalidated_count)
{
	*invalidated_first = resources->recovery_tail;
	*invalidated_count = 0;
	uint32_t mark = ++resources->recovery_epoch;
	if (!mark) {
		memset(resources->recovery_marks, 0,
				((size_t)resources->maximum_task_id + 1) *
					sizeof(*resources->recovery_marks));
		mark = ++resources->recovery_epoch;
	}
	size_t head = resources->recovery_tail;
	size_t tail = resources->recovery_tail;
	for (size_t index = 0; index < lost_count; index++) {
		uint64_t producer = 0;
		uint64_t data_id = lost_data_ids[index];
		itable_remove(resources->recovery_cache.members, data_id);
		if (vine_datavine_parametric_output_producer(resources->parametric,
				data_id, &producer) &&
				parametric_output_needed(resources, producer, data_id) &&
				resources->recovery_marks[producer] != mark &&
				!resources->recovery_pending[producer] &&
				vine_datavine_scheduler_task_state(resources->scheduler,
						(int64_t)producer) == VINE_DATAVINE_TASK_DONE) {
			resources->recovery_queue[tail++] = producer;
			resources->recovery_marks[producer] = mark;
			resources->recovery_pending[producer] = 1;
		}
	}
	uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
	int valid = 1;
	while (valid && head < tail) {
		uint64_t task_id = resources->recovery_queue[head++];
		size_t input_count = 0;
		uint64_t output = 0;
		valid = parametric_task_inputs(resources->parametric, task_id, inputs,
				&input_count, &output);
		for (size_t index = 0; valid && index < input_count; index++) {
			uint64_t producer = 0;
			if (vine_datavine_parametric_output_producer(resources->parametric,
					inputs[index], &producer) &&
					resources->recovery_marks[producer] != mark &&
					!resources->recovery_pending[producer] &&
					!output_available(runtime, workflow_id, inputs[index]) &&
					vine_datavine_scheduler_task_state(resources->scheduler,
							(int64_t)producer) == VINE_DATAVINE_TASK_DONE) {
				resources->recovery_queue[tail++] = producer;
				resources->recovery_marks[producer] = mark;
				resources->recovery_pending[producer] = 1;
			}
		}
	}
	if (valid) {
		/* The traversal discovers consumers before their missing ancestors.
		 * Submit this new slice in reverse so producers enter recovery slots
		 * before tasks that would only wait for those producers' data. */
		for (size_t left = *invalidated_first, right = tail;
				left < right;) {
			right--;
			if (left >= right)
				break;
			uint64_t swap = resources->recovery_queue[left];
			resources->recovery_queue[left] = resources->recovery_queue[right];
			resources->recovery_queue[right] = swap;
			left++;
		}
		resources->recovery_tail = tail;
		*invalidated_count = tail - *invalidated_first;
	}
	return valid;
}

static int parametric_recovery_begin(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct execution_resources *resources,
		uint64_t task_id)
{
	if (!runtime || !workflow_id || !resources || !task_id ||
			task_id > resources->maximum_task_id)
		return 0;
	if (resources->recovery_active[task_id])
		return 1;
	uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
	size_t input_count = 0;
	uint64_t output = 0;
	if (!parametric_task_inputs(resources->parametric, task_id, inputs,
			&input_count, &output) ||
			!vine_datavine_data_controller_agent_set_recovery(
				runtime->data_controller, workflow_id, output, 1))
		return 0;
	resources->recovery_active[task_id] = 1;
	resources->recovery_active_tasks[resources->recovery_active_count++] =
			task_id;
	return 1;
}

static int parametric_recovery_finish(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct execution_resources *resources)
{
	unsigned char workflow_key[32];
	if (!vine_datavine_data_controller_workflow_key(
			runtime->data_controller, workflow_id, workflow_key))
		return 0;
	uint64_t workflow_slot = vine_datavine_get_u64(workflow_key);
	if (!workflow_slot)
		workflow_slot = 1;
	int valid = 1;
	for (size_t index = 0; valid && index < resources->recovery_active_count;
			index++) {
		uint64_t task_id = resources->recovery_active_tasks[index];
		uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
		size_t input_count = 0;
		uint64_t output = 0;
		valid = task_id && task_id <= resources->maximum_task_id &&
				resources->recovery_active[task_id] &&
				!resources->recovery_pending[task_id] &&
				parametric_task_inputs(resources->parametric, task_id, inputs,
					&input_count, &output);
		if (valid && parametric_output_needed(resources, task_id, output))
			valid = vine_datavine_data_controller_agent_set_recovery(
					runtime->data_controller, workflow_id, output, 0);
		else if (valid)
			valid = vine_datavine_data_controller_agent_mark_dead(
					runtime->data_controller, workflow_slot, output, 0);
		if (valid)
			resources->recovery_active[task_id] = 0;
	}
	if (valid)
		resources->recovery_active_count = 0;
	return valid;
}

static int requested_parametric_results_ready(
		struct vine_datavine_workflow_runtime *runtime, const char *workflow_id,
		const struct vine_datavine_parametric *family)
{
	for (uint64_t index = 0; index < family->c_tasks; index++)
		if (!vine_datavine_data_controller_result_active(runtime->data_controller,
				workflow_id, family->c_data_first + index))
			return 0;
	return 1;
}

static int record_recovery_invalidations(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, const uint64_t *task_ids, size_t count,
		const uint32_t *attempts, uint64_t *event_nanoseconds)
{
	if (!count)
		return 1;
	struct vine_datavine_workflow_task_event_record *records =
			calloc(VINE_DATAVINE_WORKFLOW_EVENT_BATCH_MAX, sizeof(*records));
	if (count && !records)
		return 0;
	int valid = 1;
	for (size_t offset = 0; valid && offset < count;) {
		size_t batch = count - offset;
		if (batch > VINE_DATAVINE_WORKFLOW_EVENT_BATCH_MAX)
			batch = VINE_DATAVINE_WORKFLOW_EVENT_BATCH_MAX;
		for (size_t index = 0; index < batch; index++) {
			uint64_t task_id = task_ids[offset + index];
			records[index] = (struct vine_datavine_workflow_task_event_record){
					.type = VINE_DATAVINE_WORKFLOW_TASK_RETRY,
					.task_id = (int64_t)task_id,
					.attempt = attempts[task_id],
					.result = -(int32_t)VINE_RESULT_OUTPUT_MISSING,
			};
			valid = records[index].attempt > 0;
			if (!valid)
				break;
		}
		struct timespec started;
		struct timespec finished;
		clock_gettime(CLOCK_MONOTONIC, &started);
		if (valid) {
			struct vine_datavine_workflow_error error;
			valid = vine_datavine_workflow_store_record_task_events(
					runtime->store, workflow_id, records, batch, &error);
		}
		clock_gettime(CLOCK_MONOTONIC, &finished);
		*event_nanoseconds += elapsed_nanoseconds(&started, &finished);
		offset += batch;
	}
	free(records);
	return valid;
}

static int apply_workflow_losses(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, const uint64_t *lost_data_ids,
		size_t lost_count, struct itable *data, struct itable *tasks,
		struct itable *pending_consumers,
		struct itable *requested, struct recovery_result_cache *recovery_cache,
		struct vine_datavine_scheduler *scheduler,
		const uint32_t *attempts, uint64_t maximum_task_id,
		uint32_t *recovery_marks, uint64_t *recovery_queue,
		uint32_t *recovery_epoch,
		uint64_t *event_nanoseconds, size_t *invalidated_count)
{
	*invalidated_count = 0;
	if (!recovery_marks || !recovery_queue || !recovery_epoch)
		return 0;
	uint32_t mark = ++*recovery_epoch;
	if (!mark) {
		memset(recovery_marks, 0, ((size_t)maximum_task_id + 1) * sizeof(*recovery_marks));
		mark = ++*recovery_epoch;
	}
	size_t head = 0;
	size_t tail = 0;
	for (size_t index = 0; index < lost_count; index++) {
		uint64_t data_id = lost_data_ids[index];
		/* The Controller removed the dead handle only if it had not already
		 * been superseded by a newer attempt for this DataID. */
		itable_remove(recovery_cache->members, data_id);
		if (!itable_lookup(pending_consumers, data_id) &&
				!itable_lookup(requested, data_id))
			continue;
		uint64_t producer = 0;
		if (output_producer(data, data_id, &producer) &&
				producer <= maximum_task_id && recovery_marks[producer] != mark &&
				vine_datavine_scheduler_task_state(scheduler, (int64_t)producer) ==
						VINE_DATAVINE_TASK_DONE) {
			recovery_queue[tail++] = producer;
			recovery_marks[producer] = mark;
		}
	}
	while (head < tail) {
		uint64_t task_id = recovery_queue[head++];
		struct jx *task = itable_lookup(tasks, task_id);
		if (!task)
			return 0;
		struct jx *input;
		void *iterator = 0;
		while ((input = jx_iterate_array(vine_datavine_ir_task_inputs(task), &iterator))) {
			uint64_t data_id = vine_datavine_ir_input_data_id(input);
			uint64_t producer = 0;
			if (output_producer(data, data_id, &producer) &&
					producer <= maximum_task_id &&
					recovery_marks[producer] != mark &&
					!output_available(runtime, workflow_id, data_id) &&
					vine_datavine_scheduler_task_state(scheduler,
							(int64_t)producer) == VINE_DATAVINE_TASK_DONE) {
				recovery_queue[tail++] = producer;
				recovery_marks[producer] = mark;
			}
		}
	}
	/* Logical DONE is immutable: data recovery is a separate physical replay.
	 * The Scheduler is deliberately untouched, so children already released by
	 * task completion never wait for Controller publication or recovery. */
	int valid = record_recovery_invalidations(runtime, workflow_id,
			recovery_queue, tail, attempts, event_nanoseconds);
	if (valid)
		*invalidated_count = tail;
	return valid;
}

static int build_scheduler(struct jx *root, struct itable *data,
		struct itable *tasks, struct vine_datavine_scheduler **result)
{
	struct jx *task_array = jx_lookup(root, "tasks");
	uint64_t task_count = 0;
	uint64_t edge_count = 0;
	int64_t maximum_task_id = 1;
	struct jx *policy = jx_lookup(root, "policy");
	uint64_t maximum_tasks = policy
						 ? (uint64_t)jx_lookup_integer(policy, "maximum_tasks")
						 : 0;
	uint64_t maximum_edges = policy
						 ? (uint64_t)jx_lookup_integer(policy, "maximum_edges")
						 : 0;
	struct jx *task;
	void *iterator = 0;
	while ((task = jx_iterate_array(task_array, &iterator))) {
		int64_t task_id = (int64_t)vine_datavine_ir_task_id(task);
		if (task_id > maximum_task_id)
			maximum_task_id = task_id;
		task_count++;
		struct jx *input;
		void *input_iterator = 0;
		while ((input = jx_iterate_array(vine_datavine_ir_task_inputs(task), &input_iterator))) {
			struct jx *record = data_record(data, vine_datavine_ir_input_data_id(input));
			if (vine_datavine_ir_data_is_output(record))
				edge_count++;
		}
	}
	if (maximum_tasks < task_count)
		maximum_tasks = task_count;
	if (maximum_edges < edge_count)
		maximum_edges = edge_count;
	if (maximum_tasks > task_count) {
		uint64_t remaining = maximum_tasks - task_count;
		if ((uint64_t)maximum_task_id > INT64_MAX - remaining)
			return 0;
		maximum_task_id += (int64_t)remaining;
	}
	struct vine_datavine_scheduler *scheduler = vine_datavine_scheduler_create(
			maximum_task_id, maximum_tasks ? maximum_tasks : 1, maximum_edges);
	if (!scheduler)
		return 0;
	iterator = 0;
	while ((task = jx_iterate_array(task_array, &iterator))) {
		uint64_t task_id = vine_datavine_ir_task_id(task);
		struct itable *parents = itable_create(0);
		buffer_t encoded;
		buffer_init(&encoded);
		struct jx *input;
		void *input_iterator = 0;
		while ((input = jx_iterate_array(vine_datavine_ir_task_inputs(task), &input_iterator))) {
			struct jx *record = data_record(data, vine_datavine_ir_input_data_id(input));
			if (vine_datavine_ir_data_is_output(record)) {
				uint64_t parent = (uint64_t)vine_datavine_ir_data_producer(record);
				if (!itable_lookup(parents, parent)) {
					unsigned char bytes[8];
					put_little_u64(bytes, parent);
					buffer_putlstring(&encoded, (const char *)bytes, sizeof(bytes));
					itable_insert(parents, parent, task);
				}
			}
		}
		size_t size = 0;
		const char *bytes = buffer_tolstring(&encoded, &size);
		int valid = vine_datavine_scheduler_add_task(
				scheduler, (int64_t)task_id, bytes, size);
		buffer_free(&encoded);
		itable_delete(parents);
		if (!valid || !itable_insert(tasks, task_id, task)) {
			vine_datavine_scheduler_delete(scheduler);
			return 0;
		}
	}
	if (!vine_datavine_scheduler_seal(scheduler)) {
		vine_datavine_scheduler_delete(scheduler);
		return 0;
	}
	*result = scheduler;
	return 1;
}

static int apply_delta_root(struct jx *root,
		struct vine_datavine_scheduler *scheduler, struct itable *data,
		struct itable *tasks, struct itable *consumers,
		struct itable *pending_consumers,
		struct itable *requested, int count_pending)
{
	struct jx *record;
	void *iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "data"), &iterator))) {
		uint64_t data_id = vine_datavine_ir_data_id(record);
		if (itable_lookup(data, data_id) || !itable_insert(data, data_id, record))
			return 0;
	}
	if (!vine_datavine_scheduler_begin_update(scheduler,
				vine_datavine_scheduler_revision(scheduler)))
		return 0;
	iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "tasks"), &iterator))) {
		uint64_t task_id = vine_datavine_ir_task_id(record);
		buffer_t parents;
		buffer_init(&parents);
		struct itable *seen = itable_create(0);
		struct jx *input;
		void *input_iterator = 0;
		int valid = seen != 0;
		while (valid && (input = jx_iterate_array(vine_datavine_ir_task_inputs(record),
						 &input_iterator))) {
			uint64_t data_id = vine_datavine_ir_input_data_id(input);
			uintptr_t count = (uintptr_t)itable_lookup(consumers, data_id);
			valid = itable_insert(consumers, data_id, (void *)(count + 1));
			if (valid && count_pending)
				valid = pending_consumer_increment(pending_consumers, data_id);
			struct jx *data_value = data_record(data, data_id);
			if (valid && data_value && vine_datavine_ir_data_is_output(data_value)) {
				uint64_t parent = (uint64_t)vine_datavine_ir_data_producer(data_value);
				if (!itable_lookup(seen, parent)) {
					unsigned char encoded[8];
					put_little_u64(encoded, parent);
					valid = buffer_putlstring(&parents,
								(const char *)encoded,
								sizeof(encoded)) >= 0 &&
						itable_insert(seen, parent, record);
				}
			}
		}
		size_t parent_size = 0;
		const char *parent_bytes = buffer_tolstring(&parents, &parent_size);
		valid = valid && !itable_lookup(tasks, task_id) &&
			vine_datavine_scheduler_add_task(scheduler, (int64_t)task_id, parent_bytes, parent_size) &&
			itable_insert(tasks, task_id, record);
		itable_delete(seen);
		buffer_free(&parents);
		if (!valid) {
			vine_datavine_scheduler_abort_update(scheduler);
			return 0;
		}
	}
	iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "requested_outputs"),
				&iterator))) {
		if (!itable_insert(requested, (uint64_t)record->u.integer_value, record)) {
			vine_datavine_scheduler_abort_update(scheduler);
			return 0;
		}
	}
	return vine_datavine_scheduler_commit_update(scheduler);
}

static const char *data_path(uint64_t data_id, char path[64])
{
	snprintf(path, 64, "datavine/data/%llu", (unsigned long long)data_id);
	return path;
}

static int task_defaults_index_add(struct task_defaults_index *index,
		struct jx *root)
{
	struct jx *tasks = jx_lookup(root, "tasks");
	int count = tasks ? jx_array_length(tasks) : 0;
	if (count < 1)
		return 1;
	struct jx *first = jx_array_index(tasks, 0);
	struct jx *last = jx_array_index(tasks, count - 1);
	if (!first || !last)
		return 0;
	if (index->count == index->capacity) {
		size_t capacity = index->capacity ? index->capacity * 2 : 16;
		if (capacity < index->capacity ||
				capacity > SIZE_MAX / sizeof(*index->ranges))
			return 0;
		void *ranges = realloc(index->ranges,
				capacity * sizeof(*index->ranges));
		if (!ranges)
			return 0;
		index->ranges = ranges;
		index->capacity = capacity;
	}
	index->ranges[index->count++] = (struct task_defaults_range){
			.first_task_id = vine_datavine_ir_task_id(first),
			.last_task_id = vine_datavine_ir_task_id(last),
			.defaults = jx_lookup(root, "task_defaults"),
			.default_codec = jx_lookup(root, "data_defaults")
							 ? jx_lookup(jx_lookup(root, "data_defaults"), "codec")
							 : 0,
	};
	return 1;
}

static struct jx *task_default_codec(struct task_defaults_index *index,
		struct jx *task)
{
	uint64_t task_id = vine_datavine_ir_task_id(task);
	size_t low = 0;
	size_t high = index->count;
	while (low < high) {
		size_t middle = low + (high - low) / 2;
		struct task_defaults_range *range = &index->ranges[middle];
		if (task_id < range->first_task_id)
			high = middle;
		else if (task_id > range->last_task_id)
			low = middle + 1;
		else
			return range->default_codec;
	}
	return 0;
}

static struct jx *task_configuration(struct task_defaults_index *index,
		struct jx *task, const char *name)
{
	struct jx *value = vine_datavine_ir_task_compact(task)
					   ? 0
					   : jx_lookup(task, name);
	if (value)
		return value;
	uint64_t task_id = vine_datavine_ir_task_id(task);
	size_t low = 0;
	size_t high = index->count;
	while (low < high) {
		size_t middle = low + (high - low) / 2;
		struct task_defaults_range *range = &index->ranges[middle];
		if (task_id < range->first_task_id)
			high = middle;
		else if (task_id > range->last_task_id)
			low = middle + 1;
		else
			return range->defaults ? jx_lookup(range->defaults, name) : 0;
	}
	return 0;
}

struct task_spec_input {
	uint64_t data_id;
	struct jx *record;
};

static char *task_spec_origin_uri(
		struct vine_datavine_data_controller *controller, uint64_t data_id,
		struct jx *record, struct itable *origin_tickets)
{
	struct jx *origin = record ? vine_datavine_ir_data_origin(record) : 0;
	const char *kind = origin ? jx_lookup_string(origin, "kind") : 0;
	if (!kind)
		return 0;
	if (!strcmp(kind, "uri"))
		return strdup(jx_lookup_string(origin, "uri"));
	char *cached = origin_tickets
			? itable_lookup(origin_tickets, data_id) : 0;
	if (cached)
		return strdup(cached);
	if (!strcmp(kind, "object")) {
		char *ticket = vine_datavine_data_controller_object_ticket(controller,
				jx_lookup_string(origin, "sha256"));
		char *retained = ticket && origin_tickets ? strdup(ticket) : 0;
		if (ticket && origin_tickets &&
				(!retained || !itable_insert(origin_tickets, data_id, retained))) {
			free(retained);
			free(ticket);
			return 0;
		}
		return ticket;
	}
	if (strcmp(kind, "inline"))
		return 0;
	buffer_t decoded;
	buffer_init(&decoded);
	char *ticket = 0;
	if (b64_decode(jx_lookup_string(origin, "base64"), &decoded) == 0) {
		size_t size = 0;
		const char *bytes = buffer_tolstring(&decoded, &size);
		unsigned char digest[32];
		char encoded[65];
		static const char hexadecimal[] = "0123456789abcdef";
		SHA256((const unsigned char *)bytes, size, digest);
		for (size_t index = 0; index < 32; index++) {
			encoded[index * 2] = hexadecimal[digest[index] >> 4];
			encoded[index * 2 + 1] = hexadecimal[digest[index] & 15];
		}
		encoded[64] = 0;
		int deduplicated = 0;
		if (vine_datavine_data_controller_put_object(controller, encoded,
				bytes, size, &deduplicated))
			ticket = vine_datavine_data_controller_object_ticket(
					controller, encoded);
	}
	buffer_free(&decoded);
	char *retained = ticket && origin_tickets ? strdup(ticket) : 0;
	if (ticket && origin_tickets &&
			(!retained || !itable_insert(origin_tickets, data_id, retained))) {
		free(retained);
		free(ticket);
		return 0;
	}
	return ticket;
}

static int task_spec_input_add(struct task_spec_input *inputs, size_t *count,
		size_t capacity, uint64_t data_id, struct itable *data)
{
	if (!data_id)
		return 0;
	for (size_t index = 0; index < *count; index++)
		if (inputs[index].data_id == data_id)
			return 1;
	if (*count >= capacity)
		return 0;
	struct jx *record = data_record(data, data_id);
	if (!record)
		return 0;
	inputs[*count].data_id = data_id;
	inputs[*count].record = record;
	(*count)++;
	return 1;
}

static int attach_worker_data_spec(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct vine_task *physical, struct jx *task,
		struct jx *executor, struct jx *default_codec, struct itable *data,
		struct itable *consumers, struct itable *origin_tickets,
		struct itable *requested, uint32_t attempt, int retain_all)
{
	struct jx *task_inputs = vine_datavine_ir_task_inputs(task);
	size_t capacity = (size_t)jx_array_length(task_inputs) + 1;
	struct task_spec_input *inputs = calloc(capacity ? capacity : 1,
			sizeof(*inputs));
	if (!inputs)
		return 0;
	size_t input_count = 0;
	const char *kind = jx_lookup_string(executor, "kind");
	const char *version = jx_lookup_string(executor, "version");
	if (!strcmp(kind, "python") &&
			strcmp(version, VINE_DATAVINE_PYTHON_CALLABLE_VERSION) &&
			!task_spec_input_add(inputs, &input_count, capacity,
					(uint64_t)jx_lookup_integer(executor, "payload_ref"), data)) {
		free(inputs);
		return 0;
	}
	struct jx *input;
	void *iterator = 0;
	while ((input = jx_iterate_array(task_inputs, &iterator))) {
		if (!task_spec_input_add(inputs, &input_count, capacity,
				vine_datavine_ir_input_data_id(input), data)) {
			free(inputs);
			return 0;
		}
	}
	struct jx *outputs = vine_datavine_ir_task_outputs(task);
	size_t output_count = (size_t)jx_array_length(outputs);
	if (!output_count || input_count > UINT32_MAX || output_count > UINT32_MAX) {
		free(inputs);
		return 0;
	}
	buffer_t strings;
	buffer_init(&strings);
	unsigned char *input_records = calloc(input_count ? input_count : 1,
			VINE_DATAVINE_TASK_SPEC_INPUT);
	unsigned char *output_records = calloc(output_count,
			VINE_DATAVINE_TASK_SPEC_OUTPUT);
	int valid = input_records && output_records;
	for (size_t index = 0; valid && index < input_count; index++) {
		unsigned char *record = input_records +
				index * VINE_DATAVINE_TASK_SPEC_INPUT;
		vine_datavine_put_u64(record, inputs[index].data_id);
		if (vine_datavine_ir_data_is_output(inputs[index].record)) {
			char durable_path[PATH_MAX];
			struct vine_datavine_workflow_result_info result_info;
			if (vine_datavine_data_controller_result_path(controller,
					workflow_id, inputs[index].data_id, durable_path,
					sizeof(durable_path)) &&
					vine_datavine_data_controller_result_info(controller,
						workflow_id, inputs[index].data_id, &result_info)) {
				size_t path_size = strlen(durable_path);
				char *uri = malloc(path_size + 8);
				if (uri)
					snprintf(uri, path_size + 8, "file://%s", durable_path);
				size_t uri_size = uri ? strlen(uri) : 0;
				size_t current_size = 0;
				buffer_tolstring(&strings, &current_size);
				valid = uri && uri_size <= UINT32_MAX &&
						current_size <= UINT32_MAX &&
						buffer_putlstring(&strings, uri, uri_size) >= 0;
				if (valid) {
					vine_datavine_put_u32(record + 8, result_info.attempt);
					vine_datavine_put_u32(record + 12,
							VINE_DATAVINE_TASK_INPUT_LOCAL_FILE);
					vine_datavine_put_u32(record + 16,
							(uint32_t)current_size);
					vine_datavine_put_u32(record + 20,
							(uint32_t)uri_size);
				}
				free(uri);
			} else {
				vine_datavine_put_u32(record + 12,
						VINE_DATAVINE_TASK_INPUT_GENERATED);
			}
			continue;
		}
		char *uri = task_spec_origin_uri(controller, inputs[index].data_id,
				inputs[index].record, origin_tickets);
		size_t uri_size = uri ? strlen(uri) : 0;
		size_t current_size = 0;
		buffer_tolstring(&strings, &current_size);
		valid = uri && uri_size && uri_size <= UINT32_MAX &&
				current_size <= UINT32_MAX &&
				buffer_putlstring(&strings, uri, uri_size) >= 0;
		if (valid) {
			uint32_t input_kind = uri_size >= 8 &&
					!memcmp(uri, "file:///", 8) && !memchr(uri, '%', uri_size)
					? VINE_DATAVINE_TASK_INPUT_LOCAL_FILE
					: (uintptr_t)itable_lookup(consumers,
							inputs[index].data_id) == 1
						? VINE_DATAVINE_TASK_INPUT_URI_EPHEMERAL
						: VINE_DATAVINE_TASK_INPUT_URI;
			vine_datavine_put_u32(record + 12, input_kind);
			vine_datavine_put_u32(record + 16, (uint32_t)current_size);
			vine_datavine_put_u32(record + 20, (uint32_t)uri_size);
		}
		free(uri);
	}
	struct jx *output_files = jx_lookup(executor, "output_files");
	if (valid && output_files &&
			(size_t)jx_array_length(output_files) != output_count)
		valid = 0;
	for (size_t index = 0; valid && index < output_count; index++) {
		unsigned char *record = output_records +
				index * VINE_DATAVINE_TASK_SPEC_OUTPUT;
		uint64_t data_id = (uint64_t)jx_array_index(outputs, (int)index)
				->u.integer_value;
		uint32_t generation = attempt;
		valid = data_id && vine_datavine_data_controller_agent_expect(
				controller, workflow_id, data_id, &generation,
				itable_lookup(requested, data_id) != 0);
		const char *name = output_files
				? jx_array_index(output_files, (int)index)->u.string_value
				: ".taskvine.stdout";
		size_t name_size = name ? strlen(name) : 0;
		size_t current_size = 0;
		buffer_tolstring(&strings, &current_size);
		valid = valid && name_size && name_size <= UINT32_MAX &&
				current_size <= UINT32_MAX &&
				buffer_putlstring(&strings, name, name_size) >= 0;
		if (valid) {
			uint32_t flags = (retain_all || itable_lookup(consumers, data_id) ||
					itable_lookup(requested, data_id))
					? VINE_DATAVINE_TASK_OUTPUT_RETAIN : 0;
			if (itable_lookup(requested, data_id))
				flags |= VINE_DATAVINE_TASK_OUTPUT_REQUESTED;
			vine_datavine_put_u64(record, data_id);
			vine_datavine_put_u32(record + 8, generation);
			vine_datavine_put_u32(record + 12, flags);
			vine_datavine_put_u32(record + 16, (uint32_t)current_size);
			vine_datavine_put_u32(record + 20, (uint32_t)name_size);
			if (flags & VINE_DATAVINE_TASK_OUTPUT_REQUESTED) {
				struct jx *data_item = data_record(data, data_id);
				struct jx *codec = data_item
						? vine_datavine_ir_data_codec(data_item, default_codec) : 0;
				const char *codec_name = codec
						? jx_lookup_string(codec, "name") : 0;
				const char *codec_version = codec
						? jx_lookup_string(codec, "version") : 0;
				char durable_path[PATH_MAX];
				valid = codec_name && codec_version &&
						vine_datavine_data_controller_agent_expect_result(
								controller, workflow_id, data_id, generation,
								(int64_t)vine_datavine_ir_task_id(task),
								(int32_t)index, codec_name, codec_version) &&
						vine_datavine_data_controller_output_path(controller,
								workflow_id, data_id, generation, durable_path,
								sizeof(durable_path));
				size_t path_size = valid ? strlen(durable_path) : 0;
				buffer_tolstring(&strings, &current_size);
				valid = valid && path_size && path_size <= UINT32_MAX &&
						current_size <= UINT32_MAX &&
						buffer_putlstring(&strings, durable_path,
								path_size) >= 0;
				if (valid) {
					vine_datavine_put_u32(record + 24,
							(uint32_t)current_size);
					vine_datavine_put_u32(record + 28,
							(uint32_t)path_size);
				}
			}
		}
	}
	unsigned char key[32];
	char host[VINE_DATAVINE_AGENT_HOST_MAX];
	uint16_t port = 0;
	size_t strings_size = 0;
	const char *string_bytes = buffer_tolstring(&strings, &strings_size);
	size_t records_size = input_count * VINE_DATAVINE_TASK_SPEC_INPUT +
			output_count * VINE_DATAVINE_TASK_SPEC_OUTPUT;
	valid = valid && strings_size <= UINT32_MAX &&
			records_size <= SIZE_MAX - VINE_DATAVINE_TASK_SPEC_HEADER &&
			strings_size <= SIZE_MAX - VINE_DATAVINE_TASK_SPEC_HEADER - records_size &&
			vine_datavine_data_controller_workflow_key(controller,
					workflow_id, key) &&
			vine_datavine_data_controller_agent_endpoint(controller, host, &port);
	size_t spec_size = VINE_DATAVINE_TASK_SPEC_HEADER + records_size +
			strings_size;
	unsigned char *spec = valid ? calloc(1, spec_size) : 0;
	if (spec) {
		memcpy(spec, VINE_DATAVINE_TASK_SPEC_MAGIC, 4);
		vine_datavine_put_u16(spec + 4, 2);
		memcpy(spec + 8, key, 32);
		uint64_t workflow_slot = vine_datavine_get_u64(key);
		vine_datavine_put_u64(spec + 40, workflow_slot ? workflow_slot : 1);
		vine_datavine_put_u16(spec + 48, port);
		size_t host_size = strlen(host);
		vine_datavine_put_u16(spec + 50, (uint16_t)host_size);
		memcpy(spec + 52, host, host_size);
		vine_datavine_put_u32(spec + 116, (uint32_t)input_count);
		vine_datavine_put_u32(spec + 120, (uint32_t)output_count);
		vine_datavine_put_u32(spec + 124, (uint32_t)strings_size);
		memcpy(spec + VINE_DATAVINE_TASK_SPEC_HEADER, input_records,
				input_count * VINE_DATAVINE_TASK_SPEC_INPUT);
		memcpy(spec + VINE_DATAVINE_TASK_SPEC_HEADER +
				input_count * VINE_DATAVINE_TASK_SPEC_INPUT,
				output_records,
				output_count * VINE_DATAVINE_TASK_SPEC_OUTPUT);
		memcpy(spec + VINE_DATAVINE_TASK_SPEC_HEADER + records_size,
				string_bytes, strings_size);
		vine_task_set_auxiliary_payload(physical, spec, spec_size);
	}
	int attached = spec != 0;
	free(spec);
	free(output_records);
	free(input_records);
	buffer_free(&strings);
	free(inputs);
	return attached;
}

static struct vine_task *materialize(struct vine_manager *manager,
		struct vine_datavine_data_controller *data_controller,
		const char *workflow_id, struct jx *task, struct itable *data,
		struct itable *files, struct itable *consumers,
		struct itable *origin_tickets,
		struct itable *requested, struct task_defaults_index *defaults,
		uint32_t attempt, int retain_all)
{
	(void)manager;
	(void)files;
	struct jx *executor = task_configuration(defaults, task, "executor");
	struct jx *resources = task_configuration(defaults, task, "resources");
	struct jx *priority = task_configuration(defaults, task, "priority");
	if (!executor)
		return 0;
	const char *kind = jx_lookup_string(executor, "kind");
	if (strcmp(kind, "command") && strcmp(kind, "python") &&
			strcmp(kind, "taskvine"))
		return 0;
	/* Register output generations with the independent Data Controller before
	 * the physical task can publish. This is a local metadata mutation, not an
	 * output-admission gate: child readiness still follows task completion. */
	struct jx *expected_outputs = vine_datavine_ir_task_outputs(task);
	for (int index = 0; index < jx_array_length(expected_outputs); index++) {
		uint64_t data_id = (uint64_t)jx_array_index(
				expected_outputs, index)->u.integer_value;
		uint32_t generation = attempt;
		if (!vine_datavine_data_controller_agent_expect(data_controller,
				workflow_id, data_id, &generation,
				itable_lookup(requested, data_id) != 0))
			return 0;
	}
	buffer_t command;
	buffer_init(&command);
	void *iterator = 0;
	buffer_t function_input;
	buffer_init(&function_input);
	int python_fork = !strcmp(kind, "python") &&
			  !jx_lookup(executor, "environment");
	if (!strcmp(kind, "taskvine")) {
		uint64_t payload_id = (uint64_t)jx_lookup_integer(executor, "payload_ref");
		struct jx *payload_record = data_record(data, payload_id);
		struct jx *origin = payload_record ? jx_lookup(payload_record, "origin") : 0;
		if (!origin || strcmp(jx_lookup_string(origin, "kind"), "inline") ||
				b64_decode(jx_lookup_string(origin, "base64"), &function_input) != 0 ||
				buffer_putliteral(&command, "datavine_builtin") < 0) {
			buffer_free(&function_input);
			buffer_free(&command);
			return 0;
		}
	} else if (!strcmp(kind, "python") && !python_fork) {
		char payload_path[64];
		uint64_t payload_id = (uint64_t)jx_lookup_integer(executor, "payload_ref");
		if (!shell_word(&command, "python3") ||
				buffer_putliteral(&command, " ") < 0 ||
				!shell_word(&command, data_path(payload_id, payload_path))) {
			buffer_free(&command);
			return 0;
		}
	} else if (!python_fork) {
		struct jx *argument;
		int first = 1;
		while ((argument = jx_iterate_array(jx_lookup(executor, "argv"), &iterator))) {
			if (!first)
				buffer_putliteral(&command, " ");
			first = 0;
			const char *value = argument->u.string_value;
			char replacement[64];
			unsigned long long parsed_data_id = 0;
			char trailing = 0;
			if (sscanf(value, "{{data:%llu}}%c", &parsed_data_id, &trailing) == 1) {
				uint64_t data_id = (uint64_t)parsed_data_id;
				value = data_path(data_id, replacement);
			}
			if (!shell_word(&command, value)) {
				buffer_free(&command);
				return 0;
			}
		}
	}
	struct vine_task *physical = 0;
	if (python_fork) {
		physical = create_python_ticket(
				task, executor, resources, data, consumers, requested, retain_all, data_controller, workflow_id, attempt);
	} else if (!strcmp(kind, "taskvine")) {
		size_t input_size = 0;
		const unsigned char *input = (const unsigned char *)
				buffer_tolstring(&function_input, &input_size);
		if (input_size == 5 && input[4] == 1)
			physical = create_native_ticket(
					(int64_t)vine_datavine_ir_task_id(task), attempt, 1);
	}
	if (!physical)
		physical = vine_task_create(buffer_tostring(&command));
	buffer_free(&command);
	if (!physical) {
		buffer_free(&function_input);
		return 0;
	}
	if (!strcmp(kind, "taskvine") &&
			!vine_task_get_library_required(physical)) {
		size_t input_size = 0;
		const char *input = buffer_tolstring(&function_input, &input_size);
		vine_task_set_library_required(physical, "datavine-native-v1");
		vine_task_set_function_input(physical, input, input_size);
	}
	buffer_free(&function_input);
	/* Return FORSAKEN exactly once.  The DataVine Scheduler owns resubmission;
	 * the Manager owns only worker detection and task reclamation. */
	vine_task_set_max_forsaken(physical, 0);
	struct jx *input;
	iterator = 0;
	while ((input = jx_iterate_array(vine_datavine_ir_task_inputs(task), &iterator))) {
		uint64_t data_id = vine_datavine_ir_input_data_id(input);
		char path[64];
		if (!python_fork) {
			data_path(data_id, path);
			char variable[64];
			snprintf(variable, sizeof(variable), "DATAVINE_DATA_%llu", (unsigned long long)data_id);
			vine_task_set_env_var(physical, variable, path);
		}
	}
	if (!attach_worker_data_spec(data_controller, workflow_id, physical, task,
			executor, task_default_codec(defaults, task), data, consumers,
			origin_tickets, requested, attempt, retain_all)) {
		vine_task_delete(physical);
		return 0;
	}
	struct jx *cores = resources ? jx_lookup(resources, "cores") : 0;
	/* Workflow IR tasks are independent scientific work units. TaskVine's
	 * unspecified-resource default may consume an entire Worker, silently
	 * serializing unrelated READY tasks and workflows. A resources object that
	 * only sets memory/disk/gpus must retain the same one-core default. */
	vine_task_set_cores(physical, cores ? cores->u.integer_value : 1);
	if (resources) {
		struct jx *value = jx_lookup(resources, "memory_mb");
		if (value)
			vine_task_set_memory(physical, value->u.integer_value);
		value = jx_lookup(resources, "disk_mb");
		if (value)
			vine_task_set_disk(physical, value->u.integer_value);
		value = jx_lookup(resources, "gpus");
		if (value)
			vine_task_set_gpus(physical, (int)value->u.integer_value);
		value = jx_lookup(resources, "wall_time_seconds");
		/* The fork preloader owns its child process group and enforces this
		 * deadline directly. TaskVine cannot kill a FunctionCall child and
		 * applying both timers corrupts the library's running-slot count. */
		if (value && !python_fork)
			vine_task_set_time_max(physical, value->u.integer_value);
	}
	if (priority)
		vine_task_set_priority(physical, priority->u.integer_value);
	struct jx *environment = jx_lookup(executor, "environment");
	if (environment) {
		const char *name;
		void *environment_iterator = 0;
		while ((name = jx_iterate_keys(environment, &environment_iterator)))
			vine_task_set_env_var(physical, name, jx_lookup_string(environment, name));
	}
	return physical;
}

static struct vine_task *materialize_parametric(
		struct vine_datavine_workflow_runtime *runtime, const char *workflow_id,
		struct execution_resources *resources, uint64_t task_id,
		uint32_t attempt, int retain_all,
		struct parametric_task_view **view_result)
{
	*view_result = 0;
	struct parametric_task_view *view = parametric_task_view_create(
			resources->parametric, resources->initial_root, task_id);
	if (!view)
		return 0;
	struct vine_task *physical = materialize(runtime->manager,
			runtime->data_controller, workflow_id, view->task, view->data,
			resources->files, view->consumers, resources->origin_tickets,
			view->requested,
			&resources->task_defaults, attempt, retain_all);
	if (!physical) {
		parametric_task_view_delete(view);
		return 0;
	}
	*view_result = view;
	return physical;
}

static int maximum_attempts(struct task_defaults_index *defaults,
		struct jx *task)
{
	struct jx *retry = task_configuration(defaults, task, "retry");
	return retry ? (int)jx_lookup_integer(retry, "maximum_attempts") : 1;
}

static __attribute__((unused)) struct vine_datavine_data_publication *publish_task_outputs(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct jx *task, struct jx *executor,
		struct jx *default_codec,
		struct itable *data,
		struct itable *files, struct itable *consumers,
		struct itable *requested,
		struct vine_task *completed, uint32_t attempt, int retain_all)
{
	struct jx *outputs = vine_datavine_ir_task_outputs(task);
	if (jx_array_length(outputs) < 1)
		return 0;
	return vine_datavine_data_controller_publish_async(
			runtime->data_controller, workflow_id, task, executor, default_codec, data, files, consumers, requested, attempt, retain_all, completed);
}

static int __attribute__((unused)) restore_published_outputs(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct jx *task, struct itable *files,
		struct itable *pending_consumers)
{
	struct jx *outputs = vine_datavine_ir_task_outputs(task);
	int count = jx_array_length(outputs);
	int valid = count > 0;
	manager_lane_lock(runtime);
	for (int index = 0; valid && index < count; index++) {
		uint64_t data_id = (uint64_t)jx_array_index(
				outputs, index)
						   ->u.integer_value;
		if (!itable_lookup(pending_consumers, data_id) ||
				itable_lookup(files, data_id))
			continue;
		struct vine_file *file =
				vine_datavine_data_controller_restore_file(
						runtime->data_controller, runtime->manager, workflow_id, data_id);
		if (!file || !itable_insert(files, data_id, file)) {
			if (file)
				vine_undeclare_file(runtime->manager, file);
			valid = 0;
		}
	}
	pthread_mutex_unlock(&runtime->manager_lock);
	return valid;
}

static int recovery_cache_retain(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct recovery_result_cache *cache,
		struct itable *files, struct itable *pending_consumers,
		struct itable *requested, const uint64_t *data_ids, size_t count)
{
	uint64_t local[32];
	uint64_t *release = count <= sizeof(local) / sizeof(local[0])
						? local
						: malloc(count * sizeof(*release));
	if (!release)
		return 0;
	size_t release_count = 0;
	int valid = 1;
	for (size_t index = 0; valid && index < count; index++) {
		uint64_t data_id = data_ids[index];
		if (itable_lookup(cache->members, data_id))
			continue;
		if (!cache->limit) {
			release[release_count++] = data_id;
			continue;
		}
		if (cache->count == cache->limit) {
			struct recovery_cache_entry oldest = cache->entries[cache->head];
			cache->head = (cache->head + 1) % cache->limit;
			cache->count--;
			uintptr_t current = (uintptr_t)itable_lookup(
					cache->members, oldest.data_id);
			if (current == oldest.token) {
				itable_remove(cache->members, oldest.data_id);
				if (!itable_lookup(pending_consumers, oldest.data_id) &&
						!itable_lookup(requested, oldest.data_id))
					release[release_count++] = oldest.data_id;
			}
		}
		uintptr_t token = ++cache->next_token;
		if (!token)
			token = ++cache->next_token;
		size_t tail = (cache->head + cache->count) % cache->limit;
		cache->entries[tail] = (struct recovery_cache_entry){data_id, token};
		valid = itable_insert(cache->members, data_id, (void *)token);
		if (valid) {
			cache->count++;
			size_t members = (size_t)itable_size(cache->members);
			if (members > cache->peak)
				cache->peak = members;
		}
	}
	if (valid && release_count) {
		manager_lane_lock(runtime);
		for (size_t offset = 0; valid && offset < release_count;) {
			size_t batch = release_count - offset;
			if (batch > UINT16_MAX)
				batch = UINT16_MAX;
			valid = vine_datavine_data_controller_release_results(
					runtime->data_controller, runtime->manager, workflow_id, files, release + offset, batch);
			offset += batch;
		}
		if (valid) {
			cache->evictions += release_count;
		}
		pthread_mutex_unlock(&runtime->manager_lock);
	}
	if (release != local)
		free(release);
	return valid;
}

static int __attribute__((unused)) cache_inactive_task_inputs(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct recovery_result_cache *cache,
		struct jx *task, struct itable *data, struct itable *files,
		struct itable *pending_consumers, struct itable *requested)
{
	struct jx *inputs = vine_datavine_ir_task_inputs(task);
	size_t input_count = (size_t)jx_array_length(inputs);
	uint64_t local[32];
	uint64_t *retain = input_count <= sizeof(local) / sizeof(local[0])
					   ? local
					   : malloc(input_count * sizeof(*retain));
	if (!retain)
		return 0;
	size_t count = 0;
	struct jx *input;
	void *iterator = 0;
	while ((input = jx_iterate_array(inputs, &iterator))) {
		uint64_t data_id = vine_datavine_ir_input_data_id(input);
		uint64_t producer = 0;
		if (!itable_lookup(pending_consumers, data_id) &&
				!itable_lookup(requested, data_id) &&
				output_producer(data, data_id, &producer))
			retain[count++] = data_id;
	}
	int valid = recovery_cache_retain(runtime, workflow_id, cache, files, pending_consumers, requested, retain, count);
	if (retain != local)
		free(retain);
	return valid;
}

static int finish_logical_attempt(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct vine_datavine_scheduler *scheduler,
		struct recovery_result_cache *recovery_cache,
		struct itable *data, struct itable *files,
		struct itable *pending_consumers,
		struct itable *requested, struct jx *task,
		int64_t logical_id, uint32_t attempt, int32_t task_result,
		uint32_t maximum_attempt_count, int successful,
		uint64_t *completion_event_nanoseconds)
{
	(void)recovery_cache;
	(void)files;
	int valid = logical_id > 0 && task;
	struct vine_datavine_workflow_task_event_record event = {
			.task_id = logical_id,
			.attempt = attempt,
			.result = task_result,
	};
	if (successful) {
		event.type = VINE_DATAVINE_WORKFLOW_TASK_COMPLETED;
		event.result = 0;
		valid &= vine_datavine_scheduler_mark_done(scheduler, logical_id) &&
			 adjust_task_pending_inputs(task, pending_consumers, 0);
		unsigned char workflow_key[32];
		uint64_t workflow_slot = 0;
		if (valid && vine_datavine_data_controller_workflow_key(
				runtime->data_controller, workflow_id, workflow_key)) {
			workflow_slot = vine_datavine_get_u64(workflow_key);
			if (!workflow_slot)
				workflow_slot = 1;
		} else {
			valid = 0;
		}
		struct jx *input;
		void *iterator = 0;
		while (valid && (input = jx_iterate_array(
					vine_datavine_ir_task_inputs(task), &iterator))) {
			uint64_t data_id = vine_datavine_ir_input_data_id(input);
			uint64_t producer = 0;
			if (!itable_lookup(pending_consumers, data_id) &&
					!itable_lookup(requested, data_id) &&
					output_producer(data, data_id, &producer)) {
				int released = vine_datavine_data_controller_agent_mark_dead(
						runtime->data_controller, workflow_slot, data_id, 0);
				if (!released && getenv("DATAVINE_WORKFLOW_METRICS"))
					fprintf(stderr,
							"datavine workflow %s release_failed data=%llu consumer=%lld\n",
							workflow_id, (unsigned long long)data_id,
							(long long)logical_id);
				valid &= released;
			}
		}
	} else if (valid &&
			(task_result == -(int32_t)VINE_RESULT_FORSAKEN ||
					attempt < maximum_attempt_count)) {
		event.type = VINE_DATAVINE_WORKFLOW_TASK_RETRY;
		valid &= vine_datavine_scheduler_mark_pending(scheduler, logical_id);
	} else {
		event.type = VINE_DATAVINE_WORKFLOW_TASK_FAILED;
		valid = 0;
	}
	if (logical_id > 0) {
		struct vine_datavine_workflow_error error;
		struct timespec started;
		struct timespec finished;
		clock_gettime(CLOCK_MONOTONIC, &started);
		int recorded = vine_datavine_workflow_store_record_task_events(
				runtime->store, workflow_id, &event, 1, &error);
		clock_gettime(CLOCK_MONOTONIC, &finished);
		*completion_event_nanoseconds += elapsed_nanoseconds(&started, &finished);
		valid &= recorded;
	}
	return valid;
}

static int finish_parametric_attempt(
		struct vine_datavine_workflow_runtime *runtime, const char *workflow_id,
		struct execution_resources *resources, int64_t logical_id,
		uint32_t attempt, int32_t task_result, int successful,
		struct vine_datavine_workflow_task_event_record *event)
{
	int valid = logical_id > 0 &&
		(uint64_t)logical_id <= resources->maximum_task_id && event;
	*event = (struct vine_datavine_workflow_task_event_record){
			.task_id = logical_id,
			.attempt = attempt,
			.result = task_result,
	};
	if (successful) {
		event->type = VINE_DATAVINE_WORKFLOW_TASK_COMPLETED;
		event->result = 0;
		valid = valid && vine_datavine_scheduler_mark_done(
				resources->scheduler, logical_id);
		unsigned char workflow_key[32];
		uint64_t workflow_slot = 0;
		if (valid && vine_datavine_data_controller_workflow_key(
				runtime->data_controller, workflow_id, workflow_key)) {
			workflow_slot = vine_datavine_get_u64(workflow_key);
			if (!workflow_slot)
				workflow_slot = 1;
		} else {
			valid = 0;
		}
		uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
		size_t input_count = 0;
		uint64_t output = 0;
		valid = valid && parametric_task_inputs(resources->parametric,
				(uint64_t)logical_id, inputs, &input_count, &output);
		for (size_t index = 0; valid && index < input_count; index++) {
			uint64_t producer = 0;
			if (!vine_datavine_parametric_output_producer(resources->parametric,
					inputs[index], &producer))
				continue;
			valid = resources->parametric_remaining[producer] > 0;
			if (valid && --resources->parametric_remaining[producer] == 0) {
				int released = vine_datavine_data_controller_agent_mark_dead(
						runtime->data_controller, workflow_slot, inputs[index], 0);
				if (!released && getenv("DATAVINE_WORKFLOW_METRICS"))
					fprintf(stderr,
							"datavine workflow %s release_failed data=%llu consumer=%lld\n",
							workflow_id, (unsigned long long)inputs[index],
							(long long)logical_id);
				valid = valid && released;
			}
		}
	} else if (valid &&
			(task_result == -(int32_t)VINE_RESULT_FORSAKEN || attempt < 1)) {
		event->type = VINE_DATAVINE_WORKFLOW_TASK_RETRY;
		valid = vine_datavine_scheduler_mark_pending(resources->scheduler,
				logical_id);
	} else {
		event->type = VINE_DATAVINE_WORKFLOW_TASK_FAILED;
		valid = 0;
	}
	return valid;
}

static int drain_publications(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct vine_datavine_scheduler *scheduler,
		struct recovery_result_cache *recovery_cache,
		struct itable *data, struct itable *files,
		struct itable *pending_consumers,
		struct itable *requested,
		struct pending_publication **head, uint64_t *publishing,
		int block_one, uint32_t *completed_count,
		uint64_t *publish_nanoseconds,
		struct publication_stage_totals *publication_stages,
		uint64_t *completion_event_nanoseconds)
{
	struct pending_publication **link = head;
	int valid = 1;
	while (valid && *link) {
		struct pending_publication *pending = *link;
		if (!block_one &&
				!vine_datavine_data_publication_ready(pending->publication)) {
			link = &pending->next;
			continue;
		}
		struct timespec started;
		struct timespec finished;
		clock_gettime(CLOCK_MONOTONIC, &started);
		int published = vine_datavine_data_publication_wait(
				pending->publication);
		if (!published && getenv("DATAVINE_WORKFLOW_METRICS"))
			fprintf(stderr,
					"datavine workflow %s publication_failed logical_id=%lld\n",
					workflow_id,
					(long long)pending->logical_id);
		clock_gettime(CLOCK_MONOTONIC, &finished);
		*publish_nanoseconds += elapsed_nanoseconds(&started, &finished);
		struct vine_datavine_data_publication_metrics metrics;
		if (vine_datavine_data_publication_get_metrics(
					pending->publication, &metrics)) {
			publication_stages->queue_nanoseconds += metrics.queue_nanoseconds;
			publication_stages->commit_nanoseconds += metrics.commit_nanoseconds;
			publication_stages->pull_nanoseconds += metrics.pull_nanoseconds;
			publication_stages->decode_nanoseconds += metrics.decode_nanoseconds;
			publication_stages->function_nanoseconds += metrics.function_nanoseconds;
			publication_stages->serialize_nanoseconds += metrics.serialize_nanoseconds;
			publication_stages->fsync_nanoseconds += metrics.fsync_nanoseconds;
			publication_stages->outputs += metrics.outputs;
			publication_stages->remote_outputs += metrics.remote_outputs;
			publication_stages->durable_outputs += metrics.durable_outputs;
			publication_stages->output_bytes += metrics.output_bytes;
			publication_stages->journal_records += metrics.journal_records;
			publication_stages->task_reports += metrics.task_reports;
			publication_stages->task_reported_read_bytes +=
					metrics.task_reported_read_bytes;
			publication_stages->task_reported_cpu_milliseconds +=
					metrics.task_reported_cpu_milliseconds;
		}
		if (pending->logical_finished)
			valid = published;
		else
			valid = finish_logical_attempt(runtime, workflow_id, scheduler,
					recovery_cache, data, files, pending_consumers, requested,
					pending->task, pending->logical_id, pending->attempt,
					pending->task_result, pending->maximum_attempts, published,
					completion_event_nanoseconds);
		vine_datavine_data_publication_delete(pending->publication);
		*link = pending->next;
		int logical_finished = pending->logical_finished;
		free(pending);
		(*publishing)--;
		if (!logical_finished)
			(*completed_count)++;
		block_one = 0;
	}
	return valid;
}

static void pending_publications_delete(struct pending_publication *pending)
{
	while (pending) {
		struct pending_publication *next = pending->next;
		vine_datavine_data_publication_wait(pending->publication);
		vine_datavine_data_publication_delete(pending->publication);
		free(pending);
		pending = next;
	}
}

static size_t configured_recovery_cache_limit(void)
{
	const char *setting = getenv("DATAVINE_RECOVERY_CACHE_RESULTS");
	if (!setting || !setting[0])
		return DATAVINE_WORKFLOW_RECOVERY_CACHE_RESULTS;
	char *end = 0;
	unsigned long long parsed = strtoull(setting, &end, 10);
	if (!end || *end || parsed > SIZE_MAX / sizeof(struct recovery_cache_entry))
		return DATAVINE_WORKFLOW_RECOVERY_CACHE_RESULTS;
	return (size_t)parsed;
}

static void execution_resources_delete(struct execution_resources *resources)
{
	free(resources->task_defaults.ranges);
	free(resources->attempts);
	free(resources->recovery_marks);
	free(resources->recovery_queue);
	free(resources->recovery_pending);
	free(resources->recovery_active);
	free(resources->recovery_active_tasks);
	free(resources->recovery_cache.entries);
	free(resources->parametric_remaining);
	vine_datavine_parametric_delete(resources->parametric);
	if (resources->recovery_cache.members)
		itable_delete(resources->recovery_cache.members);
	buffer_free(&resources->recovered_tasks);
	if (resources->scheduler)
		vine_datavine_scheduler_delete(resources->scheduler);
	if (resources->physical_to_logical)
		itable_delete(resources->physical_to_logical);
	if (resources->origin_tickets) {
		uint64_t data_id;
		void *ticket;
		int iterator;
		ITABLE_ITERATE(resources->origin_tickets, iterator, data_id, ticket)
		{
			free(ticket);
		}
		itable_delete(resources->origin_tickets);
	}
	if (resources->files)
		itable_delete(resources->files);
	if (resources->consumers)
		itable_delete(resources->consumers);
	if (resources->pending_consumers)
		itable_delete(resources->pending_consumers);
	if (resources->requested)
		itable_delete(resources->requested);
	if (resources->tasks)
		itable_delete(resources->tasks);
	if (resources->data)
		itable_delete(resources->data);
	retained_roots_delete(resources->roots);
	if (resources->mailbox_initialized)
		mailbox_delete(&resources->mailbox);
}

static int execution_resources_create(struct execution_resources *resources,
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, const char *document, size_t document_size)
{
	memset(resources, 0, sizeof(*resources));
	buffer_init(&resources->recovered_tasks);
	if (pthread_mutex_init(&resources->mailbox.lock, 0))
		return 0;
	resources->mailbox_initialized = 1;
	struct jx *root = jx_parse_string_and_length(document, (int)document_size);
	resources->initial_root = root;
	int valid = retained_root_add(&resources->roots, root) &&
			task_defaults_index_add(&resources->task_defaults, root);
	if (valid && vine_datavine_parametric_present(root)) {
		resources->parametric = vine_datavine_parametric_parse(root, 0);
		valid = resources->parametric != 0;
	}
	resources->data = itable_create(0);
	resources->tasks = itable_create(0);
	resources->files = itable_create(0);
	resources->consumers = itable_create(0);
	resources->pending_consumers = itable_create(0);
	resources->requested = itable_create(0);
	resources->physical_to_logical = itable_create(0);
	resources->origin_tickets = itable_create(0);
	resources->recovery_cache.limit = configured_recovery_cache_limit();
	resources->recovery_cache.members = itable_create(0);
	resources->recovery_cache.entries = resources->recovery_cache.limit
								? calloc(resources->recovery_cache.limit,
										  sizeof(*resources->recovery_cache.entries))
								: 0;
	valid = valid && resources->data && resources->tasks && resources->files &&
		resources->consumers && resources->pending_consumers && resources->requested &&
		resources->physical_to_logical && resources->origin_tickets &&
		resources->recovery_cache.members &&
		(!resources->recovery_cache.limit || resources->recovery_cache.entries);
	if (valid && !resources->parametric) {
		struct jx *record;
		void *iterator = 0;
		while ((record = jx_iterate_array(jx_lookup(root, "data"), &iterator)))
			valid &= itable_insert(resources->data,
					vine_datavine_ir_data_id(record),
					record);
		iterator = 0;
		while ((record = jx_iterate_array(jx_lookup(root, "tasks"), &iterator))) {
			struct jx *input;
			void *input_iterator = 0;
			while ((input = jx_iterate_array(vine_datavine_ir_task_inputs(record),
						&input_iterator))) {
				uint64_t data_id = vine_datavine_ir_input_data_id(input);
				uintptr_t count = (uintptr_t)itable_lookup(
						resources->consumers, data_id);
				valid &= itable_insert(resources->consumers, data_id, (void *)(count + 1));
			}
		}
		iterator = 0;
		while ((record = jx_iterate_array(jx_lookup(root, "requested_outputs"),
					&iterator)))
			valid &= itable_insert(resources->requested,
					(uint64_t)record->u.integer_value,
					record);
	}
	/* Source and generated DataIDs are resolved by the Worker Data Agent.
	 * Creating Manager vine_file objects here is a v2 boundary violation. */
	valid = valid && restore_completed_tasks(runtime, workflow_id,
			&resources->recovered_tasks);
	if (valid)
		resources->scheduler = resources->parametric
				? vine_datavine_parametric_scheduler_create(resources->parametric)
				: 0;
	if (valid && !resources->parametric)
		valid = build_scheduler(root, resources->data, resources->tasks,
				&resources->scheduler);
	else
		valid = valid && resources->scheduler;
	if (valid) {
		struct jx *policy = jx_lookup(root, "policy");
		uint64_t maximum_tasks = policy
							 ? (uint64_t)jx_lookup_integer(policy,
									   "maximum_tasks")
							 : 0;
		uint64_t initial_tasks = resources->parametric
				? resources->parametric->tasks
				: (uint64_t)jx_array_length(jx_lookup(root, "tasks"));
		uint64_t task_id;
		void *task_value;
		int task_iterator;
		if (resources->parametric) {
			resources->maximum_task_id = resources->parametric->tasks;
		} else {
			ITABLE_ITERATE(resources->tasks, task_iterator, task_id, task_value)
			{
				if (task_id > resources->maximum_task_id)
					resources->maximum_task_id = task_id;
			}
		}
		if (maximum_tasks > initial_tasks)
			resources->maximum_task_id += maximum_tasks - initial_tasks;
	}
	resources->attempts = valid
						  ? calloc((size_t)resources->maximum_task_id + 1,
								sizeof(*resources->attempts))
						  : 0;
	resources->recovery_marks = valid
							? calloc((size_t)resources->maximum_task_id + 1,
									  sizeof(*resources->recovery_marks))
							: 0;
	resources->recovery_queue = valid
							? malloc(((size_t)resources->maximum_task_id + 1) *
									  sizeof(*resources->recovery_queue))
							: 0;
	resources->recovery_pending = valid && resources->parametric
			? calloc((size_t)resources->maximum_task_id + 1,
					sizeof(*resources->recovery_pending))
			: 0;
	resources->recovery_active = valid && resources->parametric
			? calloc((size_t)resources->maximum_task_id + 1,
					sizeof(*resources->recovery_active))
			: 0;
	resources->recovery_active_tasks = valid && resources->parametric
			? malloc(((size_t)resources->maximum_task_id + 1) *
					sizeof(*resources->recovery_active_tasks))
			: 0;
	valid = valid && resources->attempts && resources->recovery_marks &&
		resources->recovery_queue;
	if (resources->parametric)
		valid = valid && resources->recovery_pending &&
			resources->recovery_active && resources->recovery_active_tasks;
	resources->parametric_remaining = valid && resources->parametric
			? calloc((size_t)resources->maximum_task_id + 1,
					sizeof(*resources->parametric_remaining))
			: 0;
	if (resources->parametric) {
		valid = valid && resources->parametric_remaining;
		for (uint64_t task_id = 1; valid && task_id <=
				resources->parametric->a_tasks; task_id++)
			resources->parametric_remaining[task_id] =
					VINE_DATAVINE_PARAMETRIC_B_REUSE;
		for (uint64_t task_id = resources->parametric->a_tasks + 1;
				valid && task_id <= resources->parametric->a_tasks +
					resources->parametric->b_tasks; task_id++)
			resources->parametric_remaining[task_id] = 1;
	}
	if (valid)
		valid = vine_datavine_workflow_store_task_attempts_snapshot(
				runtime->store, workflow_id, resources->attempts, (size_t)resources->maximum_task_id + 1);
	return valid;
}

static int execute_document(struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, const char *document, size_t document_size)
{
	struct timespec execution_started;
	clock_gettime(CLOCK_MONOTONIC, &execution_started);
	struct execution_resources resources;
	int valid = execution_resources_create(&resources, runtime, workflow_id, document, document_size);
	struct execution_mailbox *mailbox = &resources.mailbox;
	struct itable *data = resources.data;
	struct itable *tasks = resources.tasks;
	struct itable *files = resources.files;
	struct itable *consumers = resources.consumers;
	struct itable *pending_consumers = resources.pending_consumers;
	struct itable *requested = resources.requested;
	struct itable *physical_to_logical = resources.physical_to_logical;
	struct vine_datavine_scheduler *scheduler = resources.scheduler;
	uint32_t *attempts = resources.attempts;
	if (valid)
		valid = vine_datavine_data_controller_prepare_workflow(
				runtime->data_controller, workflow_id);
	uint64_t maximum_task_id = resources.maximum_task_id;
	const char *metrics_setting = getenv("DATAVINE_WORKFLOW_METRICS");
	const char *profile_path = getenv("DATAVINE_PROFILE_PATH");
	int stderr_metrics = metrics_setting && strcmp(metrics_setting, "0");
	int report_metrics = stderr_metrics || (profile_path && profile_path[0]);
	struct vine_stats manager_stats_before = {0};
	struct vine_stats manager_stats_after = {0};
	if (report_metrics) {
		manager_lane_lock(runtime);
		vine_get_stats(runtime->manager, &manager_stats_before);
		pthread_mutex_unlock(&runtime->manager_lock);
	}
	if (report_metrics && !valid)
		fprintf(stderr, "datavine workflow %s setup_failed=1\n", workflow_id);
	struct timespec setup_finished;
	clock_gettime(CLOCK_MONOTONIC, &setup_finished);
	uint64_t running = 0;
	uint64_t publishing = 0;
	struct pending_publication *pending_publications = 0;
	uint64_t generation_cursor = 1;
	uint32_t uncheckpointed_completions = 0;
	uint64_t materialize_nanoseconds = 0;
	uint64_t submit_nanoseconds = 0;
	uint64_t manager_lock_nanoseconds = 0;
	uint64_t submission_event_nanoseconds = 0;
	uint64_t publish_nanoseconds = 0;
	struct publication_stage_totals publication_stages = {0};
	struct publication_stage_totals recovery_stages = {0};
	uint64_t completion_event_nanoseconds = 0;
	uint64_t checkpoint_nanoseconds = 0;
	uint64_t physical_submissions = 0;
	uint64_t physical_completions = 0;
	uint64_t scheduler_wait_microseconds = 0;
	uint64_t stage_in_microseconds = 0;
	uint64_t worker_execute_microseconds = 0;
	uint64_t stage_out_microseconds = 0;
	uint64_t task_bytes_sent = 0;
	uint64_t task_bytes_received = 0;
	uint64_t recovery_epochs = 0;
	uint64_t recovery_lost_data = 0;
	uint64_t recovery_invalidated_tasks = 0;
	uint64_t recovery_inflight = 0;
	uint64_t observed_data_losses = UINT64_MAX;
	int quiescent = 0;
	int recovered_applied = 0;
	const char *failure_stage = 0;
	while (valid && !runtime_stopping(runtime)) {
		valid = drain_publications(runtime, workflow_id, scheduler, &resources.recovery_cache, data, files, pending_consumers, requested, &pending_publications, &publishing, 0, &uncheckpointed_completions, &publish_nanoseconds, &publication_stages, &completion_event_nanoseconds);
		if (!valid)
			break;
		struct vine_datavine_workflow_info workflow_info;
		if (!vine_datavine_workflow_store_describe(runtime->store, workflow_id, &workflow_info) ||
				workflow_info.state == VINE_DATAVINE_WORKFLOW_CANCELLED) {
			valid = 0;
			break;
		}
		while (valid && generation_cursor < workflow_info.generation) {
			uint64_t delta_generation = 0;
			struct jx *delta_root =
					vine_datavine_workflow_store_take_delta_root(
							runtime->store, workflow_id, generation_cursor, &delta_generation);
			if (!delta_root || delta_generation != generation_cursor + 1) {
				if (delta_root)
					jx_delete(delta_root);
				valid = 0;
				break;
			}
			valid = delta_root && apply_delta_root(delta_root, scheduler, data, tasks, consumers, pending_consumers, requested, recovered_applied);
			if (valid)
				valid = task_defaults_index_add(&resources.task_defaults,
						delta_root);
			if (!valid) {
				if (delta_root)
					jx_delete(delta_root);
				break;
			}
			valid = retained_root_add(&resources.roots, delta_root);
			if (!valid)
				break;
			struct jx *new_task;
			void *new_iterator = 0;
			while ((new_task = jx_iterate_array(jx_lookup(delta_root, "tasks"),
						&new_iterator))) {
				uint64_t new_id = vine_datavine_ir_task_id(new_task);
				if (new_id > maximum_task_id) {
					valid = 0;
					break;
				}
				attempts[new_id] = vine_datavine_workflow_store_task_attempts(
						runtime->store, workflow_id, (int64_t)new_id);
			}
			generation_cursor = delta_generation;
		}
		if (!valid)
			break;
		if (!recovered_applied) {
			uint64_t *invalidated = 0;
			size_t invalidated_count = 0;
			valid = resources.parametric
					? prepare_recovered_parametric(runtime, workflow_id, &resources,
							&invalidated, &invalidated_count)
					: prepare_recovered_tasks(runtime, workflow_id, data, tasks,
							pending_consumers, requested, maximum_task_id,
							&resources.recovered_tasks, &invalidated,
							&invalidated_count);
			if (valid && invalidated_count)
				valid = record_recovery_invalidations(runtime, workflow_id, invalidated, invalidated_count, attempts, &completion_event_nanoseconds);
			size_t recovered_size = 0;
			const char *recovered = buffer_tolstring(&resources.recovered_tasks,
					&recovered_size);
			if (valid && recovered_size)
				valid = vine_datavine_scheduler_rebuild(scheduler, recovered, recovered_size);
			if (valid && resources.parametric && invalidated_count) {
				for (size_t index = invalidated_count; valid && index > 0; index--) {
					uint64_t task_id = invalidated[index - 1];
					valid = task_id > 0 && task_id <= maximum_task_id &&
							!resources.recovery_pending[task_id];
					if (valid) {
						resources.recovery_queue[resources.recovery_tail++] = task_id;
						resources.recovery_pending[task_id] = 1;
					}
				}
			}
			free(invalidated);
			recovered_applied = 1;
			if (!valid)
				break;
		}
		uint64_t data_losses = vine_datavine_data_controller_loss_events(
				runtime->data_controller);
		if (data_losses != observed_data_losses) {
			observed_data_losses = data_losses;
			manager_lane_lock(runtime);
			uint64_t *lost_data_ids = 0;
			size_t lost_count = 0;
			valid = vine_datavine_data_controller_take_workflow_losses(
					runtime->data_controller, runtime->manager, workflow_id, files, &lost_data_ids, &lost_count);
			pthread_mutex_unlock(&runtime->manager_lock);
			if (valid && lost_count) {
				size_t invalidated = 0;
				size_t invalidated_first = 0;
				valid = resources.parametric
						? apply_parametric_losses(runtime, workflow_id, lost_data_ids,
								lost_count, &resources, &invalidated_first,
								&invalidated)
						: apply_workflow_losses(runtime, workflow_id, lost_data_ids,
								lost_count, data, tasks, pending_consumers, requested,
								&resources.recovery_cache, scheduler, attempts,
								maximum_task_id, resources.recovery_marks,
								resources.recovery_queue, &resources.recovery_epoch,
								&completion_event_nanoseconds, &invalidated);
				if (valid && resources.parametric && invalidated)
					valid = record_recovery_invalidations(runtime, workflow_id,
							resources.recovery_queue + invalidated_first,
							invalidated, attempts,
							&completion_event_nanoseconds);
				/* Ancestors were appended after their consumers. Submit in reverse
				 * so their data is normally available first; a child dispatched
				 * early still waits in its Worker Agent without consuming cores. */
				if (valid && invalidated && !resources.parametric) {
					manager_lane_lock(runtime);
					for (size_t index = invalidated; valid && index > 0; index--) {
						uint64_t task_id = resources.recovery_queue[index - 1];
						uint32_t attempt = attempts[task_id] + 1;
						struct parametric_task_view *parametric_view = 0;
						struct vine_task *physical = resources.parametric
								? materialize_parametric(runtime, workflow_id, &resources,
										task_id, attempt, 1, &parametric_view)
								: materialize(runtime->manager,
										runtime->data_controller, workflow_id,
										itable_lookup(tasks, task_id), data, files,
										pending_consumers, resources.origin_tickets,
										requested,
										&resources.task_defaults, attempt, 1);
						struct physical_attempt *mapping = physical
								? calloc(1, sizeof(*mapping)) : 0;
						if (mapping) {
							mapping->logical_id = (int64_t)task_id;
							mapping->attempt = attempt;
							mapping->recovery = 1;
							mapping->parametric_view = parametric_view;
						}
						int physical_id = mapping
								? runtime_submit_locked(runtime, physical, mailbox) : -1;
						valid = physical_id > 0 && itable_insert(
								physical_to_logical, (uint64_t)physical_id, mapping);
						if (valid) {
							attempts[task_id] = attempt;
							running++;
							physical_submissions++;
						} else {
							physical_attempt_delete(mapping);
							if (!mapping)
								parametric_task_view_delete(parametric_view);
							if (physical_id < 1 && physical)
								vine_task_delete(physical);
						}
					}
					pthread_mutex_unlock(&runtime->manager_lock);
				}
				recovery_epochs++;
				recovery_lost_data += lost_count;
				recovery_invalidated_tasks += invalidated;
			}
			free(lost_data_ids);
		}
		if (!valid)
			break;
		if (resources.parametric && !recovery_inflight &&
				resources.recovery_head == resources.recovery_tail &&
				resources.recovery_active_count) {
			valid = parametric_recovery_finish(runtime, workflow_id, &resources);
			if (!valid) {
				failure_stage = "recovery_finish";
				break;
			}
		}
		int64_t logical_id;
		struct timespec manager_lock_started;
		struct timespec manager_lock_finished;
		clock_gettime(CLOCK_MONOTONIC, &manager_lock_started);
		manager_lane_lock(runtime);
		clock_gettime(CLOCK_MONOTONIC, &manager_lock_finished);
		manager_lock_nanoseconds += elapsed_nanoseconds(
				&manager_lock_started, &manager_lock_finished);
		struct vine_datavine_workflow_task_event_record
				submission_events[DATAVINE_WORKFLOW_EVENT_BATCH];
		size_t submission_event_count = 0;
		while (valid && resources.parametric &&
				running < VINE_DATAVINE_WORKFLOW_SUBMISSION_WINDOW +
					VINE_DATAVINE_WORKFLOW_RECOVERY_RESERVE &&
				submission_event_count < DATAVINE_WORKFLOW_EVENT_BATCH &&
				resources.recovery_head < resources.recovery_tail) {
			uint64_t task_id = resources.recovery_queue[resources.recovery_head++];
			uint32_t attempt = attempts[task_id] + 1;
			struct parametric_task_view *parametric_view = 0;
			struct timespec stage_started;
			struct timespec stage_finished;
			clock_gettime(CLOCK_MONOTONIC, &stage_started);
			valid = parametric_recovery_begin(runtime, workflow_id, &resources,
					task_id);
			struct vine_task *physical = valid
					? materialize_parametric(runtime, workflow_id, &resources,
						task_id, attempt, 1, &parametric_view)
					: 0;
			/* Recovery producers must pass consumers already queued behind the
			 * missing generation. */
			if (physical)
				vine_task_set_priority(physical, 1e12);
			clock_gettime(CLOCK_MONOTONIC, &stage_finished);
			materialize_nanoseconds += elapsed_nanoseconds(&stage_started,
					&stage_finished);
			struct physical_attempt *mapping = physical
					? calloc(1, sizeof(*mapping)) : 0;
			if (mapping) {
				mapping->logical_id = (int64_t)task_id;
				mapping->attempt = attempt;
				mapping->recovery = 1;
				mapping->parametric_view = parametric_view;
			}
			clock_gettime(CLOCK_MONOTONIC, &stage_started);
			int physical_id = mapping
					? runtime_submit_locked(runtime, physical, mailbox) : -1;
			clock_gettime(CLOCK_MONOTONIC, &stage_finished);
			submit_nanoseconds += elapsed_nanoseconds(&stage_started,
					&stage_finished);
			valid = physical_id > 0 && mapping &&
				itable_insert(physical_to_logical, (uint64_t)physical_id, mapping);
			if (valid) {
				attempts[task_id] = attempt;
				running++;
				recovery_inflight++;
				physical_submissions++;
				submission_events[submission_event_count++] =
						(struct vine_datavine_workflow_task_event_record){
								.type = VINE_DATAVINE_WORKFLOW_TASK_SUBMITTED,
								.task_id = (int64_t)task_id,
								.attempt = attempt,
						};
			} else {
				failure_stage = physical ? "recovery_submit"
						: "recovery_materialize";
				resources.recovery_pending[task_id] = 0;
				physical_attempt_delete(mapping);
				if (!mapping)
					parametric_task_view_delete(parametric_view);
				if (physical_id < 1 && physical)
					vine_task_delete(physical);
			}
		}
		if (resources.parametric &&
				resources.recovery_head == resources.recovery_tail)
			resources.recovery_head = resources.recovery_tail = 0;
		while (running < VINE_DATAVINE_WORKFLOW_SUBMISSION_WINDOW &&
				submission_event_count < DATAVINE_WORKFLOW_EVENT_BATCH &&
				(logical_id = vine_datavine_scheduler_take(scheduler)) > 0) {
			struct jx *task = resources.parametric ? 0
					: itable_lookup(tasks, (uint64_t)logical_id);
			struct parametric_task_view *parametric_view = 0;
			struct timespec stage_started;
			struct timespec stage_finished;
			clock_gettime(CLOCK_MONOTONIC, &stage_started);
			struct vine_task *physical = resources.parametric
					? materialize_parametric(runtime, workflow_id, &resources,
							(uint64_t)logical_id, attempts[logical_id] + 1,
							workflow_info.state != VINE_DATAVINE_WORKFLOW_RUNNING,
							&parametric_view)
					: materialize(runtime->manager, runtime->data_controller,
							workflow_id, task, data, files, pending_consumers,
							resources.origin_tickets, requested,
							&resources.task_defaults,
							attempts[logical_id] + 1,
							workflow_info.state != VINE_DATAVINE_WORKFLOW_RUNNING);
			clock_gettime(CLOCK_MONOTONIC, &stage_finished);
			materialize_nanoseconds += elapsed_nanoseconds(
					&stage_started, &stage_finished);
			if (!physical) {
				if (report_metrics)
					fprintf(stderr, "datavine workflow %s materialize_failed task=%lld\n", workflow_id, (long long)logical_id);
				valid = 0;
				break;
			}
			clock_gettime(CLOCK_MONOTONIC, &stage_started);
			int physical_id = runtime_submit_locked(runtime, physical, mailbox);
			clock_gettime(CLOCK_MONOTONIC, &stage_finished);
			submit_nanoseconds += elapsed_nanoseconds(&stage_started, &stage_finished);
			struct physical_attempt *mapping = physical_id > 0
					? calloc(1, sizeof(*mapping)) : 0;
			if (mapping) {
				mapping->logical_id = logical_id;
				mapping->attempt = attempts[logical_id] + 1;
				mapping->parametric_view = parametric_view;
			}
			if (physical_id < 1 || !mapping || !itable_insert(physical_to_logical,
								   (uint64_t)physical_id, mapping)) {
				if (report_metrics)
					fprintf(stderr, "datavine workflow %s submit_failed task=%lld physical=%d\n", workflow_id, (long long)logical_id, physical_id);
				physical_attempt_delete(mapping);
				if (!mapping)
					parametric_task_view_delete(parametric_view);
				if (physical_id < 1)
					vine_task_delete(physical);
				valid = 0;
				break;
			}
			running++;
			physical_submissions++;
			attempts[logical_id]++;
			submission_events[submission_event_count++] =
					(struct vine_datavine_workflow_task_event_record){
							.type = VINE_DATAVINE_WORKFLOW_TASK_SUBMITTED,
							.task_id = logical_id,
							.attempt = attempts[logical_id],
					};
		}
		pthread_mutex_unlock(&runtime->manager_lock);
		if (submission_event_count) {
			struct vine_datavine_workflow_error event_error;
			struct timespec event_started;
			struct timespec event_finished;
			clock_gettime(CLOCK_MONOTONIC, &event_started);
			int recorded = vine_datavine_workflow_store_record_task_events(
					runtime->store, workflow_id, submission_events, submission_event_count, &event_error);
			clock_gettime(CLOCK_MONOTONIC, &event_finished);
			submission_event_nanoseconds += elapsed_nanoseconds(
					&event_started, &event_finished);
			if (!recorded && report_metrics)
				fprintf(stderr, "datavine workflow %s event_failed code=%d path=%s detail=%s\n", workflow_id, event_error.code, event_error.path, event_error.message);
			valid = valid && recorded;
		}
		if (!publishing && vine_datavine_scheduler_complete(scheduler)) {
			int results_ready = resources.parametric
					? requested_parametric_results_ready(runtime, workflow_id,
							resources.parametric)
					: requested_results_ready(runtime, workflow_id, requested);
			if (!results_ready) {
				usleep(1000);
				continue;
			}
			if (workflow_info.state == VINE_DATAVINE_WORKFLOW_RUNNING)
				break;
			if (workflow_info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN) {
				struct vine_datavine_workflow_error quiescent_error;
				int quiescent_status = vine_datavine_workflow_store_mark_quiescent(
						runtime->store,
						workflow_id,
						workflow_info.generation,
						&workflow_info,
						&quiescent_error);
				if (!quiescent_status) {
					valid = 0;
					break;
				}
				if (quiescent_status == 2)
					continue;
			}
			quiescent = valid;
			break;
		}
		if (!running) {
			if (publishing) {
				valid = drain_publications(runtime, workflow_id, scheduler, &resources.recovery_cache, data, files, pending_consumers, requested, &pending_publications, &publishing, 1, &uncheckpointed_completions, &publish_nanoseconds, &publication_stages, &completion_event_nanoseconds);
				continue;
			}
			valid = 0;
			break;
		}
		struct vine_task *completed = runtime_wait(runtime, mailbox, 1);
		if (!completed)
			continue;
		int drained_completions = 0;
		struct vine_datavine_workflow_task_event_record
				parametric_completion_events[DATAVINE_WORKFLOW_EVENT_BATCH];
		size_t parametric_completion_event_count = 0;
		do {
			int physical_id = vine_task_get_id(completed);
			int64_t submitted_at = vine_task_get_metric(completed, "time_when_submitted");
			int64_t commit_started = vine_task_get_metric(completed, "time_when_commit_start");
			int64_t commit_finished = vine_task_get_metric(completed, "time_when_commit_end");
			int64_t retrieval_started = vine_task_get_metric(completed, "time_when_retrieval");
			int64_t done_at = vine_task_get_metric(completed, "time_when_done");
			if (commit_started >= submitted_at)
				scheduler_wait_microseconds += (uint64_t)(commit_started - submitted_at);
			if (commit_finished >= commit_started)
				stage_in_microseconds += (uint64_t)(commit_finished - commit_started);
			worker_execute_microseconds += (uint64_t)vine_task_get_metric(
					completed, "time_workers_execute_last");
			if (done_at >= retrieval_started)
				stage_out_microseconds += (uint64_t)(done_at - retrieval_started);
			task_bytes_sent += (uint64_t)vine_task_get_metric(completed, "bytes_sent");
			task_bytes_received += (uint64_t)vine_task_get_metric(completed, "bytes_received");
			pthread_mutex_lock(&runtime->routing_lock);
			itable_remove(runtime->completion_owners, (uint64_t)physical_id);
			pthread_mutex_unlock(&runtime->routing_lock);
			struct physical_attempt *mapping = itable_remove(
					physical_to_logical, (uint64_t)physical_id);
			int64_t completed_logical_id = mapping ? mapping->logical_id : 0;
			int recovery_attempt = mapping && mapping->recovery;
			running--;
			int recovery_accounted = !recovery_attempt || recovery_inflight > 0;
			if (recovery_attempt && recovery_inflight)
				recovery_inflight--;
			physical_completions++;
			int physical_success = completed_logical_id > 0 &&
						   vine_task_get_result(completed) == VINE_RESULT_SUCCESS &&
						   vine_task_get_exit_code(completed) == 0;
			int32_t task_result = vine_task_get_result(completed) == VINE_RESULT_SUCCESS
								  ? vine_task_get_exit_code(completed)
								  : -(int32_t)vine_task_get_result(completed);
			if (resources.parametric && physical_success) {
				struct vine_datavine_data_publication_metrics metrics;
				if (vine_datavine_data_controller_task_metrics(
						runtime->data_controller, completed, &metrics)) {
					struct publication_stage_totals *stages = recovery_attempt
							? &recovery_stages : &publication_stages;
					stages->function_nanoseconds +=
							metrics.function_nanoseconds;
					stages->task_reports += metrics.task_reports;
					stages->task_reported_read_bytes +=
							metrics.task_reported_read_bytes;
					stages->task_reported_cpu_milliseconds +=
							metrics.task_reported_cpu_milliseconds;
				}
			}
			if (report_metrics && !physical_success)
				fprintf(stderr,
						"datavine workflow %s task_failed logical_id=%lld "
						"result=%d exit_code=%d recovery=%d\n",
						workflow_id,
						(long long)completed_logical_id,
						vine_task_get_result(completed),
						vine_task_get_exit_code(completed), recovery_attempt);
			int task_valid = completed_logical_id > 0 && recovery_accounted;
			struct jx *task = task_valid
							  ? (resources.parametric
										? mapping->parametric_view->task
										: itable_lookup(tasks,
												(uint64_t)completed_logical_id))
							  : 0;
			uint32_t attempt = mapping ? mapping->attempt : 0;
			if (physical_success && recovery_attempt) {
				/* Worker commit already installed and asynchronously advertised
				 * the replacement generation. Logical state stays DONE. */
				task_valid = 1;
				if (resources.parametric)
					resources.recovery_pending[completed_logical_id] = 0;
			} else if (physical_success) {
				uint32_t attempt_limit = task_valid
									 ? (uint32_t)maximum_attempts(
											   &resources.task_defaults, task)
									 : 1;
				if (resources.parametric) {
					task_valid = finish_parametric_attempt(runtime, workflow_id,
							&resources, completed_logical_id, attempt,
							task_result, 1,
							&parametric_completion_events[
									parametric_completion_event_count++]);
				} else {
					task_valid = finish_logical_attempt(runtime, workflow_id,
							scheduler, &resources.recovery_cache, data, files,
							pending_consumers, requested, task,
							completed_logical_id, attempt, task_result,
							attempt_limit, 1, &completion_event_nanoseconds);
				}
				uncheckpointed_completions += completed_logical_id > 0;
			} else if ((task_result == -(int32_t)VINE_RESULT_FORSAKEN ||
					task_result == -(int32_t)VINE_RESULT_OUTPUT_TRANSFER_ERROR ||
					(recovery_attempt &&
					 task_result == -(int32_t)VINE_RESULT_OUTPUT_MISSING)) &&
					attempt < DATAVINE_WORKFLOW_INFRASTRUCTURE_ATTEMPTS) {
				/* A freshly connected Worker can reject a FunctionCall before its
				 * library process becomes READY, or lose it during connection churn.
				 * FORSAKEN and Worker-Agent output I/O failure are infrastructure.
				 * A deterministic recovery task has already produced its output once,
				 * so a missing replay output is also retried as transient Worker state;
				 * ordinary missing output remains an application failure. None of these
				 * consume the workflow retry budget. A recovery keeps logical DONE
				 * immutable; an ordinary task remains RUNNING. */
				uint32_t next_attempt = attempt + 1;
				struct parametric_task_view *retry_view = 0;
				int retry_retain_all = recovery_attempt ||
					workflow_info.state != VINE_DATAVINE_WORKFLOW_RUNNING;
				/* materialize() registers Worker-Agent data handles with the
				 * Manager. Keep that operation in the same narrow Manager lane as
				 * submit, just like the ordinary submission path. */
				manager_lane_lock(runtime);
				struct vine_task *retry = resources.parametric
						? materialize_parametric(runtime, workflow_id, &resources,
								(uint64_t)completed_logical_id, next_attempt,
								retry_retain_all,
								&retry_view)
						: materialize(runtime->manager, runtime->data_controller,
								workflow_id, task, data, files, pending_consumers,
								resources.origin_tickets, requested,
								&resources.task_defaults, next_attempt,
								retry_retain_all);
				struct physical_attempt *retry_mapping = retry
						? calloc(1, sizeof(*retry_mapping)) : 0;
				if (retry_mapping) {
					retry_mapping->logical_id = completed_logical_id;
					retry_mapping->attempt = next_attempt;
					retry_mapping->recovery = recovery_attempt;
					retry_mapping->parametric_view = retry_view;
				}
				int retry_physical_id = retry_mapping
						? runtime_submit_locked(runtime, retry, mailbox) : -1;
				task_valid = retry_physical_id > 0 &&
					itable_insert(physical_to_logical,
							(uint64_t)retry_physical_id, retry_mapping);
				pthread_mutex_unlock(&runtime->manager_lock);
				if (task_valid) {
					attempts[completed_logical_id] = next_attempt;
					running++;
					if (recovery_attempt)
						recovery_inflight++;
					physical_submissions++;
					struct vine_datavine_workflow_task_event_record retry_events[] = {
							{.type = VINE_DATAVINE_WORKFLOW_TASK_RETRY,
							 .task_id = completed_logical_id,
							 .attempt = attempt,
							 .result = task_result},
							{.type = VINE_DATAVINE_WORKFLOW_TASK_SUBMITTED,
							 .task_id = completed_logical_id,
							 .attempt = next_attempt},
					};
					struct vine_datavine_workflow_error retry_error = {0};
					task_valid = vine_datavine_workflow_store_record_task_events(
							runtime->store, workflow_id, retry_events,
							2, &retry_error);
					if (!task_valid && report_metrics) {
						fprintf(stderr,
								"datavine workflow %s retry_event_failed "
								"logical_id=%lld attempt=%u code=%d path=%s detail=%s\n",
								workflow_id, (long long)completed_logical_id,
								next_attempt, retry_error.code, retry_error.path,
								retry_error.message);
						failure_stage = "retry_event";
					}
				} else {
					if (report_metrics)
						fprintf(stderr,
								"datavine workflow %s retry_submit_failed "
								"logical_id=%lld attempt=%u materialized=%d "
								"mapping=%d physical=%d\n",
								workflow_id, (long long)completed_logical_id,
								next_attempt, retry != 0, retry_mapping != 0,
								retry_physical_id);
					failure_stage = retry ? "retry_submit" : "retry_materialize";
					physical_attempt_delete(retry_mapping);
					if (!retry_mapping)
						parametric_task_view_delete(retry_view);
					if (retry_physical_id < 1 && retry)
						vine_task_delete(retry);
				}
			} else if (recovery_attempt) {
				/* Non-transient replay failures are explicit and fail closed. */
				if (resources.parametric)
					resources.recovery_pending[completed_logical_id] = 0;
				failure_stage = "recovery_task";
				task_valid = 0;
			} else {
				uint32_t attempt_limit = task_valid
									 ? (uint32_t)maximum_attempts(
											   &resources.task_defaults, task)
									 : 1;
				if (resources.parametric) {
					task_valid = finish_parametric_attempt(runtime, workflow_id,
							&resources, completed_logical_id, attempt,
							task_result, 0,
							&parametric_completion_events[
									parametric_completion_event_count++]);
				} else {
					task_valid = finish_logical_attempt(runtime, workflow_id,
							scheduler, &resources.recovery_cache, data, files,
							pending_consumers, requested, task,
							completed_logical_id, attempt, task_result,
							attempt_limit, 0, &completion_event_nanoseconds);
				}
				uncheckpointed_completions += completed_logical_id > 0;
			}
			physical_attempt_delete(mapping);
			vine_task_delete(completed);
			valid &= task_valid;
			if (valid && uncheckpointed_completions >=
							DATAVINE_WORKFLOW_CHECKPOINT_COMPLETIONS) {
				struct timespec checkpoint_started;
				struct timespec checkpoint_finished;
				clock_gettime(CLOCK_MONOTONIC, &checkpoint_started);
				valid = vine_datavine_workflow_store_checkpoint(runtime->store,
						workflow_id);
				clock_gettime(CLOCK_MONOTONIC, &checkpoint_finished);
				checkpoint_nanoseconds += elapsed_nanoseconds(
						&checkpoint_started, &checkpoint_finished);
				uncheckpointed_completions = 0;
			}
			drained_completions++;
			completed = valid &&
					drained_completions < DATAVINE_WORKFLOW_EVENT_BATCH
							? runtime_wait(runtime, mailbox, 0)
							: 0;
		} while (completed);
		if (parametric_completion_event_count) {
			struct vine_datavine_workflow_error event_error;
			struct timespec event_started;
			struct timespec event_finished;
			clock_gettime(CLOCK_MONOTONIC, &event_started);
			int recorded = vine_datavine_workflow_store_record_task_events(
					runtime->store, workflow_id, parametric_completion_events,
					parametric_completion_event_count, &event_error);
			clock_gettime(CLOCK_MONOTONIC, &event_finished);
			completion_event_nanoseconds += elapsed_nanoseconds(
					&event_started, &event_finished);
			valid = valid && recorded;
		}
	}
	if (running)
		cancel_running_tasks(runtime, mailbox, physical_to_logical, running);
	if (report_metrics && !valid)
		fprintf(stderr,
				"datavine workflow %s runtime_invalid stage=%s running=%llu\n",
				workflow_id, failure_stage ? failure_stage : "unspecified",
				(unsigned long long)running);
	pending_publications_delete(pending_publications);
	if (uncheckpointed_completions) {
		struct timespec checkpoint_started;
		struct timespec checkpoint_finished;
		clock_gettime(CLOCK_MONOTONIC, &checkpoint_started);
		valid = vine_datavine_workflow_store_checkpoint(runtime->store,
					workflow_id) &&
			valid;
		clock_gettime(CLOCK_MONOTONIC, &checkpoint_finished);
		checkpoint_nanoseconds += elapsed_nanoseconds(
				&checkpoint_started, &checkpoint_finished);
	}
	size_t recovery_cache_active = resources.recovery_cache.members
							   ? (size_t)itable_size(resources.recovery_cache.members)
							   : 0;
	size_t recovery_cache_peak = resources.recovery_cache.peak;
	size_t recovery_cache_limit = resources.recovery_cache.limit;
	uint64_t recovery_cache_evictions = resources.recovery_cache.evictions;
	execution_resources_delete(&resources);
	if (report_metrics) {
		manager_lane_lock(runtime);
		vine_get_stats(runtime->manager, &manager_stats_after);
		pthread_mutex_unlock(&runtime->manager_lock);
	}
	size_t controller_active_results = 0;
	size_t controller_peak_results = 0;
	if (report_metrics)
		vine_datavine_data_controller_result_counts(runtime->data_controller,
				workflow_id,
				&controller_active_results,
				&controller_peak_results);
	struct vine_datavine_agent_stats agent_stats = {0};
	if (report_metrics) {
		unsigned char workflow_key[32];
		if (vine_datavine_data_controller_workflow_key(runtime->data_controller,
				workflow_id, workflow_key)) {
			uint64_t workflow_slot = vine_datavine_get_u64(workflow_key);
			if (!workflow_slot)
				workflow_slot = 1;
			vine_datavine_data_controller_agent_stats(runtime->data_controller,
					workflow_slot, &agent_stats);
		}
	}
	struct timespec execution_finished;
	clock_gettime(CLOCK_MONOTONIC, &execution_finished);
	double setup_seconds = (setup_finished.tv_sec - execution_started.tv_sec) +
				   (setup_finished.tv_nsec - execution_started.tv_nsec) / 1e9;
	double run_seconds = (execution_finished.tv_sec - setup_finished.tv_sec) +
				 (execution_finished.tv_nsec - setup_finished.tv_nsec) / 1e9;
	if (report_metrics) {
		FILE *metrics_stream = stderr_metrics ? stderr : fopen(profile_path, "a");
		if (!metrics_stream)
			metrics_stream = stderr;
		double task_count = physical_completions ? physical_completions : 1;
		double core_count = 1;
		if (manager_stats_after.total_cores > 0)
			core_count = manager_stats_after.total_cores;
		double url_stage_in_seconds =
				(manager_stats_after.time_url_received -
						manager_stats_before.time_url_received) /
				1e6;
		double queue_mean_seconds =
				scheduler_wait_microseconds / 1e6 / task_count;
		double scheduler_delay_seconds = queue_mean_seconds -
						 (url_stage_in_seconds < queue_mean_seconds
										 ? url_stage_in_seconds
										 : queue_mean_seconds);
		double worker_parallel_seconds =
				worker_execute_microseconds / 1e6 / core_count;
		double function_parallel_seconds =
				publication_stages.function_nanoseconds / 1e9 / core_count;
		double object_pull_parallel_seconds =
				publication_stages.pull_nanoseconds / 1e9 / core_count;
		double decode_parallel_seconds =
				publication_stages.decode_nanoseconds / 1e9 / core_count;
		double serialize_parallel_seconds =
				publication_stages.serialize_nanoseconds / 1e9 / core_count;
		double worker_overhead_seconds = worker_parallel_seconds -
						 object_pull_parallel_seconds - function_parallel_seconds - decode_parallel_seconds -
						 serialize_parallel_seconds;
		if (worker_overhead_seconds < 0)
			worker_overhead_seconds = 0;
		double dominant_value = scheduler_delay_seconds;
		const char *dominant_stage = "scheduler_delay";
#define DOMINANT_STAGE(name, value) \
	if ((value) > dominant_value) { \
		dominant_value = (value); \
		dominant_stage = (name); \
	}
		DOMINANT_STAGE("data_stage_in", url_stage_in_seconds);
		DOMINANT_STAGE("data_object_pull", object_pull_parallel_seconds);
		DOMINANT_STAGE("python_function", function_parallel_seconds);
		DOMINANT_STAGE("python_decode", decode_parallel_seconds);
		DOMINANT_STAGE("python_serialize", serialize_parallel_seconds);
		DOMINANT_STAGE("worker_overhead", worker_overhead_seconds);
		DOMINANT_STAGE("data_publish", publish_nanoseconds / 1e9);
#undef DOMINANT_STAGE
		fprintf(metrics_stream,
				"datavine workflow %s setup_seconds=%.6f run_seconds=%.6f "
				"physical_submissions=%llu physical_completions=%llu "
				"materialize_seconds=%.6f submit_seconds=%.6f "
				"manager_lock_seconds=%.6f "
				"submission_event_seconds=%.6f publish_seconds=%.6f "
				"publication_prepare_seconds=%.6f "
				"publication_queue_seconds=%.6f "
				"publication_commit_seconds=%.6f "
				"python_object_pull_seconds=%.6f "
				"python_decode_seconds=%.6f python_function_seconds=%.6f "
				"python_serialize_seconds=%.6f "
				"python_fsync_seconds=%.6f "
				"publication_outputs=%llu publication_remote_outputs=%llu "
				"publication_durable_outputs=%llu publication_bytes=%llu "
				"publication_journal_records=%llu "
				"manager_bytes_sent=%lld manager_bytes_received=%lld "
				"manager_time_send_good_us=%lld "
				"manager_time_receive_good_us=%lld "
				"manager_time_scheduling_us=%lld "
				"manager_time_workers_execute_good_us=%lld "
				"manager_workers_removed=%lld "
				"controller_active_results=%llu controller_peak_results=%llu "
				"agent_active_data=%llu agent_active_replicas=%llu "
				"agent_active_waiters=%llu agent_active_sessions=%llu "
				"agent_peak_data=%llu agent_peak_replicas=%llu "
				"agent_peak_waiters=%llu "
				"recovery_cache_active=%llu recovery_cache_peak=%llu "
				"recovery_cache_limit=%llu recovery_cache_evictions=%llu "
				"recovery_epochs=%llu recovery_lost_data=%llu "
				"recovery_invalidated_tasks=%llu "
				"scheduler_wait_seconds=%.6f stage_in_seconds=%.6f "
				"worker_execute_seconds=%.6f stage_out_seconds=%.6f "
				"scheduler_delay_seconds=%.6f url_stage_in_seconds=%.6f "
				"url_stage_in_bytes=%lld worker_parallel_seconds=%.6f "
				"task_bytes_sent=%llu task_bytes_received=%llu "
				"task_reports=%llu task_reported_read_bytes=%llu "
				"task_reported_cpu_milliseconds=%llu "
				"recovery_task_reports=%llu "
				"recovery_task_reported_read_bytes=%llu "
				"recovery_task_reported_cpu_milliseconds=%llu "
				"dominant_stage=%s dominant_stage_seconds=%.6f "
				"completion_event_seconds=%.6f checkpoint_seconds=%.6f\n",
				workflow_id,
				setup_seconds,
				run_seconds,
				(unsigned long long)physical_submissions,
				(unsigned long long)physical_completions,
				materialize_nanoseconds / 1e9,
				submit_nanoseconds / 1e9,
				manager_lock_nanoseconds / 1e9,
				submission_event_nanoseconds / 1e9,
				publish_nanoseconds / 1e9,
				publication_stages.prepare_nanoseconds / 1e9,
				publication_stages.queue_nanoseconds / 1e9,
				publication_stages.commit_nanoseconds / 1e9,
				publication_stages.pull_nanoseconds / 1e9,
				publication_stages.decode_nanoseconds / 1e9,
				publication_stages.function_nanoseconds / 1e9,
				publication_stages.serialize_nanoseconds / 1e9,
				publication_stages.fsync_nanoseconds / 1e9,
				(unsigned long long)publication_stages.outputs,
				(unsigned long long)publication_stages.remote_outputs,
				(unsigned long long)publication_stages.durable_outputs,
				(unsigned long long)publication_stages.output_bytes,
				(unsigned long long)publication_stages.journal_records,
				(long long)(manager_stats_after.bytes_sent -
						manager_stats_before.bytes_sent),
				(long long)(manager_stats_after.bytes_received -
						manager_stats_before.bytes_received),
				(long long)(manager_stats_after.time_send_good -
						manager_stats_before.time_send_good),
				(long long)(manager_stats_after.time_receive_good -
						manager_stats_before.time_receive_good),
				(long long)(manager_stats_after.time_scheduling -
						manager_stats_before.time_scheduling),
				(long long)(manager_stats_after.time_workers_execute_good -
						manager_stats_before.time_workers_execute_good),
				(long long)(manager_stats_after.workers_removed -
						manager_stats_before.workers_removed),
				(unsigned long long)controller_active_results,
				(unsigned long long)controller_peak_results,
				(unsigned long long)agent_stats.active_data,
				(unsigned long long)agent_stats.active_replicas,
				(unsigned long long)agent_stats.active_waiters,
				(unsigned long long)agent_stats.active_sessions,
				(unsigned long long)agent_stats.peak_data,
				(unsigned long long)agent_stats.peak_replicas,
				(unsigned long long)agent_stats.peak_waiters,
				(unsigned long long)recovery_cache_active,
				(unsigned long long)recovery_cache_peak,
				(unsigned long long)recovery_cache_limit,
				(unsigned long long)recovery_cache_evictions,
				(unsigned long long)recovery_epochs,
				(unsigned long long)recovery_lost_data,
				(unsigned long long)recovery_invalidated_tasks,
				scheduler_wait_microseconds / 1e6,
				stage_in_microseconds / 1e6,
				worker_execute_microseconds / 1e6,
				stage_out_microseconds / 1e6,
				scheduler_delay_seconds,
				url_stage_in_seconds,
				(long long)(manager_stats_after.bytes_url_received -
						manager_stats_before.bytes_url_received),
				worker_parallel_seconds,
				(unsigned long long)task_bytes_sent,
				(unsigned long long)task_bytes_received,
				(unsigned long long)publication_stages.task_reports,
				(unsigned long long)publication_stages.task_reported_read_bytes,
				(unsigned long long)publication_stages.task_reported_cpu_milliseconds,
				(unsigned long long)recovery_stages.task_reports,
				(unsigned long long)recovery_stages.task_reported_read_bytes,
				(unsigned long long)recovery_stages.task_reported_cpu_milliseconds,
				dominant_stage,
				dominant_value,
				completion_event_nanoseconds / 1e9,
				checkpoint_nanoseconds / 1e9);
		if (metrics_stream != stderr)
			fclose(metrics_stream);
	}
	if (quiescent && valid && !runtime_stopping(runtime))
		return 2;
	return valid && !runtime_stopping(runtime);
}

static void *runtime_main(void *argument)
{
	struct vine_datavine_workflow_runtime *runtime = argument;
	atomic_fetch_add(&runtime->lanes_running, 1);
	while (!runtime_stopping(runtime)) {
		struct vine_datavine_workflow_info info;
		char *document = 0;
		size_t document_size = 0;
		if (!vine_datavine_workflow_store_take_runnable(runtime->store,
					&info,
					&document,
					&document_size)) {
			usleep(1000);
			continue;
		}
		atomic_fetch_add(&runtime->workflows_running, 1);
		int outcome = execute_document(runtime, info.workflow_id, document, document_size);
		atomic_fetch_sub(&runtime->workflows_running, 1);
		free(document);
		if (!runtime_stopping(runtime) && outcome == 3) {
			struct vine_datavine_workflow_error error;
			vine_datavine_workflow_store_recover(runtime->store,
					info.workflow_id,
					&error);
		} else if (!runtime_stopping(runtime) && outcome != 2) {
			struct vine_datavine_workflow_error error;
			if (vine_datavine_workflow_store_finish(runtime->store,
						info.workflow_id,
						outcome == 1,
						&info,
						&error)) {
				manager_lane_lock(runtime);
				vine_datavine_data_controller_finish_workflow(
						runtime->data_controller, runtime->manager, info.workflow_id);
				pthread_mutex_unlock(&runtime->manager_lock);
			}
		}
	}
	atomic_fetch_sub(&runtime->lanes_running, 1);
	return 0;
}

struct vine_datavine_workflow_runtime *vine_datavine_workflow_runtime_start(
		struct vine_datavine_workflow_store *store,
		struct vine_datavine_data_controller *data_controller,
		struct vine_manager *manager,
		const char *native_executor_path,
		const char *python_executor_path)
{
	if (!store || !data_controller || !manager || !native_executor_path ||
			!python_executor_path)
		return 0;
	struct vine_file *executor = vine_declare_file(manager, native_executor_path, VINE_CACHE_LEVEL_WORKER, VINE_PEER_NOSHARE);
	struct vine_task *library = vine_task_create("./datavine_executor");
	if (!executor || !library ||
			!vine_task_add_input(library, executor, "datavine_executor", 0)) {
		if (library)
			vine_task_delete(library);
		return 0;
	}
	vine_file_set_mode(executor, 0755);
	vine_task_set_cores(library, 1);
	vine_task_set_function_slots(library, 256);
	vine_task_set_function_exec_mode_from_string(library, "direct");
	vine_manager_install_library(manager, library, "datavine-native-v1");
	struct vine_file *python_executor = vine_declare_file(manager,
			python_executor_path,
			VINE_CACHE_LEVEL_WORKER,
			VINE_PEER_NOSHARE);
	const char *object_root = vine_datavine_data_controller_object_root(
			data_controller);
	char *persistence_context =
			vine_datavine_data_controller_persistence_context(data_controller);
	struct vine_task *python_library = vine_task_create(
			"./datavine_python_executor");
	if (!python_executor || !python_library || !object_root || !persistence_context ||
			!vine_task_add_input(python_library, python_executor, "datavine_python_executor", 0)) {
		free(persistence_context);
		if (python_library)
			vine_task_delete(python_library);
		return 0;
	}
	vine_file_set_mode(python_executor, 0755);
	vine_task_set_env_var(python_library, "DATAVINE_OBJECT_ROOT", object_root);
	vine_task_set_env_var(python_library, "DATAVINE_PERSIST_CONTEXT_V1", persistence_context);
	free(persistence_context);
	/* Keep resources and slots unspecified. TaskVine then gives the fork
	 * library the worker's available cores and one child slot per core. This is
	 * its native FunctionCall resource model and prevents fork overcommit. */
	vine_task_set_function_exec_mode_from_string(python_library, "fork");
	vine_manager_install_library(manager, python_library, "datavine-python-v1");
	/* Workflow lanes briefly acquire the Manager lock between progress cycles. */
	vine_tune(manager, "idle-poll-milliseconds", 10);
	vine_tune(manager, "max-retrievals", 256);
	struct vine_datavine_workflow_runtime *runtime = calloc(1, sizeof(*runtime));
	if (!runtime)
		return 0;
	atomic_init(&runtime->stopping, 0);
	atomic_init(&runtime->lanes_running, 0);
	atomic_init(&runtime->workflows_running, 0);
	atomic_init(&runtime->worker_connections, 0);
	atomic_init(&runtime->worker_losses, 0);
	runtime->store = store;
	runtime->data_controller = data_controller;
	runtime->manager = manager;
	if (!vine_manager_enable_worker_events(manager)) {
		free(runtime);
		return 0;
	}
	if (!vine_manager_enable_last_replica_loss_events(manager)) {
		free(runtime);
		return 0;
	}
	runtime->completion_owners = itable_create(0);
	if (!runtime->completion_owners) {
		free(runtime);
		return 0;
	}
	if (pthread_mutex_init(&runtime->manager_lock, 0)) {
		itable_delete(runtime->completion_owners);
		free(runtime);
		return 0;
	}
	if (pthread_mutex_init(&runtime->routing_lock, 0)) {
		pthread_mutex_destroy(&runtime->manager_lock);
		itable_delete(runtime->completion_owners);
		free(runtime);
		return 0;
	}
	return runtime;
}

void vine_datavine_workflow_runtime_run(
		struct vine_datavine_workflow_runtime *runtime,
		volatile sig_atomic_t *external_stopping)
{
	if (!runtime)
		return;
	runtime->external_stopping = external_stopping;
	for (size_t index = 0; index < DATAVINE_WORKFLOW_RUNTIME_LANES; index++) {
		if (pthread_create(&runtime->lanes[index], 0, runtime_main, runtime))
			break;
		runtime->lane_count++;
	}
	if (!runtime->lane_count) {
		atomic_store(&runtime->stopping, 1);
		return;
	}
	int poll_milliseconds = 10;
	while (!runtime_stopping(runtime) || atomic_load(&runtime->lanes_running)) {
		int desired = atomic_load(&runtime->workflows_running) ? 1 : 10;
		manager_lane_lock(runtime);
		if (desired != poll_milliseconds) {
			vine_tune(runtime->manager, "idle-poll-milliseconds", desired);
			poll_milliseconds = desired;
		}
		struct vine_task *task = vine_wait_for_milliseconds(runtime->manager,
				poll_milliseconds);
		struct vine_worker_event worker_event;
		while (vine_manager_poll_worker_event(runtime->manager, &worker_event) > 0) {
			if (worker_event.type == VINE_WORKER_EVENT_CONNECTED)
				atomic_fetch_add(&runtime->worker_connections, 1);
			else if (worker_event.type == VINE_WORKER_EVENT_LOST) {
				atomic_fetch_add(&runtime->worker_losses, 1);
			}
		}
		char *replica_losses[4096];
		size_t replica_loss_count = 0;
		int replica_loss_status = 0;
		while (replica_loss_count < sizeof(replica_losses) / sizeof(replica_losses[0]) &&
				(replica_loss_status = vine_manager_poll_last_replica_loss(
						 runtime->manager,
						 &replica_losses[replica_loss_count])) > 0)
			replica_loss_count++;
		pthread_mutex_unlock(&runtime->manager_lock);
		for (size_t index = 0; index < replica_loss_count; index++) {
			vine_datavine_data_controller_last_replica_lost(
					runtime->data_controller, replica_losses[index]);
			free(replica_losses[index]);
		}
		if (replica_loss_status < 0)
			atomic_store(&runtime->stopping, 1);
		if (task) {
			pthread_mutex_lock(&runtime->routing_lock);
			struct execution_mailbox *owner = itable_lookup(
					runtime->completion_owners,
					(uint64_t)vine_task_get_id(task));
			pthread_mutex_unlock(&runtime->routing_lock);
			if (!owner)
				vine_task_delete(task);
			else if (!mailbox_put(owner, task))
				abort();
		} else {
			usleep(1000);
		}
	}
	atomic_store(&runtime->stopping, 1);
	for (size_t index = 0; index < runtime->lane_count; index++)
		pthread_join(runtime->lanes[index], 0);
	runtime->lane_count = 0;
}

void vine_datavine_workflow_runtime_stop(
		struct vine_datavine_workflow_runtime *runtime)
{
	if (!runtime)
		return;
	atomic_store(&runtime->stopping, 1);
	pthread_mutex_destroy(&runtime->routing_lock);
	pthread_mutex_destroy(&runtime->manager_lock);
	itable_delete(runtime->completion_owners);
	free(runtime);
}
