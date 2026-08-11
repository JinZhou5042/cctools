/* DataVine native command-workflow execution owner. */

#include "vine_datavine_workflow_store.h"

#include "b64.h"
#include "buffer.h"
#include "itable.h"
#include "jx.h"
#include "jx_parse.h"
#include "taskvine.h"
#include "vine_datavine_data_controller.h"
#include "vine_datavine_scheduler.h"
#include "vine_datavine_protocol.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define DATAVINE_WORKFLOW_SUBMISSION_WINDOW 4096
#define DATAVINE_WORKFLOW_CHECKPOINT_COMPLETIONS 4096
#define DATAVINE_WORKFLOW_RUNTIME_LANES 4

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

struct completion_node {
	struct vine_task *task;
	struct completion_node *next;
};

struct pending_publication {
	struct vine_datavine_data_publication *publication;
	struct jx *task;
	int64_t logical_id;
	uint32_t attempt;
	int32_t task_result;
	struct pending_publication *next;
};

struct execution_mailbox {
	pthread_mutex_t lock;
	struct completion_node *head;
	struct completion_node *tail;
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

struct vine_datavine_workflow_runtime {
	struct vine_datavine_workflow_store *store;
	struct vine_datavine_data_controller *data_controller;
	struct vine_manager *manager;
	pthread_t threads[DATAVINE_WORKFLOW_RUNTIME_LANES];
	pthread_t pump_thread;
	size_t thread_count;
	pthread_mutex_t manager_lock;
	pthread_mutex_t routing_lock;
	struct itable *completion_owners;
	atomic_int stopping;
	atomic_int lanes_running;
};

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
		if (!cancelled)
			continue;
		pthread_mutex_lock(&runtime->routing_lock);
		itable_remove(runtime->completion_owners,
				(uint64_t)vine_task_get_id(cancelled));
		pthread_mutex_unlock(&runtime->routing_lock);
		itable_remove(physical_to_logical,
				(uint64_t)vine_task_get_id(cancelled));
		vine_task_delete(cancelled);
		running--;
	}
}

static void *runtime_pump(void *argument)
{
	struct vine_datavine_workflow_runtime *runtime = argument;
	while (!atomic_load(&runtime->stopping) ||
			atomic_load(&runtime->lanes_running)) {
		pthread_mutex_lock(&runtime->manager_lock);
		struct vine_task *task = vine_wait_for_milliseconds(runtime->manager, 10);
		pthread_mutex_unlock(&runtime->manager_lock);
		if (!task) {
			usleep(1000);
			continue;
		}
		pthread_mutex_lock(&runtime->routing_lock);
		struct execution_mailbox *owner = itable_lookup(
				runtime->completion_owners,
				(uint64_t)vine_task_get_id(task));
		pthread_mutex_unlock(&runtime->routing_lock);
		if (!owner) {
			vine_task_delete(task);
		} else if (!mailbox_put(owner, task)) {
			/* The Manager has already removed this completion from its queue.
			 * Dropping it would leave the durable workflow permanently RUNNING,
			 * so fail-stop and recover from the workflow journal instead. */
			abort();
		}
	}
	return 0;
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
		struct jx *executor, struct itable *data)
{
	uint64_t payload_id = (uint64_t)jx_lookup_integer(executor, "payload_ref");
	if (!payload_id)
		return 0;
	uint64_t wall_seconds = 0;
	struct jx *resources = jx_lookup(task, "resources");
	struct jx *wall_time = resources
					       ? jx_lookup(resources, "wall_time_seconds")
					       : 0;
	if (wall_time)
		wall_seconds = (uint64_t)wall_time->u.integer_value;
	unsigned char source_ticket[24] = {'D', 'V', 'P', '1'};
	unsigned char *ticket = source_ticket;
	size_t ticket_size = sizeof(source_ticket);
	const char *function_name = "datavine_python_source";
	vine_datavine_put_u64(ticket + 8, payload_id);
	vine_datavine_put_u64(ticket + 16, wall_seconds);
	if (!strcmp(jx_lookup_string(executor, "version"), "callable-v1")) {
		uint64_t function_ref = (uint64_t)jx_lookup_integer(executor, "function_ref");
		const char *digest = jx_lookup_string(executor, "function_digest");
		struct jx *payload_record = itable_lookup(data, payload_id);
		struct jx *origin = payload_record ? jx_lookup(payload_record, "origin") : 0;
		buffer_t invocation;
		buffer_init(&invocation);
		if (!function_ref || !digest || strlen(digest) != 64 || !origin ||
				strcmp(jx_lookup_string(origin, "kind"), "inline") ||
				b64_decode(jx_lookup_string(origin, "base64"), &invocation) != 0) {
			buffer_free(&invocation);
			return 0;
		}
		size_t invocation_size = 0;
		const char *invocation_bytes = buffer_tolstring(&invocation, &invocation_size);
		if (invocation_size > UINT32_MAX || invocation_size > SIZE_MAX - 56) {
			buffer_free(&invocation);
			return 0;
		}
		ticket_size = 56 + invocation_size;
		ticket = calloc(1, ticket_size);
		if (!ticket) {
			buffer_free(&invocation);
			return 0;
		}
		memcpy(ticket, "DVP3", 4);
		vine_datavine_put_u32(ticket + 4, (uint32_t)invocation_size);
		vine_datavine_put_u64(ticket + 8, function_ref);
		vine_datavine_put_u64(ticket + 16, wall_seconds);
		for (size_t index = 0; index < 32; index++) {
			unsigned int value = 0;
			if (sscanf(digest + index * 2, "%2x", &value) != 1) {
				buffer_free(&invocation);
				free(ticket);
				return 0;
			}
			ticket[24 + index] = (unsigned char)value;
		}
		memcpy(ticket + 56, invocation_bytes, invocation_size);
		buffer_free(&invocation);
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

static int prepare_origin(struct vine_manager *manager, uint64_t data_id,
		struct jx *record, struct itable *files)
{
	if (itable_lookup(files, data_id))
		return 1;
	struct jx *origin = jx_lookup(record, "origin");
	const char *kind = jx_lookup_string(origin, "kind");
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
	}
	return !file || itable_insert(files, data_id, file);
}

static int prepare_origins(struct vine_manager *manager, struct itable *data,
		struct itable *files)
{
	UINT64_T data_id;
	void *value;
	int iterator;
	ITABLE_ITERATE(data, iterator, data_id, value)
	{
		if (!prepare_origin(manager, data_id, value, files))
			return 0;
	}
	return 1;
}

static int prepare_delta_inputs(struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct jx *root, struct itable *data,
		struct itable *files)
{
	struct jx *record;
	void *iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "data"), &iterator))) {
		uint64_t data_id = (uint64_t)jx_lookup_integer(record, "data_id");
		struct jx *origin = jx_lookup(record, "origin");
		const char *kind = jx_lookup_string(origin, "kind");
		if (!strcmp(kind, "inline") || !strcmp(kind, "uri")) {
			manager_lane_lock(runtime);
			int prepared = prepare_origin(runtime->manager, data_id, record, files);
			pthread_mutex_unlock(&runtime->manager_lock);
			if (!prepared)
				return 0;
		}
	}
	iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "tasks"), &iterator))) {
		struct jx *input;
		void *input_iterator = 0;
		while ((input = jx_iterate_array(jx_lookup(record, "inputs"),
					&input_iterator))) {
			uint64_t data_id = (uint64_t)jx_lookup_integer(input, "data_id");
			if (itable_lookup(files, data_id))
				continue;
			struct jx *data_item = data_record(data, data_id);
			struct jx *origin = data_item ? jx_lookup(data_item, "origin") : 0;
			if (!origin || strcmp(jx_lookup_string(origin, "kind"), "output"))
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

static int restore_outputs(struct vine_datavine_workflow_runtime *runtime,
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
		struct jx *origin = jx_lookup(value, "origin");
		if (strcmp(jx_lookup_string(origin, "kind"), "output"))
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

static int task_outputs_recoverable(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct jx *task,
		struct itable *consumers, struct itable *requested)
{
	struct jx *outputs = task ? jx_lookup(task, "output_data_ids") : 0;
	int count = outputs ? jx_array_length(outputs) : 0;
	for (int index = 0; index < count; index++) {
		uint64_t data_id = (uint64_t)jx_array_index(outputs, index)->u.integer_value;
		if (!itable_lookup(consumers, data_id) &&
				!itable_lookup(requested, data_id))
			continue;
		struct vine_datavine_workflow_result_info info;
		if (!vine_datavine_data_controller_result_active(
					runtime->data_controller, workflow_id, data_id) &&
				!vine_datavine_workflow_store_legacy_result_info(
					runtime->store, workflow_id, data_id, &info))
			return 0;
	}
	return task != 0;
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

static int filter_recoverable_tasks(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct itable *tasks,
		struct itable *consumers, struct itable *requested,
		buffer_t *completed_tasks)
{
	size_t size = 0;
	const unsigned char *encoded = (const unsigned char *)buffer_tolstring(
			completed_tasks, &size);
	if (size % 8)
		return 0;
	buffer_t filtered;
	buffer_init(&filtered);
	int valid = 1;
	for (size_t offset = 0; valid && offset < size; offset += 8) {
		uint64_t task_id = get_little_u64(encoded + offset);
		struct jx *task = itable_lookup(tasks, task_id);
		/* Worker-local temp values intentionally are not journaled. After a
		 * service restart they are gone, so their producers must run again. */
		if (task_outputs_recoverable(runtime, workflow_id, task,
				consumers, requested) &&
				buffer_putlstring(&filtered, (const char *)(encoded + offset), 8) < 0)
			valid = 0;
	}
	if (valid) {
		size_t filtered_size = 0;
		const char *filtered_data = buffer_tolstring(&filtered, &filtered_size);
		buffer_rewind(completed_tasks, 0);
		valid = !filtered_size ||
			buffer_putlstring(completed_tasks, filtered_data, filtered_size) >= 0;
	}
	buffer_free(&filtered);
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
		int64_t task_id = jx_lookup_integer(task, "task_id");
		if (task_id > maximum_task_id)
			maximum_task_id = task_id;
		task_count++;
		struct jx *input;
		void *input_iterator = 0;
		while ((input = jx_iterate_array(jx_lookup(task, "inputs"), &input_iterator))) {
			struct jx *record = data_record(data, (uint64_t)jx_lookup_integer(input, "data_id"));
			if (!strcmp(jx_lookup_string(jx_lookup(record, "origin"), "kind"), "output"))
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
		uint64_t task_id = (uint64_t)jx_lookup_integer(task, "task_id");
		struct itable *parents = itable_create(0);
		buffer_t encoded;
		buffer_init(&encoded);
		struct jx *input;
		void *input_iterator = 0;
		while ((input = jx_iterate_array(jx_lookup(task, "inputs"), &input_iterator))) {
			struct jx *record = data_record(data, (uint64_t)jx_lookup_integer(input, "data_id"));
			struct jx *origin = jx_lookup(record, "origin");
			if (!strcmp(jx_lookup_string(origin, "kind"), "output")) {
				uint64_t parent = (uint64_t)jx_lookup_integer(origin, "task_id");
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
		struct itable *requested)
{
	struct jx *record;
	void *iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "data"), &iterator))) {
		uint64_t data_id = (uint64_t)jx_lookup_integer(record, "data_id");
		if (itable_lookup(data, data_id) || !itable_insert(data, data_id, record))
			return 0;
	}
	if (!vine_datavine_scheduler_begin_update(scheduler,
			    vine_datavine_scheduler_revision(scheduler)))
		return 0;
	iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "tasks"), &iterator))) {
		uint64_t task_id = (uint64_t)jx_lookup_integer(record, "task_id");
		buffer_t parents;
		buffer_init(&parents);
		struct itable *seen = itable_create(0);
		struct jx *input;
		void *input_iterator = 0;
		int valid = seen != 0;
		while (valid && (input = jx_iterate_array(jx_lookup(record, "inputs"),
						 &input_iterator))) {
			uint64_t data_id = (uint64_t)jx_lookup_integer(input, "data_id");
			uintptr_t count = (uintptr_t)itable_lookup(consumers, data_id);
			valid = itable_insert(consumers, data_id, (void *)(count + 1));
			struct jx *data_value = data_record(data, data_id);
			struct jx *origin = data_value ? jx_lookup(data_value, "origin") : 0;
			if (valid && origin && !strcmp(jx_lookup_string(origin, "kind"), "output")) {
				uint64_t parent = (uint64_t)jx_lookup_integer(origin, "task_id");
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

static struct vine_task *materialize(struct vine_manager *manager,
		struct vine_datavine_data_controller *data_controller,
		const char *workflow_id, struct jx *task, struct itable *data,
		struct itable *files, struct itable *consumers,
		struct itable *requested, uint32_t attempt, int retain_all)
{
	struct jx *executor = jx_lookup(task, "executor");
	const char *kind = jx_lookup_string(executor, "kind");
	if (strcmp(kind, "command") && strcmp(kind, "python") &&
			strcmp(kind, "taskvine"))
		return 0;
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
		physical = create_python_ticket(task, executor, data);
	} else if (!strcmp(kind, "taskvine")) {
		size_t input_size = 0;
		const unsigned char *input = (const unsigned char *)
				buffer_tolstring(&function_input, &input_size);
		if (input_size == 5 && input[4] == 1)
			physical = create_native_ticket(
					jx_lookup_integer(task, "task_id"), attempt, 1);
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
	struct itable *mounted = itable_create(0);
	if (!mounted) {
		vine_task_delete(physical);
		return 0;
	}
	/* Logical attempts and Worker-loss recovery belong to the native DataVine
	Scheduler.  Do not let TaskVine invisibly retry a forsaken physical task. */
	vine_task_set_max_forsaken(physical, 0);
	if (!strcmp(kind, "python")) {
		uint64_t payload_id = (uint64_t)jx_lookup_integer(executor, "payload_ref");
		if (strcmp(jx_lookup_string(executor, "version"), "callable-v1")) {
			struct vine_file *payload = itable_lookup(files, payload_id);
			char path[64];
			if (!payload || !vine_task_add_input(physical, payload, data_path(payload_id, path), 0)) {
				itable_delete(mounted);
				vine_task_delete(physical);
				return 0;
			}
			itable_insert(mounted, payload_id, payload);
		}
		if (!strcmp(jx_lookup_string(executor, "version"), "callable-v1")) {
			uint64_t function_id = (uint64_t)jx_lookup_integer(executor, "function_ref");
			struct vine_file *function_file = itable_lookup(files, function_id);
			char function_path[64];
			if (!function_file || !vine_task_add_input(physical, function_file, data_path(function_id, function_path), 0)) {
				itable_delete(mounted);
				vine_task_delete(physical);
				return 0;
			}
			itable_insert(mounted, function_id, function_file);
		}
	}
	struct jx *input;
	iterator = 0;
	while ((input = jx_iterate_array(jx_lookup(task, "inputs"), &iterator))) {
		uint64_t data_id = (uint64_t)jx_lookup_integer(input, "data_id");
		struct vine_file *file = itable_lookup(files, data_id);
		char path[64];
		if (!file || (!itable_lookup(mounted, data_id) &&
					     !vine_task_add_input(physical, file, data_path(data_id, path), 0))) {
			itable_delete(mounted);
			vine_task_delete(physical);
			return 0;
		}
		itable_insert(mounted, data_id, file);
		data_path(data_id, path);
		char variable[64];
		snprintf(variable, sizeof(variable), "DATAVINE_DATA_%llu", (unsigned long long)data_id);
		vine_task_set_env_var(physical, variable, path);
	}
	if (!vine_datavine_data_controller_bind_outputs(data_controller, manager, workflow_id, physical, task, files, consumers, requested, attempt, retain_all)) {
		itable_delete(mounted);
		vine_task_delete(physical);
		return 0;
	}
	struct jx *resources = jx_lookup(task, "resources");
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
	struct jx *priority = jx_lookup(task, "priority");
	if (priority)
		vine_task_set_priority(physical, priority->u.integer_value);
	struct jx *environment = jx_lookup(executor, "environment");
	if (environment) {
		const char *name;
		void *environment_iterator = 0;
		while ((name = jx_iterate_keys(environment, &environment_iterator)))
			vine_task_set_env_var(physical, name, jx_lookup_string(environment, name));
	}
	itable_delete(mounted);
	return physical;
}

static int maximum_attempts(struct jx *task)
{
	struct jx *retry = jx_lookup(task, "retry");
	return retry ? (int)jx_lookup_integer(retry, "maximum_attempts") : 1;
}

static struct vine_datavine_data_publication *publish_task_outputs(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct jx *task, struct itable *data,
		struct itable *files, struct itable *consumers,
		struct itable *requested,
		struct vine_task *completed, uint32_t attempt, int retain_all)
{
	struct jx *outputs = jx_lookup(task, "output_data_ids");
	if (jx_array_length(outputs) < 1)
		return 0;
	return vine_datavine_data_controller_publish_async(
			runtime->data_controller, workflow_id, task, data, files,
			consumers, requested, attempt, retain_all, completed);
}

static int restore_published_outputs(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct jx *task, struct itable *files,
		struct itable *consumers)
{
	struct jx *outputs = jx_lookup(task, "output_data_ids");
	int count = jx_array_length(outputs);
	int valid = count > 0;
	manager_lane_lock(runtime);
	for (int index = 0; valid && index < count; index++) {
		uint64_t data_id = (uint64_t)jx_array_index(
				outputs, index)
						   ->u.integer_value;
		if (!itable_lookup(consumers, data_id) ||
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

static int finish_logical_attempt(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct vine_datavine_scheduler *scheduler,
		struct itable *files, struct itable *consumers,
		struct itable *requested, struct jx *task,
		int64_t logical_id, uint32_t attempt, int32_t task_result,
		int successful, uint64_t *completion_event_nanoseconds)
{
	if (successful)
		successful = restore_published_outputs(
				runtime, workflow_id, task, files, consumers);
	int valid = logical_id > 0 && task;
	struct vine_datavine_workflow_task_event_record event = {
			.task_id = logical_id,
			.attempt = attempt,
			.result = task_result,
	};
	if (successful) {
		event.type = VINE_DATAVINE_WORKFLOW_TASK_COMPLETED;
		event.result = 0;
		valid &= vine_datavine_scheduler_mark_done(scheduler, logical_id);
	} else if (valid && attempt < (uint32_t)maximum_attempts(task)) {
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

static int drain_publications(
		struct vine_datavine_workflow_runtime *runtime,
		const char *workflow_id, struct vine_datavine_scheduler *scheduler,
		struct itable *files, struct itable *consumers,
		struct itable *requested,
		struct pending_publication **head, uint64_t *publishing,
		int block_one, uint32_t *completed_count,
		uint64_t *publish_nanoseconds,
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
					workflow_id, (long long)pending->logical_id);
		clock_gettime(CLOCK_MONOTONIC, &finished);
		*publish_nanoseconds += elapsed_nanoseconds(&started, &finished);
		valid = finish_logical_attempt(runtime, workflow_id, scheduler, files,
				consumers, requested, pending->task, pending->logical_id,
				pending->attempt, pending->task_result, published,
				completion_event_nanoseconds);
		vine_datavine_data_publication_delete(pending->publication);
		*link = pending->next;
		free(pending);
		(*publishing)--;
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

struct execution_resources {
	struct execution_mailbox mailbox;
	int mailbox_initialized;
	struct retained_root *roots;
	struct itable *data;
	struct itable *tasks;
	struct itable *files;
	struct itable *consumers;
	struct itable *requested;
	struct itable *physical_to_logical;
	struct vine_datavine_scheduler *scheduler;
	buffer_t recovered_tasks;
	uint32_t *attempts;
	uint64_t maximum_task_id;
};

static void execution_resources_delete(struct execution_resources *resources)
{
	free(resources->attempts);
	buffer_free(&resources->recovered_tasks);
	if (resources->scheduler)
		vine_datavine_scheduler_delete(resources->scheduler);
	if (resources->physical_to_logical)
		itable_delete(resources->physical_to_logical);
	if (resources->files)
		itable_delete(resources->files);
	if (resources->consumers)
		itable_delete(resources->consumers);
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
	int valid = retained_root_add(&resources->roots, root);
	resources->data = itable_create(0);
	resources->tasks = itable_create(0);
	resources->files = itable_create(0);
	resources->consumers = itable_create(0);
	resources->requested = itable_create(0);
	resources->physical_to_logical = itable_create(0);
	valid = valid && resources->data && resources->tasks && resources->files &&
		resources->consumers && resources->requested &&
		resources->physical_to_logical;
	if (valid) {
		struct jx *record;
		void *iterator = 0;
		while ((record = jx_iterate_array(jx_lookup(root, "data"), &iterator)))
			valid &= itable_insert(resources->data,
					(uint64_t)jx_lookup_integer(record, "data_id"),
					record);
		iterator = 0;
		while ((record = jx_iterate_array(jx_lookup(root, "tasks"), &iterator))) {
			struct jx *input;
			void *input_iterator = 0;
			while ((input = jx_iterate_array(jx_lookup(record, "inputs"),
						&input_iterator))) {
				uint64_t data_id = (uint64_t)jx_lookup_integer(input,
						"data_id");
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
	if (valid) {
		manager_lane_lock(runtime);
		valid = prepare_origins(runtime->manager, resources->data, resources->files) &&
			restore_outputs(runtime, workflow_id, resources->data, resources->files, resources->consumers);
		pthread_mutex_unlock(&runtime->manager_lock);
	}
	valid = valid && restore_completed_tasks(runtime, workflow_id,
		&resources->recovered_tasks) &&
		build_scheduler(root, resources->data, resources->tasks, &resources->scheduler);
	if (valid) {
		struct jx *policy = jx_lookup(root, "policy");
		uint64_t maximum_tasks = policy
							 ? (uint64_t)jx_lookup_integer(policy,
									   "maximum_tasks")
							 : 0;
		uint64_t initial_tasks = (uint64_t)jx_array_length(jx_lookup(root,
				"tasks"));
		uint64_t task_id;
		void *task_value;
		int task_iterator;
		ITABLE_ITERATE(resources->tasks, task_iterator, task_id, task_value)
		{
			if (task_id > resources->maximum_task_id)
				resources->maximum_task_id = task_id;
		}
		if (maximum_tasks > initial_tasks)
			resources->maximum_task_id += maximum_tasks - initial_tasks;
	}
	resources->attempts = valid
					      ? calloc((size_t)resources->maximum_task_id + 1,
								sizeof(*resources->attempts))
					      : 0;
	valid = valid && resources->attempts;
	if (valid) {
		uint64_t task_id;
		void *task_value;
		int task_iterator;
		ITABLE_ITERATE(resources->tasks, task_iterator, task_id, task_value)
		{
			resources->attempts[task_id] =
					vine_datavine_workflow_store_task_attempts(
							runtime->store, workflow_id, (int64_t)task_id);
		}
	}
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
	struct itable *requested = resources.requested;
	struct itable *physical_to_logical = resources.physical_to_logical;
	struct vine_datavine_scheduler *scheduler = resources.scheduler;
	uint32_t *attempts = resources.attempts;
	uint64_t maximum_task_id = resources.maximum_task_id;
	int report_metrics = getenv("DATAVINE_WORKFLOW_METRICS") != 0;
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
	uint64_t submission_event_nanoseconds = 0;
	uint64_t publish_nanoseconds = 0;
	uint64_t completion_event_nanoseconds = 0;
	uint64_t checkpoint_nanoseconds = 0;
	uint64_t physical_submissions = 0;
	uint64_t physical_completions = 0;
	int quiescent = 0;
	int recovered_applied = 0;
	while (valid && !atomic_load(&runtime->stopping)) {
		valid = drain_publications(runtime, workflow_id, scheduler, files,
				consumers, requested, &pending_publications, &publishing, 0,
				&uncheckpointed_completions, &publish_nanoseconds,
				&completion_event_nanoseconds);
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
			valid = delta_root && apply_delta_root(delta_root, scheduler, data, tasks, consumers, requested);
			if (valid)
				valid = prepare_delta_inputs(runtime, workflow_id, delta_root, data, files);
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
				uint64_t new_id = (uint64_t)jx_lookup_integer(new_task, "task_id");
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
			valid = filter_recoverable_tasks(runtime, workflow_id, tasks,
					consumers, requested, &resources.recovered_tasks);
			size_t recovered_size = 0;
			const char *recovered = buffer_tolstring(&resources.recovered_tasks,
					&recovered_size);
			if (valid && recovered_size)
				valid = vine_datavine_scheduler_rebuild(scheduler, recovered, recovered_size);
			recovered_applied = 1;
			if (!valid)
				break;
		}
		int64_t logical_id;
		manager_lane_lock(runtime);
		while (running + publishing < DATAVINE_WORKFLOW_SUBMISSION_WINDOW &&
				(logical_id = vine_datavine_scheduler_take(scheduler)) > 0) {
			struct jx *task = itable_lookup(tasks, (uint64_t)logical_id);
			struct timespec stage_started;
			struct timespec stage_finished;
			clock_gettime(CLOCK_MONOTONIC, &stage_started);
			struct vine_task *physical = materialize(runtime->manager,
					runtime->data_controller,
					workflow_id,
					task,
					data,
					files,
					consumers,
					requested,
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
			if (physical_id < 1 || !itable_insert(physical_to_logical,
							       (uint64_t)physical_id,
							       (void *)(uintptr_t)logical_id)) {
				if (report_metrics)
					fprintf(stderr, "datavine workflow %s submit_failed task=%lld physical=%d\n", workflow_id, (long long)logical_id, physical_id);
				vine_task_delete(physical);
				valid = 0;
				break;
			}
			running++;
			physical_submissions++;
			attempts[logical_id]++;
			struct vine_datavine_workflow_task_event_record event = {
					.type = VINE_DATAVINE_WORKFLOW_TASK_SUBMITTED,
					.task_id = logical_id,
					.attempt = attempts[logical_id],
			};
			struct vine_datavine_workflow_error event_error;
			clock_gettime(CLOCK_MONOTONIC, &stage_started);
			valid = vine_datavine_workflow_store_record_task_events(
					runtime->store, workflow_id, &event, 1, &event_error);
			clock_gettime(CLOCK_MONOTONIC, &stage_finished);
			submission_event_nanoseconds += elapsed_nanoseconds(
					&stage_started, &stage_finished);
			if (!valid && report_metrics)
				fprintf(stderr, "datavine workflow %s event_failed code=%d path=%s detail=%s\n", workflow_id, event_error.code, event_error.path, event_error.message);
		}
		pthread_mutex_unlock(&runtime->manager_lock);
		if (!publishing && vine_datavine_scheduler_complete(scheduler)) {
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
				valid = drain_publications(runtime, workflow_id, scheduler, files,
						consumers, requested, &pending_publications,
						&publishing, 1, &uncheckpointed_completions,
						&publish_nanoseconds,
						&completion_event_nanoseconds);
				continue;
			}
			valid = 0;
			break;
		}
		struct vine_task *completed = runtime_wait(runtime, mailbox, 1);
		if (!completed)
			continue;
		int drained_completions = 0;
		do {
			int physical_id = vine_task_get_id(completed);
			pthread_mutex_lock(&runtime->routing_lock);
			itable_remove(runtime->completion_owners, (uint64_t)physical_id);
			pthread_mutex_unlock(&runtime->routing_lock);
			int64_t completed_logical_id = (int64_t)(uintptr_t)itable_remove(
					physical_to_logical, (uint64_t)physical_id);
			running--;
			physical_completions++;
			int physical_success = completed_logical_id > 0 &&
					       vine_task_get_result(completed) == VINE_RESULT_SUCCESS &&
					       vine_task_get_exit_code(completed) == 0;
			int32_t task_result = vine_task_get_result(completed) == VINE_RESULT_SUCCESS
							      ? vine_task_get_exit_code(completed)
							      : -(int32_t)vine_task_get_result(completed);
			if (report_metrics && !physical_success)
				fprintf(stderr,
						"datavine workflow %s task_failed logical_id=%lld "
						"result=%d exit_code=%d\n",
						workflow_id, (long long)completed_logical_id,
						vine_task_get_result(completed),
						vine_task_get_exit_code(completed));
			int task_valid = completed_logical_id > 0;
			struct jx *task = task_valid
							  ? itable_lookup(tasks, (uint64_t)completed_logical_id)
							  : 0;
			uint32_t attempt = task_valid ? attempts[completed_logical_id] : 0;
			if (physical_success) {
				struct pending_publication *pending = calloc(1, sizeof(*pending));
				struct vine_datavine_data_publication *publication =
						pending ? publish_task_outputs(runtime, workflow_id, task,
								data, files, consumers, requested, completed,
								attempt,
								workflow_info.state != VINE_DATAVINE_WORKFLOW_RUNNING)
							: 0;
				if (publication) {
					pending->publication = publication;
					pending->task = task;
					pending->logical_id = completed_logical_id;
					pending->attempt = attempt;
					pending->task_result = task_result;
					pending->next = pending_publications;
					pending_publications = pending;
					publishing++;
				} else {
					free(pending);
					task_valid = finish_logical_attempt(runtime, workflow_id,
							scheduler, files, consumers, requested, task,
							completed_logical_id, attempt, task_result, 0,
							&completion_event_nanoseconds);
					uncheckpointed_completions += completed_logical_id > 0;
				}
			} else {
				task_valid = finish_logical_attempt(runtime, workflow_id,
						scheduler, files, consumers, requested, task,
						completed_logical_id, attempt, task_result, 0,
						&completion_event_nanoseconds);
				uncheckpointed_completions += completed_logical_id > 0;
			}
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
			completed = valid && drained_completions < 256
						    ? runtime_wait(runtime, mailbox, 0)
						    : 0;
		} while (completed);
	}
	if (running)
		cancel_running_tasks(runtime, mailbox, physical_to_logical, running);
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
	execution_resources_delete(&resources);
	struct timespec execution_finished;
	clock_gettime(CLOCK_MONOTONIC, &execution_finished);
	double setup_seconds = (setup_finished.tv_sec - execution_started.tv_sec) +
			       (setup_finished.tv_nsec - execution_started.tv_nsec) / 1e9;
	double run_seconds = (execution_finished.tv_sec - setup_finished.tv_sec) +
			     (execution_finished.tv_nsec - setup_finished.tv_nsec) / 1e9;
	if (report_metrics)
		fprintf(stderr,
				"datavine workflow %s setup_seconds=%.6f run_seconds=%.6f "
				"physical_submissions=%llu physical_completions=%llu "
				"materialize_seconds=%.6f submit_seconds=%.6f "
				"submission_event_seconds=%.6f publish_seconds=%.6f "
				"completion_event_seconds=%.6f checkpoint_seconds=%.6f\n",
				workflow_id,
				setup_seconds,
				run_seconds,
				(unsigned long long)physical_submissions,
				(unsigned long long)physical_completions,
				materialize_nanoseconds / 1e9,
				submit_nanoseconds / 1e9,
				submission_event_nanoseconds / 1e9,
				publish_nanoseconds / 1e9,
				completion_event_nanoseconds / 1e9,
				checkpoint_nanoseconds / 1e9);
	if (quiescent && valid && !atomic_load(&runtime->stopping))
		return 2;
	return valid && !atomic_load(&runtime->stopping);
}

static void *runtime_main(void *argument)
{
	struct vine_datavine_workflow_runtime *runtime = argument;
	while (!atomic_load(&runtime->stopping)) {
		struct vine_datavine_workflow_info info;
		char *document = 0;
		size_t document_size = 0;
		if (!vine_datavine_workflow_store_take_runnable(runtime->store,
				    &info,
				    &document,
				    &document_size)) {
			/* Worker registration, status, heartbeats, and Factory demand must
			 * progress even before the first workflow is submitted. */
			usleep(1000);
			continue;
		}
		int outcome = execute_document(runtime, info.workflow_id, document, document_size);
		free(document);
		if (!atomic_load(&runtime->stopping) && outcome != 2) {
			struct vine_datavine_workflow_error error;
			if (vine_datavine_workflow_store_finish(runtime->store,
					    info.workflow_id,
					    outcome == 1,
					    &info,
					    &error)) {
				manager_lane_lock(runtime);
				vine_datavine_data_controller_finish_workflow(
						runtime->data_controller, runtime->manager,
						info.workflow_id);
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
	struct vine_task *python_library = vine_task_create(
			"./datavine_python_executor");
	if (!python_executor || !python_library ||
			!vine_task_add_input(python_library, python_executor, "datavine_python_executor", 0)) {
		if (python_library)
			vine_task_delete(python_library);
		return 0;
	}
	vine_file_set_mode(python_executor, 0755);
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
	runtime->store = store;
	runtime->data_controller = data_controller;
	runtime->manager = manager;
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
	if (pthread_create(&runtime->pump_thread, 0, runtime_pump, runtime)) {
		pthread_mutex_destroy(&runtime->routing_lock);
		pthread_mutex_destroy(&runtime->manager_lock);
		itable_delete(runtime->completion_owners);
		free(runtime);
		return 0;
	}
	for (size_t lane = 0; lane < DATAVINE_WORKFLOW_RUNTIME_LANES; lane++) {
		if (pthread_create(&runtime->threads[lane], 0, runtime_main, runtime)) {
			atomic_store(&runtime->stopping, 1);
			for (size_t joined = 0; joined < runtime->thread_count; joined++)
				pthread_join(runtime->threads[joined], 0);
			pthread_join(runtime->pump_thread, 0);
			pthread_mutex_destroy(&runtime->routing_lock);
			pthread_mutex_destroy(&runtime->manager_lock);
			itable_delete(runtime->completion_owners);
			free(runtime);
			return 0;
		}
		runtime->thread_count++;
		atomic_fetch_add(&runtime->lanes_running, 1);
	}
	return runtime;
}

void vine_datavine_workflow_runtime_stop(
		struct vine_datavine_workflow_runtime *runtime)
{
	if (!runtime)
		return;
	atomic_store(&runtime->stopping, 1);
	for (size_t lane = 0; lane < runtime->thread_count; lane++)
		pthread_join(runtime->threads[lane], 0);
	pthread_join(runtime->pump_thread, 0);
	pthread_mutex_destroy(&runtime->routing_lock);
	pthread_mutex_destroy(&runtime->manager_lock);
	itable_delete(runtime->completion_owners);
	free(runtime);
}
