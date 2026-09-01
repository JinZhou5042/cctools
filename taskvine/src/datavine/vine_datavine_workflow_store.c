/* DataVine workflow store implementation.
Copyright (C) 2026- The University of Notre Dame
See the file COPYING for details.
*/

#include "vine_datavine_workflow_store.h"

#include "hash_table.h"
#include "itable.h"
#include "jx.h"
#include "jx_parse.h"
#include "vine_datavine_journal.h"
#include "vine_datavine_ir.h"
#include "vine_datavine_parametric.h"
#include "vine_datavine_protocol.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum store_record_opcode {
	STORE_SUBMIT = 100,
	/* 101 was never assigned and remains reserved. */
	STORE_SEAL = 102,
	STORE_CANCEL = 103,
	STORE_START = 104,
	STORE_COMPLETE = 105,
	STORE_FAIL = 106,
	/* 107 was used by a retired payload-in-journal format. */
	STORE_RECOVER = 108,
	STORE_TASK_EVENT = 109,
	/* 110 was used by a retired payload-in-journal format. */
	STORE_CHECKPOINT = 111,
	STORE_TASK_EVENT_BATCH = 112,
	STORE_APPEND_DELTA = 113,
	STORE_QUIESCENT = 114,
	/* 115 was used by a retired payload-in-journal format. */
};
#define STORE_MAX_EVENTS 64

struct stored_transaction {
	uint64_t generation;
	char *document;
	size_t document_size;
	struct jx *root;
	struct stored_transaction *next;
};

struct stored_workflow {
	char *workflow_id;
	char *document;
	size_t document_size;
	struct vine_datavine_workflow_info info;
	struct vine_datavine_workflow_event events[STORE_MAX_EVENTS];
	size_t event_start;
	size_t event_count;
	struct itable *attempts;
	struct itable *completed_tasks;
	struct itable *data_producers;
	struct vine_datavine_parametric *parametric;
	uint64_t maximum_tasks;
	uint64_t maximum_edges;
	uint64_t maximum_task_id;
	uint64_t maximum_data_id;
	struct stored_transaction *delta_head;
	struct stored_transaction *delta_tail;
	int runtime_claimed;
};

struct idempotency_record {
	struct stored_workflow *workflow;
	char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1];
};

struct vine_datavine_workflow_store {
	pthread_mutex_t lock;
	struct stored_workflow *workflow;
	struct hash_table *idempotency;
	struct vine_datavine_journal *journal;
};

static struct stored_workflow *workflow_lookup(
		struct vine_datavine_workflow_store *store, const char *workflow_id)
{
	return store && store->workflow && workflow_id &&
			!strcmp(store->workflow->workflow_id, workflow_id)
			? store->workflow : 0;
}

static void transaction_delete(struct stored_transaction *transaction)
{
	if (!transaction)
		return;
	free(transaction->document);
	if (transaction->root)
		jx_delete(transaction->root);
	free(transaction);
}

static void transaction_list_delete(struct stored_transaction *transaction)
{
	while (transaction) {
		struct stored_transaction *next = transaction->next;
		transaction_delete(transaction);
		transaction = next;
	}
}

static struct stored_transaction *transaction_create(const char *json,
		size_t size)
{
	struct stored_transaction *transaction = calloc(1, sizeof(*transaction));
	if (!transaction)
		return 0;
	transaction->document = malloc(size + 1);
	transaction->root = jx_parse_string_and_length(json, (int)size);
	if (!transaction->document || !transaction->root) {
		transaction_delete(transaction);
		return 0;
	}
	memcpy(transaction->document, json, size);
	transaction->document[size] = 0;
	transaction->document_size = size;
	return transaction;
}

static int store_fail(struct vine_datavine_workflow_error *error,
		enum vine_datavine_workflow_error_code code,
		const char *path, const char *message)
{
	if (error) {
		error->code = code;
		snprintf(error->path, sizeof(error->path), "%s", path);
		snprintf(error->message, sizeof(error->message), "%s", message);
	}
	return 0;
}

static void workflow_delete(void *value)
{
	struct stored_workflow *workflow = value;
	if (!workflow)
		return;
	free(workflow->workflow_id);
	free(workflow->document);
	transaction_list_delete(workflow->delta_head);
	if (workflow->attempts)
		itable_delete(workflow->attempts);
	if (workflow->completed_tasks)
		itable_delete(workflow->completed_tasks);
	if (workflow->data_producers)
		itable_delete(workflow->data_producers);
	vine_datavine_parametric_delete(workflow->parametric);
	free(workflow);
}

static int update_graph_indices(struct stored_workflow *workflow,
		struct jx *root, int initial)
{
	if (initial) {
		if (vine_datavine_parametric_present(root)) {
			workflow->parametric = vine_datavine_parametric_parse(root, 0);
			if (!workflow->parametric)
				return 0;
			workflow->maximum_tasks = workflow->parametric->tasks;
			workflow->maximum_edges = workflow->parametric->scheduler_edges;
			workflow->maximum_task_id = workflow->parametric->tasks;
			workflow->maximum_data_id = workflow->parametric->c_data_first +
					workflow->parametric->c_tasks - 1;
		}
		struct jx *policy = jx_lookup(root, "policy");
		struct jx *maximum_tasks = policy ? jx_lookup(policy,
									"maximum_tasks")
						  : 0;
		struct jx *maximum_edges = policy ? jx_lookup(policy,
									"maximum_edges")
						  : 0;
		workflow->maximum_tasks = workflow->parametric
					? workflow->maximum_tasks
					: maximum_tasks
							  ? (uint64_t)maximum_tasks->u.integer_value
							  : UINT64_C(10000000);
		workflow->maximum_edges = workflow->parametric
					? workflow->maximum_edges
					: maximum_edges
							  ? (uint64_t)maximum_edges->u.integer_value
							  : UINT64_C(100000000);
	}
	struct jx *record;
	void *iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "data"), &iterator))) {
		uint64_t data_id = vine_datavine_ir_data_id(record);
		int64_t producer = vine_datavine_ir_data_producer(record);
		if (!itable_insert(workflow->data_producers, data_id, (void *)(uintptr_t)(producer + 1)))
			return 0;
		if (data_id > workflow->maximum_data_id)
			workflow->maximum_data_id = data_id;
	}
	iterator = 0;
	while ((record = jx_iterate_array(jx_lookup(root, "tasks"), &iterator))) {
		uint64_t task_id = vine_datavine_ir_task_id(record);
		if (task_id > workflow->maximum_task_id)
			workflow->maximum_task_id = task_id;
	}
	return 1;
}

static int lookup_accepted_data(void *context, uint64_t data_id,
		int64_t *producer_task_id)
{
	struct stored_workflow *workflow = context;
	void *encoded = itable_lookup(workflow->data_producers, data_id);
	if (!encoded)
		return 0;
	if (producer_task_id)
		*producer_task_id = (int64_t)(uintptr_t)encoded - 1;
	return 1;
}

static void release_terminal_graph(struct stored_workflow *workflow)
{
	free(workflow->document);
	workflow->document = 0;
	workflow->document_size = 0;
	transaction_list_delete(workflow->delta_head);
	workflow->delta_head = 0;
	workflow->delta_tail = 0;
	if (workflow->data_producers) {
		itable_delete(workflow->data_producers);
		workflow->data_producers = 0;
	}
	if (workflow->attempts) {
		itable_delete(workflow->attempts);
		workflow->attempts = 0;
	}
	if (workflow->completed_tasks) {
		itable_delete(workflow->completed_tasks);
		workflow->completed_tasks = 0;
	}
}

static void idempotency_delete(void *value)
{
	free(value);
}

static int parse_identity(const char *json, size_t size,
		const char *fallback_digest, char **workflow_id,
		char **idempotency_key, int *streaming)
{
	struct jx *root = jx_parse_string_and_length(json, (int)size);
	if (!root)
		return 0;
	const char *id = jx_lookup_string(root, "workflow_id");
	const char *key = jx_lookup_string(root, "idempotency_key");
	const char *mode = jx_lookup_string(root, "mode");
	char generated[64];
	if (!id) {
		snprintf(generated, sizeof(generated), "dv-%s", fallback_digest);
		id = generated;
	}
	*workflow_id = strdup(id);
	*idempotency_key = key ? strdup(key) : 0;
	*streaming = mode && !strcmp(mode, "streaming") ? 1 : mode && !strcmp(mode, "staged") ? 2
												  : 0;
	jx_delete(root);
	if (!*workflow_id || !*idempotency_key) {
		free(*workflow_id);
		free(*idempotency_key);
		*workflow_id = 0;
		*idempotency_key = 0;
		return 0;
	}
	return 1;
}

static void add_event(struct stored_workflow *workflow,
		enum vine_datavine_workflow_event_type type)
{
	workflow->info.event_id++;
	if (workflow->event_count == STORE_MAX_EVENTS) {
		workflow->event_start = (workflow->event_start + 1) % STORE_MAX_EVENTS;
	} else {
		workflow->event_count++;
	}
	struct vine_datavine_workflow_event *event =
			&workflow->events[(workflow->event_start +
							  workflow->event_count - 1) %
					  STORE_MAX_EVENTS];
	memset(event, 0, sizeof(*event));
	event->event_id = workflow->info.event_id;
	event->generation = workflow->info.generation;
	event->type = type;
	snprintf(event->digest, sizeof(event->digest), "%s", workflow->info.digest);
}

static int apply_task_event(struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		enum vine_datavine_workflow_event_type type,
		int64_t task_id, uint32_t attempt, int32_t result,
		struct vine_datavine_workflow_error *error, int commit)
{
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (!workflow)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_REFERENCE, "$.workflow_id", "unknown workflow_id");
	if ((workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING &&
				workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING_OPEN &&
				workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT) ||
			task_id < 1 ||
			!attempt || type < VINE_DATAVINE_WORKFLOW_TASK_SUBMITTED ||
			type > VINE_DATAVINE_WORKFLOW_TASK_FAILED)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.event", "invalid task event");
	size_t id_size = strlen(workflow_id);
	if (id_size > UINT32_MAX)
		return 0;
	if (commit) {
		unsigned char *payload = calloc(1, 24 + id_size);
		if (!payload)
			return 0;
		vine_datavine_put_u32(payload, (uint32_t)id_size);
		vine_datavine_put_u32(payload + 4, (uint32_t)type);
		vine_datavine_put_u64(payload + 8, (uint64_t)task_id);
		vine_datavine_put_u32(payload + 16, attempt);
		vine_datavine_put_u32(payload + 20, (uint32_t)result);
		memcpy(payload + 24, workflow_id, id_size);
		int committed = vine_datavine_journal_enqueue(store->journal,
				STORE_TASK_EVENT,
				payload,
				24 + id_size);
		free(payload);
		if (!committed)
			return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "task event journal commit failed");
	}
	add_event(workflow, type);
	struct vine_datavine_workflow_event *event =
			&workflow->events[(workflow->event_start +
							  workflow->event_count - 1) %
					  STORE_MAX_EVENTS];
	event->task_id = task_id;
	event->attempt = attempt;
	event->result = result;
	if (type == VINE_DATAVINE_WORKFLOW_TASK_SUBMITTED)
		itable_insert(workflow->attempts, (uint64_t)task_id, (void *)(uintptr_t)attempt);
	if (type == VINE_DATAVINE_WORKFLOW_TASK_COMPLETED)
		itable_insert(workflow->completed_tasks, (uint64_t)task_id, (void *)(uintptr_t)attempt);
	else if (type == VINE_DATAVINE_WORKFLOW_TASK_RETRY ||
			type == VINE_DATAVINE_WORKFLOW_TASK_FAILED)
		itable_remove(workflow->completed_tasks, (uint64_t)task_id);
	return 1;
}

static int remember_idempotency(struct vine_datavine_workflow_store *store,
		const char *key, struct stored_workflow *workflow,
		const char *digest)
{
	struct idempotency_record *record = calloc(1, sizeof(*record));
	if (!record)
		return 0;
	record->workflow = workflow;
	snprintf(record->digest, sizeof(record->digest), "%s", digest);
	if (!hash_table_insert(store->idempotency, key, record)) {
		free(record);
		return 0;
	}
	return 1;
}

static int apply_submit(struct vine_datavine_workflow_store *store,
		const char *json, size_t size,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error, int commit)
{
	char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1];
	struct vine_datavine_workflow_summary summary;
	if (!vine_datavine_workflow_validate(json, size, digest, &summary, error))
		return 0;
	char *workflow_id = 0;
	char *key = 0;
	int streaming = 0;
	if (!parse_identity(json, size, digest, &workflow_id, &key, &streaming))
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not allocate workflow identity");
	struct idempotency_record *known = hash_table_lookup(store->idempotency, key);
	if (known) {
		int same = !strcmp(known->digest, digest);
		if (same && result)
			*result = known->workflow->info;
		free(workflow_id);
		free(key);
		return same ? 1 : store_fail(error, VINE_DATAVINE_WORKFLOW_DUPLICATE, "$.idempotency_key", "idempotency key already names different content");
	}
	if (store->workflow) {
		free(workflow_id);
		free(key);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_DUPLICATE, "$.workflow_id", "workflow_id already exists");
	}
	struct stored_workflow *workflow = calloc(1, sizeof(*workflow));
	char *document = malloc(size + 1);
	if (!workflow || !document) {
		free(workflow);
		free(document);
		free(workflow_id);
		free(key);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not allocate workflow");
	}
	memcpy(document, json, size);
	document[size] = 0;
	workflow->workflow_id = workflow_id;
	workflow->document = document;
	workflow->document_size = size;
	workflow->attempts = itable_create(0);
	workflow->completed_tasks = itable_create(0);
	workflow->data_producers = itable_create(0);
	if (!workflow->attempts || !workflow->completed_tasks ||
			!workflow->data_producers) {
		workflow_delete(workflow);
		free(key);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not allocate workflow indices");
	}
	struct jx *accepted_root = jx_parse_string_and_length(json, (int)size);
	if (!accepted_root || !update_graph_indices(workflow, accepted_root, 1)) {
		if (accepted_root)
			jx_delete(accepted_root);
		workflow_delete(workflow);
		free(key);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not build workflow graph indices");
	}
	jx_delete(accepted_root);
	snprintf(workflow->info.workflow_id, sizeof(workflow->info.workflow_id), "%s", workflow_id);
	snprintf(workflow->info.digest, sizeof(workflow->info.digest), "%s", digest);
	workflow->info.generation = 1;
	workflow->info.state = streaming == 1 ? VINE_DATAVINE_WORKFLOW_OPEN : streaming == 2 ? VINE_DATAVINE_WORKFLOW_STAGED
												 : VINE_DATAVINE_WORKFLOW_SEALED;
	workflow->info.summary = summary;
	add_event(workflow, VINE_DATAVINE_WORKFLOW_ACCEPTED);
	if (commit && !vine_datavine_journal_commit(store->journal, STORE_SUBMIT, (const unsigned char *)json, size)) {
		workflow_delete(workflow);
		free(key);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "workflow journal commit failed");
	}
	store->workflow = workflow;
	if (!remember_idempotency(store, key, workflow, digest)) {
		store->workflow = 0;
		workflow_delete(workflow);
		free(key);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "workflow index insertion failed");
	}
	free(key);
	if (result)
		*result = workflow->info;
	return 1;
}

static int apply_append_delta(struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t expected_generation,
		const char *json, size_t size,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error, int commit)
{
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (!workflow)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_REFERENCE, "$.workflow_id", "unknown workflow_id");
	char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1];
	struct stored_transaction *transaction = transaction_create(json, size);
	if (!transaction)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not allocate delta transaction");
	struct vine_datavine_workflow_summary delta;
	if (!vine_datavine_workflow_delta_validate_parsed(transaction->root, workflow->maximum_task_id, workflow->maximum_data_id, workflow->maximum_tasks, workflow->maximum_edges, workflow->info.summary.tasks, workflow->info.summary.edges, lookup_accepted_data, workflow, digest, &delta, error)) {
		transaction_delete(transaction);
		return 0;
	}
	const char *delta_id = jx_lookup_string(transaction->root, "workflow_id");
	const char *idempotency_key = jx_lookup_string(transaction->root, "idempotency_key");
	if (strcmp(delta_id, workflow_id)) {
		transaction_delete(transaction);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.workflow_id", "delta names a different workflow");
	}
	struct idempotency_record *known = hash_table_lookup(store->idempotency,
			idempotency_key);
	if (known) {
		int same = known->workflow == workflow && !strcmp(known->digest, digest);
		if (same && result)
			*result = workflow->info;
		transaction_delete(transaction);
		return same ? 1 : store_fail(error, VINE_DATAVINE_WORKFLOW_DUPLICATE, "$.idempotency_key", "idempotency key already names different content");
	}
	if ((workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN &&
				workflow->info.state != VINE_DATAVINE_WORKFLOW_STAGED &&
				workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING_OPEN &&
				workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT) ||
			workflow->info.generation != expected_generation) {
		transaction_delete(transaction);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.generation", "workflow is not appendable at expected generation");
	}
	struct idempotency_record *record = calloc(1, sizeof(*record));
	char *key = strdup(idempotency_key);
	if (!record || !key) {
		transaction_delete(transaction);
		free(record);
		free(key);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not allocate delta transaction");
	}
	if (commit) {
		unsigned char *payload = malloc(size + 8);
		if (!payload) {
			transaction_delete(transaction);
			free(record);
			free(key);
			return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not allocate delta journal payload");
		}
		vine_datavine_put_u64(payload, expected_generation);
		memcpy(payload + 8, json, size);
		int committed = vine_datavine_journal_commit(store->journal,
				STORE_APPEND_DELTA,
				payload,
				size + 8);
		free(payload);
		if (!committed) {
			transaction_delete(transaction);
			free(record);
			free(key);
			return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "delta journal commit failed");
		}
	}
	if (!update_graph_indices(workflow, transaction->root, 0)) {
		transaction_delete(transaction);
		free(record);
		free(key);
		if (commit)
			abort();
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not update delta indices");
	}
	workflow->info.generation++;
	workflow->info.summary.tasks += delta.tasks;
	workflow->info.summary.data += delta.data;
	workflow->info.summary.edges += delta.edges;
	workflow->info.summary.requested_outputs += delta.requested_outputs;
	snprintf(workflow->info.digest, sizeof(workflow->info.digest), "%s", digest);
	transaction->generation = workflow->info.generation;
	if (workflow->delta_tail)
		workflow->delta_tail->next = transaction;
	else
		workflow->delta_head = transaction;
	workflow->delta_tail = transaction;
	add_event(workflow, VINE_DATAVINE_WORKFLOW_APPENDED);
	if (workflow->info.state == VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT) {
		workflow->info.state = VINE_DATAVINE_WORKFLOW_RUNNING_OPEN;
		add_event(workflow, VINE_DATAVINE_WORKFLOW_RESUMED_EVENT);
	}
	record->workflow = workflow;
	snprintf(record->digest, sizeof(record->digest), "%s", digest);
	if (!hash_table_insert(store->idempotency, key, record)) {
		if (commit)
			abort();
		free(record);
		free(key);
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "delta idempotency insertion failed");
	}
	free(key);
	if (result)
		*result = workflow->info;
	return 1;
}

static int apply_transition(struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t expected_generation,
		enum vine_datavine_workflow_state state,
		enum vine_datavine_workflow_event_type event_type, uint16_t opcode,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error, int commit)
{
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (!workflow)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_REFERENCE, "$.workflow_id", "unknown workflow_id");
	if (workflow->info.state == state) {
		if (result)
			*result = workflow->info;
		return 1;
	}
	if (state == VINE_DATAVINE_WORKFLOW_SEALED &&
			((workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN &&
					 workflow->info.state != VINE_DATAVINE_WORKFLOW_STAGED &&
					 workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT) ||
					workflow->info.generation != expected_generation))
		return store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.generation", "workflow is not open at expected generation");
	if (state == VINE_DATAVINE_WORKFLOW_RUNNING &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_SEALED &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING_OPEN &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.state", "workflow is not sealed");
	if ((state == VINE_DATAVINE_WORKFLOW_COMPLETED ||
				state == VINE_DATAVINE_WORKFLOW_FAILED) &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING &&
			!(state == VINE_DATAVINE_WORKFLOW_FAILED &&
					workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN))
		return store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.state", "workflow is not running");
	if (state == VINE_DATAVINE_WORKFLOW_CANCELLED &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_STAGED &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_SEALED &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING_OPEN &&
			workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.state", "terminal workflow cannot be cancelled");
	size_t id_size = strlen(workflow_id);
	if (id_size > UINT32_MAX)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$.workflow_id", "workflow_id is too large");
	unsigned char *payload = malloc(id_size + 12);
	if (!payload)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not allocate transition payload");
	vine_datavine_put_u64(payload, expected_generation);
	vine_datavine_put_u32(payload + 8, (uint32_t)id_size);
	memcpy(payload + 12, workflow_id, id_size);
	int committed = !commit || vine_datavine_journal_commit(store->journal, opcode, payload, id_size + 12);
	free(payload);
	if (!committed)
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "transition journal commit failed");
	workflow->info.state = state;
	add_event(workflow, event_type);
	if (state == VINE_DATAVINE_WORKFLOW_COMPLETED ||
			state == VINE_DATAVINE_WORKFLOW_FAILED) {
		workflow->runtime_claimed = 0;
		release_terminal_graph(workflow);
	}
	if (result)
		*result = workflow->info;
	return 1;
}

static int apply_recover(struct vine_datavine_workflow_store *store,
		const char *workflow_id, struct vine_datavine_workflow_error *error,
		int commit)
{
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (!workflow || (workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING &&
					 workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING_OPEN &&
					 workflow->info.state != VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT))
		return store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.state", "only a running workflow can be recovered");
	size_t id_size = strlen(workflow_id);
	if (commit && !vine_datavine_journal_commit(store->journal, STORE_RECOVER, (const unsigned char *)workflow_id, id_size))
		return store_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "workflow recovery journal commit failed");
	workflow->info.state = workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING
						   ? VINE_DATAVINE_WORKFLOW_SEALED
						   : VINE_DATAVINE_WORKFLOW_OPEN;
	add_event(workflow, VINE_DATAVINE_WORKFLOW_RECOVERED_EVENT);
	workflow->runtime_claimed = 0;
	return 1;
}

int vine_datavine_workflow_store_recover(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		struct vine_datavine_workflow_error *error)
{
	if (!store || !workflow_id || !error)
		return 0;
	pthread_mutex_lock(&store->lock);
	int valid = apply_recover(store, workflow_id, error, 1);
	pthread_mutex_unlock(&store->lock);
	return valid;
}

static int replay_workflow_id(const unsigned char *payload, size_t payload_size,
		size_t offset, size_t id_size,
		char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1])
{
	if (!id_size || id_size > VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX ||
			offset > payload_size || id_size > payload_size - offset)
		return 0;
	memcpy(workflow_id, payload + offset, id_size);
	workflow_id[id_size] = 0;
	return 1;
}

static int replay_append_delta(struct vine_datavine_workflow_store *store,
		const unsigned char *payload, size_t payload_size,
		struct vine_datavine_workflow_error *error)
{
	if (payload_size < 8)
		return 0;
	struct jx *root = jx_parse_string_and_length(
			(const char *)payload + 8, (int)(payload_size - 8));
	const char *id = root ? jx_lookup_string(root, "workflow_id") : 0;
	char *workflow_id = id ? strdup(id) : 0;
	if (root)
		jx_delete(root);
	int valid = workflow_id && apply_append_delta(store,
						   workflow_id,
						   vine_datavine_get_u64(payload),
						   (const char *)payload + 8,
						   payload_size - 8,
						   0,
						   error,
						   0);
	free(workflow_id);
	return valid;
}

static int replay_task_event(struct vine_datavine_workflow_store *store,
		const unsigned char *payload, size_t payload_size,
		struct vine_datavine_workflow_error *error)
{
	if (payload_size < 24)
		return 0;
	uint32_t id_size = vine_datavine_get_u32(payload);
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (payload_size != 24U + id_size ||
			!replay_workflow_id(payload, payload_size, 24, id_size, workflow_id))
		return 0;
	return apply_task_event(store,
			workflow_id,
			(enum vine_datavine_workflow_event_type)vine_datavine_get_u32(
					payload + 4),
			(int64_t)vine_datavine_get_u64(payload + 8),
			vine_datavine_get_u32(payload + 16),
			(int32_t)vine_datavine_get_u32(payload + 20),
			error,
			0);
}

static int replay_task_event_batch(struct vine_datavine_workflow_store *store,
		const unsigned char *payload, size_t payload_size,
		struct vine_datavine_workflow_error *error)
{
	if (payload_size < 8)
		return 0;
	uint32_t id_size = vine_datavine_get_u32(payload);
	uint32_t count = vine_datavine_get_u32(payload + 4);
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (!count || count > VINE_DATAVINE_WORKFLOW_EVENT_BATCH_MAX ||
			payload_size != 8U + id_size + (size_t)count * 20U ||
			!replay_workflow_id(payload, payload_size, 8, id_size, workflow_id))
		return 0;
	const unsigned char *record = payload + 8 + id_size;
	for (uint32_t index = 0; index < count; index++, record += 20) {
		if (!apply_task_event(store,
					workflow_id,
					(enum vine_datavine_workflow_event_type)
							vine_datavine_get_u32(record),
					(int64_t)vine_datavine_get_u64(record + 4),
					vine_datavine_get_u32(record + 12),
					(int32_t)vine_datavine_get_u32(record + 16),
					error,
					0))
			return 0;
	}
	return 1;
}

struct replay_transition {
	uint16_t opcode;
	enum vine_datavine_workflow_state state;
	enum vine_datavine_workflow_event_type event;
};

static const struct replay_transition replay_transitions[] = {
		{STORE_SEAL, VINE_DATAVINE_WORKFLOW_SEALED, VINE_DATAVINE_WORKFLOW_SEALED_EVENT},
		{STORE_CANCEL, VINE_DATAVINE_WORKFLOW_CANCELLED, VINE_DATAVINE_WORKFLOW_CANCELLED_EVENT},
		{STORE_START, VINE_DATAVINE_WORKFLOW_RUNNING, VINE_DATAVINE_WORKFLOW_STARTED},
		{STORE_COMPLETE, VINE_DATAVINE_WORKFLOW_COMPLETED, VINE_DATAVINE_WORKFLOW_COMPLETED_EVENT},
		{STORE_FAIL, VINE_DATAVINE_WORKFLOW_FAILED, VINE_DATAVINE_WORKFLOW_FAILED_EVENT},
};

static int replay_transition(struct vine_datavine_workflow_store *store,
		uint16_t opcode, const unsigned char *payload, size_t payload_size,
		struct vine_datavine_workflow_error *error)
{
	if (payload_size < 12)
		return 0;
	uint32_t id_size = vine_datavine_get_u32(payload + 8);
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (payload_size != 12U + id_size ||
			!replay_workflow_id(payload, payload_size, 12, id_size, workflow_id))
		return 0;
	const struct replay_transition *transition = 0;
	for (size_t index = 0;
			index < sizeof(replay_transitions) / sizeof(replay_transitions[0]);
			index++) {
		if (replay_transitions[index].opcode == opcode) {
			transition = &replay_transitions[index];
			break;
		}
	}
	if (!transition)
		return 0;
	enum vine_datavine_workflow_state state = transition->state;
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (opcode == STORE_SEAL && workflow &&
			(workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN ||
					workflow->info.state ==
							VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT))
		state = VINE_DATAVINE_WORKFLOW_RUNNING;
	else if (opcode == STORE_START && workflow &&
			workflow->info.state == VINE_DATAVINE_WORKFLOW_OPEN)
		state = VINE_DATAVINE_WORKFLOW_RUNNING_OPEN;
	return apply_transition(store,
			workflow_id,
			vine_datavine_get_u64(payload),
			state,
			transition->event,
			opcode,
			0,
			error,
			0);
}

static int replay_identifier_record(struct vine_datavine_workflow_store *store,
		uint16_t opcode, const unsigned char *payload, size_t payload_size,
		struct vine_datavine_workflow_error *error)
{
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (!replay_workflow_id(payload, payload_size, 0, payload_size, workflow_id))
		return 0;
	if (opcode == STORE_CHECKPOINT)
		return 1;
	if (opcode == STORE_RECOVER)
		return apply_recover(store, workflow_id, error, 0);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (opcode != STORE_QUIESCENT || !workflow ||
			workflow->info.state != VINE_DATAVINE_WORKFLOW_RUNNING_OPEN)
		return 0;
	workflow->info.state = VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT;
	add_event(workflow, VINE_DATAVINE_WORKFLOW_QUIESCENT_EVENT);
	return 1;
}

static int replay_record(void *context, uint16_t opcode,
		const unsigned char *payload, size_t payload_size)
{
	struct vine_datavine_workflow_store *store = context;
	struct vine_datavine_workflow_error error;
	switch (opcode) {
	case STORE_SUBMIT:
		return apply_submit(store, (const char *)payload, payload_size, 0, &error, 0);
	case STORE_APPEND_DELTA:
		return replay_append_delta(store, payload, payload_size, &error);
	case STORE_TASK_EVENT:
		return replay_task_event(store, payload, payload_size, &error);
	case STORE_TASK_EVENT_BATCH:
		return replay_task_event_batch(store, payload, payload_size, &error);
	case STORE_RECOVER:
	case STORE_CHECKPOINT:
	case STORE_QUIESCENT:
		return replay_identifier_record(store, opcode, payload, payload_size, &error);
	case STORE_SEAL:
	case STORE_CANCEL:
	case STORE_START:
	case STORE_COMPLETE:
	case STORE_FAIL:
		return replay_transition(store, opcode, payload, payload_size, &error);
	default:
		return opcode < STORE_SUBMIT;
	}
}

struct vine_datavine_workflow_store *vine_datavine_workflow_store_open(
		const char *journal_path)
{
	struct vine_datavine_workflow_store *store = calloc(1, sizeof(*store));
	if (!store)
		return 0;
	if (pthread_mutex_init(&store->lock, 0)) {
		free(store);
		return 0;
	}
	store->idempotency = hash_table_create(0, 0);
	if (!store->idempotency) {
		vine_datavine_workflow_store_close(store);
		return 0;
	}
	store->journal = vine_datavine_journal_open(journal_path);
	if (!store->journal || !vine_datavine_journal_replay(store->journal, replay_record, store)) {
		vine_datavine_workflow_store_close(store);
		return 0;
	}
	{
		struct stored_workflow *workflow = store->workflow;
		if (workflow && (workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING ||
				workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN ||
				workflow->info.state == VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT)) {
			struct vine_datavine_workflow_error error;
			if (!apply_recover(store, workflow->workflow_id, &error, 1)) {
				vine_datavine_workflow_store_close(store);
				return 0;
			}
		}
	}
	return store;
}

struct vine_datavine_journal *vine_datavine_workflow_store_journal(
		struct vine_datavine_workflow_store *store)
{
	return store ? store->journal : 0;
}

void vine_datavine_workflow_store_close(struct vine_datavine_workflow_store *store)
{
	if (!store)
		return;
	if (store->journal)
		vine_datavine_journal_close(store->journal);
	if (store->idempotency) {
		hash_table_clear(store->idempotency, idempotency_delete);
		hash_table_delete(store->idempotency);
	}
	workflow_delete(store->workflow);
	pthread_mutex_destroy(&store->lock);
	free(store);
}

int vine_datavine_workflow_store_submit(struct vine_datavine_workflow_store *store,
		const char *json, size_t size, struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error)
{
	if (!store)
		return 0;
	pthread_mutex_lock(&store->lock);
	int valid = apply_submit(store, json, size, result, error, 1);
	pthread_mutex_unlock(&store->lock);
	return valid;
}

int vine_datavine_workflow_store_append_delta(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t expected_generation,
		const char *json, size_t size,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error)
{
	if (!store || !workflow_id)
		return 0;
	pthread_mutex_lock(&store->lock);
	int valid = apply_append_delta(store, workflow_id, expected_generation, json, size, result, error, 1);
	pthread_mutex_unlock(&store->lock);
	return valid;
}

int vine_datavine_workflow_store_seal(struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t expected_generation,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error)
{
	if (!store || !workflow_id)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	enum vine_datavine_workflow_state state = workflow &&
										  workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN
								  ? VINE_DATAVINE_WORKFLOW_RUNNING
								  : VINE_DATAVINE_WORKFLOW_SEALED;
	int valid = apply_transition(store, workflow_id, expected_generation, state, VINE_DATAVINE_WORKFLOW_SEALED_EVENT, STORE_SEAL, result, error, 1);
	pthread_mutex_unlock(&store->lock);
	return valid;
}

int vine_datavine_workflow_store_cancel(struct vine_datavine_workflow_store *store,
		const char *workflow_id, struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error)
{
	if (!store || !workflow_id)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	uint64_t generation = workflow ? workflow->info.generation : 0;
	int valid = apply_transition(store, workflow_id, generation, VINE_DATAVINE_WORKFLOW_CANCELLED, VINE_DATAVINE_WORKFLOW_CANCELLED_EVENT, STORE_CANCEL, result, error, 1);
	pthread_mutex_unlock(&store->lock);
	return valid;
}

int vine_datavine_workflow_store_describe(struct vine_datavine_workflow_store *store,
		const char *workflow_id, struct vine_datavine_workflow_info *result)
{
	if (!store || !workflow_id || !result)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (workflow)
		*result = workflow->info;
	pthread_mutex_unlock(&store->lock);
	return workflow != 0;
}

int vine_datavine_workflow_store_frontier(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t *maximum_task_id,
		uint64_t *maximum_data_id, uint64_t *maximum_tasks,
		uint64_t *maximum_edges)
{
	if (!store || !workflow_id || !maximum_task_id || !maximum_data_id ||
			!maximum_tasks || !maximum_edges)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (workflow) {
		*maximum_task_id = workflow->maximum_task_id;
		*maximum_data_id = workflow->maximum_data_id;
		*maximum_tasks = workflow->maximum_tasks;
		*maximum_edges = workflow->maximum_edges;
	}
	pthread_mutex_unlock(&store->lock);
	return workflow != 0;
}

size_t vine_datavine_workflow_store_watch(struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t after_event_id,
		struct vine_datavine_workflow_event *events, size_t capacity)
{
	if (!store || !workflow_id || (!events && capacity))
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	size_t count = 0;
	if (workflow) {
		for (size_t i = 0; i < workflow->event_count && count < capacity; i++) {
			struct vine_datavine_workflow_event *event =
					&workflow->events[(workflow->event_start + i) %
							  STORE_MAX_EVENTS];
			if (event->event_id > after_event_id)
				events[count++] = *event;
		}
	}
	pthread_mutex_unlock(&store->lock);
	return count;
}

int vine_datavine_workflow_store_take_runnable(
		struct vine_datavine_workflow_store *store,
		struct vine_datavine_workflow_info *result,
		char **document, size_t *document_size)
{
	if (!store || !result || !document || !document_size)
		return 0;
	*document = 0;
	*document_size = 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = 0;
	struct stored_workflow *candidate = store->workflow;
	if (candidate && !candidate->runtime_claimed &&
			(candidate->info.state == VINE_DATAVINE_WORKFLOW_SEALED ||
					candidate->info.state == VINE_DATAVINE_WORKFLOW_OPEN ||
					candidate->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN ||
					candidate->info.state == VINE_DATAVINE_WORKFLOW_RUNNING))
		workflow = candidate;
	int valid = 0;
	if (workflow) {
		char *copy = malloc(workflow->document_size + 1);
		if (copy) {
			memcpy(copy, workflow->document, workflow->document_size);
			copy[workflow->document_size] = 0;
			struct vine_datavine_workflow_error error;
			enum vine_datavine_workflow_state state = workflow->info.state ==
												  VINE_DATAVINE_WORKFLOW_OPEN
										  ? VINE_DATAVINE_WORKFLOW_RUNNING_OPEN
										  : VINE_DATAVINE_WORKFLOW_RUNNING;
			/* A resumed workflow already has its durable START event. This also
			 * covers append immediately followed by seal before reactor claim. */
			if (workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN ||
					workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING) {
				valid = 1;
				*result = workflow->info;
				/* START is the only transition from OPEN into RUNNING_OPEN. */
			} else if (state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN) {
				size_t id_size = strlen(workflow->workflow_id);
				unsigned char *payload = malloc(id_size + 12);
				if (payload) {
					vine_datavine_put_u64(payload, workflow->info.generation);
					vine_datavine_put_u32(payload + 8, (uint32_t)id_size);
					memcpy(payload + 12, workflow->workflow_id, id_size);
					valid = vine_datavine_journal_commit(store->journal,
							STORE_START,
							payload,
							id_size + 12);
					free(payload);
				}
				if (valid) {
					workflow->info.state = state;
					add_event(workflow, VINE_DATAVINE_WORKFLOW_STARTED);
					*result = workflow->info;
				}
			} else {
				valid = apply_transition(store, workflow->workflow_id, workflow->info.generation, state, VINE_DATAVINE_WORKFLOW_STARTED, STORE_START, result, &error, 1);
			}
			if (valid) {
				workflow->runtime_claimed = 1;
				*document = copy;
				*document_size = workflow->document_size;
			} else {
				free(copy);
			}
		}
	}
	pthread_mutex_unlock(&store->lock);
	return valid;
}

struct jx *vine_datavine_workflow_store_take_delta_root(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint64_t after_generation,
		uint64_t *generation)
{
	if (!store || !workflow_id || !generation)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	struct stored_transaction *transaction = workflow
								 ? workflow->delta_head
								 : 0;
	while (transaction && transaction->generation <= after_generation)
		transaction = transaction->next;
	struct jx *root = 0;
	if (transaction) {
		root = transaction->root;
		transaction->root = 0;
		if (!root)
			root = jx_parse_string_and_length(transaction->document,
					(int)transaction->document_size);
		if (root)
			*generation = transaction->generation;
	}
	pthread_mutex_unlock(&store->lock);
	return root;
}

int vine_datavine_workflow_store_mark_quiescent(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		uint64_t expected_generation,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error)
{
	if (!store || !workflow_id)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	if (workflow && workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN &&
			workflow->info.generation != expected_generation) {
		if (result)
			*result = workflow->info;
		pthread_mutex_unlock(&store->lock);
		return 2;
	}
	int valid = workflow && workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN &&
			workflow->info.generation == expected_generation;
	if (valid && !vine_datavine_journal_commit(store->journal, STORE_QUIESCENT, (const unsigned char *)workflow_id, strlen(workflow_id)))
		valid = 0;
	if (valid) {
		workflow->info.state = VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT;
		workflow->runtime_claimed = 0;
		add_event(workflow, VINE_DATAVINE_WORKFLOW_QUIESCENT_EVENT);
		if (result)
			*result = workflow->info;
	} else if (workflow && workflow->info.state == VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT &&
			workflow->info.generation == expected_generation) {
		valid = 1;
		if (result)
			*result = workflow->info;
	} else if (!workflow) {
		store_fail(error, VINE_DATAVINE_WORKFLOW_REFERENCE, "$.workflow_id", "unknown workflow_id");
	} else {
		store_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.state", "workflow is not running and open");
	}
	pthread_mutex_unlock(&store->lock);
	return valid;
}

int vine_datavine_workflow_store_finish(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, int successful,
		struct vine_datavine_workflow_info *result,
		struct vine_datavine_workflow_error *error)
{
	if (!store || !workflow_id)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	uint64_t generation = workflow ? workflow->info.generation : 0;
	int valid = apply_transition(store, workflow_id, generation, successful ? VINE_DATAVINE_WORKFLOW_COMPLETED : VINE_DATAVINE_WORKFLOW_FAILED, successful ? VINE_DATAVINE_WORKFLOW_COMPLETED_EVENT : VINE_DATAVINE_WORKFLOW_FAILED_EVENT, successful ? STORE_COMPLETE : STORE_FAIL, result, error, 1);
	pthread_mutex_unlock(&store->lock);
	return valid;
}

int vine_datavine_workflow_store_record_task_events(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id,
		const struct vine_datavine_workflow_task_event_record *records,
		size_t count, struct vine_datavine_workflow_error *error)
{
	if (!store || !workflow_id || !records || !count ||
			count > VINE_DATAVINE_WORKFLOW_EVENT_BATCH_MAX)
		return 0;
	size_t id_size = strlen(workflow_id);
	if (!id_size || id_size > VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX ||
			count > (SIZE_MAX - 8 - id_size) / 20)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	int valid = workflow &&
			(workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING ||
					workflow->info.state == VINE_DATAVINE_WORKFLOW_RUNNING_OPEN ||
					workflow->info.state == VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT);
	for (size_t index = 0; valid && index < count; index++)
		valid = records[index].task_id > 0 && records[index].attempt > 0 &&
			records[index].type >= VINE_DATAVINE_WORKFLOW_TASK_SUBMITTED &&
			records[index].type <= VINE_DATAVINE_WORKFLOW_TASK_FAILED;
	if (!valid) {
		store_fail(error, workflow ? VINE_DATAVINE_WORKFLOW_VALUE : VINE_DATAVINE_WORKFLOW_REFERENCE, workflow ? "$.event" : "$.workflow_id", workflow ? "invalid task event batch" : "unknown workflow_id");
	}
	size_t payload_size = 8 + id_size + count * 20;
	unsigned char *payload = valid ? malloc(payload_size) : 0;
	if (valid && !payload)
		valid = 0;
	if (valid) {
		vine_datavine_put_u32(payload, (uint32_t)id_size);
		vine_datavine_put_u32(payload + 4, (uint32_t)count);
		memcpy(payload + 8, workflow_id, id_size);
		unsigned char *record = payload + 8 + id_size;
		for (size_t index = 0; index < count; index++, record += 20) {
			vine_datavine_put_u32(record, (uint32_t)records[index].type);
			vine_datavine_put_u64(record + 4, (uint64_t)records[index].task_id);
			vine_datavine_put_u32(record + 12, records[index].attempt);
			vine_datavine_put_u32(record + 16, (uint32_t)records[index].result);
		}
		valid = vine_datavine_journal_enqueue(store->journal,
				STORE_TASK_EVENT_BATCH,
				payload,
				payload_size);
	}
	free(payload);
	for (size_t index = 0; valid && index < count; index++)
		valid = apply_task_event(store, workflow_id, records[index].type, records[index].task_id, records[index].attempt, records[index].result, error, 0);
	pthread_mutex_unlock(&store->lock);
	return valid;
}

uint32_t vine_datavine_workflow_store_task_attempts(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, int64_t task_id)
{
	if (!store || !workflow_id || task_id < 1)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	uint32_t attempts = workflow
						? (uint32_t)(uintptr_t)itable_lookup(workflow->attempts,
								  (uint64_t)task_id)
						: 0;
	pthread_mutex_unlock(&store->lock);
	return attempts;
}

int vine_datavine_workflow_store_task_attempts_snapshot(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, uint32_t *attempts, size_t count)
{
	if (!store || !workflow_id || !attempts || count < 2)
		return 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	int valid = workflow != 0;
	if (workflow) {
		uint64_t task_id;
		void *value;
		int iterator;
		ITABLE_ITERATE(workflow->attempts, iterator, task_id, value)
		{
			if (task_id >= count) {
				valid = 0;
				break;
			}
			attempts[task_id] = (uint32_t)(uintptr_t)value;
		}
	}
	pthread_mutex_unlock(&store->lock);
	return valid;
}

int vine_datavine_workflow_store_completed_task_ids(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id, int64_t **task_ids, size_t *count)
{
	if (!store || !workflow_id || !task_ids || !count)
		return 0;
	*task_ids = 0;
	*count = 0;
	pthread_mutex_lock(&store->lock);
	struct stored_workflow *workflow = workflow_lookup(store, workflow_id);
	size_t size = workflow ? (size_t)itable_size(workflow->completed_tasks) : 0;
	int64_t *copy = size ? malloc(size * sizeof(*copy)) : malloc(1);
	if (workflow && copy) {
		uint64_t task_id;
		void *value;
		int iterator;
		size_t index = 0;
		ITABLE_ITERATE(workflow->completed_tasks, iterator, task_id, value)
		{
			copy[index++] = (int64_t)task_id;
		}
		*task_ids = copy;
		*count = index;
	}
	pthread_mutex_unlock(&store->lock);
	return *task_ids != 0;
}

int vine_datavine_workflow_store_checkpoint(
		struct vine_datavine_workflow_store *store,
		const char *workflow_id)
{
	if (!store || !workflow_id || !workflow_id[0] ||
			strlen(workflow_id) > VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX)
		return 0;
	pthread_mutex_lock(&store->lock);
	int valid = workflow_lookup(store, workflow_id) &&
			vine_datavine_journal_commit(store->journal, STORE_CHECKPOINT, (const unsigned char *)workflow_id, strlen(workflow_id));
	pthread_mutex_unlock(&store->lock);
	return valid;
}
