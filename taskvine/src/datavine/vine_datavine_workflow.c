/* DataVine workflow implementation.
Copyright (C) 2026- The University of Notre Dame
See the file COPYING for details.
*/

#include "vine_datavine_workflow_store.h"

#include "b64.h"
#include "buffer.h"
#include "itable.h"
#include "jx.h"
#include "jx_canonicalize.h"
#include "jx_parse.h"
#include "sha1.h"
#include "vine_datavine_ir.h"
#include "vine_datavine_parametric.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define WORKFLOW_MAX_JSON (64U * 1024U * 1024U)
#define WORKFLOW_MAX_TASKS UINT64_C(10000000)
#define WORKFLOW_MAX_DATA UINT64_C(20000000)
#define WORKFLOW_MAX_EDGES UINT64_C(100000000)

struct validation {
	struct vine_datavine_workflow_error *error;
	struct itable *tasks;
	struct itable *data;
	struct itable *produced;
	uint64_t task_count;
	uint64_t data_count;
	uint64_t edge_count;
	uint64_t requested_count;
	uint64_t maximum_tasks;
	uint64_t maximum_edges;
	uint64_t existing_maximum_task_id;
	uint64_t existing_maximum_data_id;
	uint64_t existing_tasks;
	uint64_t existing_edges;
	vine_datavine_workflow_data_lookup_t lookup_data;
	void *lookup_context;
	int delta;
};

static int lookup_data_record(struct validation *v, uint64_t data_id,
		struct jx **record, int64_t *producer_task_id)
{
	struct jx *local = itable_lookup(v->data, data_id);
	if (local) {
		if (producer_task_id)
			*producer_task_id = vine_datavine_ir_data_producer(local);
		if (record)
			*record = local;
		return 1;
	}
	if (v->delta && data_id <= v->existing_maximum_data_id &&
			v->lookup_data && v->lookup_data(v->lookup_context, data_id, producer_task_id)) {
		if (record)
			*record = 0;
		return 1;
	}
	return 0;
}

static int fail(struct validation *v,
		enum vine_datavine_workflow_error_code code,
		const char *path, const char *format, ...)
{
	if (v->error) {
		v->error->code = code;
		snprintf(v->error->path, sizeof(v->error->path), "%s", path ? path : "$");
		va_list args;
		va_start(args, format);
		vsnprintf(v->error->message, sizeof(v->error->message), format, args);
		va_end(args);
	}
	return 0;
}

static struct jx *required(struct validation *v, struct jx *object,
		const char *key, jx_type_t type, const char *path)
{
	struct jx *value = jx_lookup(object, key);
	if (!value) {
		fail(v, VINE_DATAVINE_WORKFLOW_REQUIRED, path, "missing required field");
		return 0;
	}
	if (!jx_istype(value, type)) {
		fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "field has wrong type");
		return 0;
	}
	return value;
}

static int nonempty_string(struct validation *v, struct jx *object,
		const char *key, const char *path, size_t maximum)
{
	struct jx *value = required(v, object, key, JX_STRING, path);
	if (!value)
		return 0;
	size_t size = strlen(value->u.string_value);
	return size && size <= maximum
				   ? 1
				   : fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "string length must be between 1 and %zu", maximum);
}

static int positive_integer(struct validation *v, struct jx *object,
		const char *key, const char *path, uint64_t *result, int allow_zero)
{
	struct jx *value = required(v, object, key, JX_INTEGER, path);
	if (!value)
		return 0;
	if (value->u.integer_value < (allow_zero ? 0 : 1))
		return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "integer is outside the allowed range");
	*result = (uint64_t)value->u.integer_value;
	return 1;
}

static int optional_object(struct validation *v, struct jx *object,
		const char *key, const char *path)
{
	struct jx *value = jx_lookup(object, key);
	return !value || jx_istype(value, JX_OBJECT)
				   ? 1
				   : fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "field must be an object");
}

static int allowed_keys(struct validation *v, struct jx *object,
		const char *path, const char *const *allowed)
{
	void *iterator = 0;
	const char *key;
	while ((key = jx_iterate_keys(object, &iterator))) {
		int found = 0;
		for (size_t i = 0; allowed[i]; i++) {
			if (!strcmp(key, allowed[i])) {
				found = 1;
				break;
			}
		}
		if (!found) {
			char key_path[256];
			snprintf(key_path, sizeof(key_path), "%s.%s", path, key);
			return fail(v, VINE_DATAVINE_WORKFLOW_SCHEMA, key_path, "unknown field");
		}
	}
	return 1;
}

static int validate_codec(struct validation *v, struct jx *codec,
		const char *path)
{
	static const char *const keys[] = {"name", "version", 0};
	char field[256];
	if (!jx_istype(codec, JX_OBJECT))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "codec must be an object");
	if (!allowed_keys(v, codec, path, keys))
		return 0;
	snprintf(field, sizeof(field), "%s.name", path);
	if (!nonempty_string(v, codec, "name", field, 128))
		return 0;
	snprintf(field, sizeof(field), "%s.version", path);
	return nonempty_string(v, codec, "version", field, 64);
}

static int validate_origin(struct validation *v, struct jx *origin,
		const char *path, uint64_t data_id)
{
	if (!jx_istype(origin, JX_OBJECT))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "origin must be an object");
	struct jx *kind = required(v, origin, "kind", JX_STRING, path);
	if (!kind)
		return 0;
	const char *name = kind->u.string_value;
	if (!strcmp(name, "inline")) {
		static const char *const keys[] = {"kind", "base64", 0};
		return allowed_keys(v, origin, path, keys) &&
			   nonempty_string(v, origin, "base64", path, 1048576);
	}
	if (!strcmp(name, "uri")) {
		static const char *const keys[] = {"kind", "uri", 0};
		return allowed_keys(v, origin, path, keys) &&
			   nonempty_string(v, origin, "uri", path, 4096);
	}
	if (!strcmp(name, "object")) {
		static const char *const keys[] = {"kind", "sha256", 0};
		if (!allowed_keys(v, origin, path, keys) ||
				!nonempty_string(v, origin, "sha256", path, 64) ||
				strlen(jx_lookup_string(origin, "sha256")) != 64)
			return 0;
		for (const char *cursor = jx_lookup_string(origin, "sha256");
				*cursor;
				cursor++) {
			if (!((*cursor >= '0' && *cursor <= '9') ||
						(*cursor >= 'a' && *cursor <= 'f')))
				return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "object sha256 must be lowercase hexadecimal");
		}
		return 1;
	}
	if (!strcmp(name, "output")) {
		static const char *const keys[] = {"kind", "task_id", "output_index", 0};
		uint64_t task_id = 0;
		uint64_t output_index = 0;
		if (!allowed_keys(v, origin, path, keys) ||
				!positive_integer(v, origin, "task_id", path, &task_id, 0) ||
				!positive_integer(v, origin, "output_index", path, &output_index, 1))
			return 0;
		(void)data_id;
		return 1;
	}
	return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "unsupported origin kind");
}

static int validate_data_defaults(struct validation *v, struct jx *defaults)
{
	static const char *const keys[] = {"codec", 0};
	if (!defaults)
		return 1;
	if (!jx_istype(defaults, JX_OBJECT) ||
			!allowed_keys(v, defaults, "$.data_defaults", keys))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, "$.data_defaults", "data_defaults must be an object");
	struct jx *codec = jx_lookup(defaults, "codec");
	return !codec || validate_codec(v, codec, "$.data_defaults.codec");
}

static int validate_data(struct validation *v, struct jx *array,
		struct jx *defaults)
{
	static const char *const keys[] = {"data_id", "codec", "origin", "content_sha256", 0};
	uint64_t previous = 0;
	uint64_t index = 0;
	struct jx *item;
	void *iterator = 0;
	while ((item = jx_iterate_array(array, &iterator))) {
		char path[128];
		snprintf(path, sizeof(path), "$.data[%llu]", (unsigned long long)index);
		int compact = vine_datavine_ir_data_compact(item);
		if (compact) {
			if (jx_array_length(item) != 3 ||
					!jx_istype(jx_array_index(item, 0), JX_INTEGER) ||
					!jx_istype(jx_array_index(item, 1), JX_INTEGER) ||
					!jx_istype(jx_array_index(item, 2), JX_INTEGER) ||
					jx_array_index(item, 0)->u.integer_value < 1 ||
					jx_array_index(item, 1)->u.integer_value < 1 ||
					jx_array_index(item, 2)->u.integer_value < 0)
				return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "compact data must be [data_id, task_id, output_index]");
			if (!defaults || !jx_lookup(defaults, "codec"))
				return fail(v, VINE_DATAVINE_WORKFLOW_REQUIRED, path, "compact data requires data_defaults.codec");
		} else if (!jx_istype(item, JX_OBJECT))
			return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "data record must be an object");
		if (!compact && !allowed_keys(v, item, path, keys))
			return 0;
		uint64_t data_id = vine_datavine_ir_data_id(item);
		if (!data_id)
			return 0;
		if (data_id <= previous || (v->delta &&
							   data_id <= v->existing_maximum_data_id))
			return fail(v, VINE_DATAVINE_WORKFLOW_DUPLICATE, path, "data records must have unique ascending data_id values");
		previous = data_id;
		struct jx *inline_codec = compact ? 0 : jx_lookup(item, "codec");
		struct jx *codec = inline_codec ? inline_codec
				   : defaults	? jx_lookup(defaults, "codec")
						: 0;
		struct jx *origin = compact ? 0 : required(v, item, "origin", JX_OBJECT, path);
		if (!codec)
			return fail(v, VINE_DATAVINE_WORKFLOW_REQUIRED, path, "data requires codec or data_defaults.codec");
		if ((!compact && !origin) || (inline_codec && !validate_codec(v, codec, path)) ||
				(!compact &&
						!validate_origin(v, origin, path, data_id)))
			return 0;
		struct jx *hash = compact ? 0 : jx_lookup(item, "content_sha256");
		if (hash) {
			if (!jx_istype(hash, JX_STRING) || strlen(hash->u.string_value) != 64)
				return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "invalid content_sha256");
			for (int i = 0; i < 64; i++) {
				char c = hash->u.string_value[i];
				if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
					return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "invalid content_sha256");
			}
		}
		if (!compact && !strcmp(jx_lookup_string(origin, "kind"), "object") &&
				(!hash || strcmp(hash->u.string_value,
							  jx_lookup_string(origin, "sha256"))))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "object origin must match content_sha256");
		if (!itable_insert(v->data, data_id, item))
			return fail(v, VINE_DATAVINE_WORKFLOW_DUPLICATE, path, "duplicate data_id");
		index++;
		if (index > WORKFLOW_MAX_DATA)
			return fail(v, VINE_DATAVINE_WORKFLOW_LIMIT, "$.data", "too many data records");
	}
	v->data_count = index;
	return 1;
}

static int validate_output_files(struct validation *v, struct jx *files,
		const char *path, int required)
{
	if (!files)
		return required
					   ? fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "executor requires output_files")
					   : 1;
	if (!jx_istype(files, JX_ARRAY) || !files->u.items)
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "output_files must be a nonempty array");
	struct jx *item;
	void *iterator = 0;
	while ((item = jx_iterate_array(files, &iterator))) {
		if (!jx_istype(item, JX_STRING) || !item->u.string_value[0] ||
				strlen(item->u.string_value) > 4096 ||
				item->u.string_value[0] == '/' ||
				strstr(item->u.string_value, "../") ||
				!strcmp(item->u.string_value, ".."))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "output_files must contain safe relative paths");
	}
	return 1;
}

static int validate_executor(struct validation *v, struct jx *executor,
		const char *path)
{
	static const char *const keys[] = {"kind", "version", "payload_ref", "function_ref", "function_digest", "argv", "output_files", "environment", 0};
	if (!allowed_keys(v, executor, path, keys) ||
			!nonempty_string(v, executor, "kind", path, 32) ||
			!nonempty_string(v, executor, "version", path, 64))
		return 0;
	const char *kind = jx_lookup_string(executor, "kind");
	if (!strcmp(kind, "command")) {
		if (strcmp(jx_lookup_string(executor, "version"), VINE_DATAVINE_COMMAND_EXECUTOR_VERSION))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "command executor version must be 1");
		struct jx *argv = required(v, executor, "argv", JX_ARRAY, path);
		if (!argv || !argv->u.items)
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "command argv must not be empty");
		struct jx *arg;
		void *iterator = 0;
		while ((arg = jx_iterate_array(argv, &iterator))) {
			if (!jx_istype(arg, JX_STRING) || strlen(arg->u.string_value) > 1048576)
				return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "command argv must contain bounded strings");
		}
		struct jx *output_files = jx_lookup(executor, "output_files");
		if (!validate_output_files(v, output_files, path, 0))
			return 0;
	} else if (!strcmp(kind, "python") || !strcmp(kind, "taskvine")) {
		const char *version = !strcmp(kind, "python")
							  ? jx_lookup_string(executor, "version")
							  : 0;
		struct jx *output_files = jx_lookup(executor, "output_files");
		if (!strcmp(kind, "taskvine") && output_files)
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "output_files is not valid for taskvine executors");
		if (!strcmp(kind, "python") && strcmp(version, VINE_DATAVINE_PYTHON_CALLABLE_VERSION) &&
				strcmp(version, VINE_DATAVINE_PYTHON_SOURCE_VERSION))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "python executor version must be callable-v1 or source-v1");
		if (!strcmp(kind, "python") &&
				!validate_output_files(v, output_files, path, 1))
			return 0;
		uint64_t payload_ref = 0;
		struct jx *payload = 0;
		int64_t payload_producer = 0;
		if (!positive_integer(v, executor, "payload_ref", path, &payload_ref, 0) ||
				!lookup_data_record(v, payload_ref, &payload, &payload_producer))
			return fail(v, VINE_DATAVINE_WORKFLOW_REFERENCE, path, "executor payload_ref is unknown");
		if (!strcmp(kind, "python")) {
			struct jx *function_ref_value = jx_lookup(executor, "function_ref");
			struct jx *function_digest_value = jx_lookup(executor, "function_digest");
			if (!strcmp(version, VINE_DATAVINE_PYTHON_CALLABLE_VERSION)) {
				uint64_t function_ref = 0;
				int64_t producer = 0;
				if (!positive_integer(v, executor, "function_ref", path, &function_ref, 0) ||
						!lookup_data_record(v, function_ref, 0, &producer) || producer > 0)
					return fail(v, VINE_DATAVINE_WORKFLOW_REFERENCE, path, "callable function_ref must name external or inline data");
				if (!function_digest_value || !jx_istype(function_digest_value, JX_STRING) ||
						strlen(function_digest_value->u.string_value) != 64)
					return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "callable function_digest must be SHA-256 hex");
				for (const char *cursor = function_digest_value->u.string_value; *cursor; cursor++) {
					if (!((*cursor >= '0' && *cursor <= '9') ||
								(*cursor >= 'a' && *cursor <= 'f')))
						return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "callable function_digest must be lowercase SHA-256 hex");
				}
				if (payload_producer > 0)
					return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "callable invocation payload must be external or inline data");
			} else if (function_ref_value || function_digest_value) {
				return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "function registration requires a Python callable executor");
			}
		}
		if (!strcmp(kind, "taskvine")) {
			if (strcmp(jx_lookup_string(executor, "version"), VINE_DATAVINE_TASKVINE_EXECUTOR_VERSION))
				return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "taskvine executor version must be builtin-v1");
			/* An accepted payload was validated in its original transaction. */
			if (!payload)
				return 1;
			struct jx *origin = jx_lookup(payload, "origin");
			if (strcmp(jx_lookup_string(origin, "kind"), "inline"))
				return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "taskvine builtin payload must be inline");
			buffer_t decoded;
			buffer_init(&decoded);
			int decode_status = b64_decode(jx_lookup_string(origin, "base64"), &decoded);
			size_t payload_size = 0;
			const char *bytes = buffer_tolstring(&decoded, &payload_size);
			int valid_payload = decode_status == 0 && payload_size >= 5 &&
						!memcmp(bytes, "DVB1", 4) &&
						(((unsigned char)bytes[4] == 1 && payload_size == 5) ||
								(unsigned char)bytes[4] == 2);
			buffer_free(&decoded);
			if (!valid_payload)
				return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "invalid taskvine builtin payload");
		}
	} else {
		return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "unsupported executor kind");
	}
	struct jx *environment = jx_lookup(executor, "environment");
	if (environment) {
		if (!jx_istype(environment, JX_OBJECT))
			return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "environment must be an object");
		void *iterator = 0;
		struct jx *value;
		while ((value = jx_iterate_values(environment, &iterator))) {
			if (!jx_istype(value, JX_STRING))
				return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "environment values must be strings");
		}
	}
	return 1;
}

static int validate_resources(struct validation *v, struct jx *resources,
		const char *path)
{
	static const char *const keys[] = {"cores", "memory_mb", "disk_mb", "gpus", "wall_time_seconds", 0};
	if (!jx_istype(resources, JX_OBJECT) || !allowed_keys(v, resources, path, keys))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "invalid resources object");
	for (size_t i = 0; keys[i]; i++) {
		struct jx *value = jx_lookup(resources, keys[i]);
		if (value && (!jx_istype(value, JX_INTEGER) ||
						 value->u.integer_value < ((!strcmp(keys[i], "cores") || !strcmp(keys[i], "wall_time_seconds")) ? 1 : 0)))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "invalid resource value");
	}
	return 1;
}

static int validate_string_map(struct validation *v, struct jx *map,
		const char *path)
{
	if (!jx_istype(map, JX_OBJECT))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "field must be an object");
	void *iterator = 0;
	struct jx *value;
	while ((value = jx_iterate_values(map, &iterator))) {
		if (!jx_istype(value, JX_STRING))
			return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "object values must be strings");
	}
	return 1;
}

static int validate_retry(struct validation *v, struct jx *retry,
		const char *path)
{
	static const char *const keys[] = {"maximum_attempts", "retryable_results", 0};
	if (!jx_istype(retry, JX_OBJECT) || !allowed_keys(v, retry, path, keys))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "invalid retry object");
	uint64_t attempts = 0;
	if (!positive_integer(v, retry, "maximum_attempts", path, &attempts, 0))
		return 0;
	struct jx *results = jx_lookup(retry, "retryable_results");
	if (results) {
		if (!jx_istype(results, JX_ARRAY))
			return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "retryable_results must be an array");
		struct jx *item;
		void *iterator = 0;
		while ((item = jx_iterate_array(results, &iterator))) {
			if (!jx_istype(item, JX_STRING) || !item->u.string_value[0] || strlen(item->u.string_value) > 128)
				return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "invalid retryable result");
		}
	}
	return 1;
}

static int validate_task_defaults(struct validation *v, struct jx *defaults)
{
	static const char *const keys[] = {"executor", "resources", "retry", "priority", 0};
	if (!defaults)
		return 1;
	if (!jx_istype(defaults, JX_OBJECT) ||
			!allowed_keys(v, defaults, "$.task_defaults", keys))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, "$.task_defaults", "task_defaults must be an object");
	struct jx *executor = jx_lookup(defaults, "executor");
	struct jx *resources = jx_lookup(defaults, "resources");
	struct jx *retry = jx_lookup(defaults, "retry");
	struct jx *priority = jx_lookup(defaults, "priority");
	return (!executor || (jx_istype(executor, JX_OBJECT) &&
						 validate_executor(v, executor, "$.task_defaults.executor"))) &&
		   (!resources || validate_resources(v, resources, "$.task_defaults.resources")) &&
		   (!retry || validate_retry(v, retry, "$.task_defaults.retry")) &&
		   (!priority || jx_istype(priority, JX_INTEGER));
}

static struct jx *task_setting(struct jx *task, struct jx *defaults,
		const char *name)
{
	struct jx *value = vine_datavine_ir_task_compact(task)
					   ? 0
					   : jx_lookup(task, name);
	return value ? value : defaults ? jx_lookup(defaults, name)
					: 0;
}

static int validate_tasks(struct validation *v, struct jx *array,
		struct jx *defaults)
{
	static const char *const keys[] = {"task_id", "executor", "inputs", "output_data_ids", "resources", "retry", "priority", "labels", 0};
	uint64_t previous = 0;
	uint64_t index = 0;
	struct jx *item;
	void *iterator = 0;
	while ((item = jx_iterate_array(array, &iterator))) {
		char path[128];
		snprintf(path, sizeof(path), "$.tasks[%llu]", (unsigned long long)index);
		int compact = vine_datavine_ir_task_compact(item);
		if (compact) {
			if (jx_array_length(item) != 3 ||
					!jx_istype(jx_array_index(item, 0), JX_INTEGER) ||
					jx_array_index(item, 0)->u.integer_value < 1 ||
					!jx_istype(jx_array_index(item, 1), JX_ARRAY) ||
					!jx_istype(jx_array_index(item, 2), JX_ARRAY))
				return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "compact task must be [task_id, input_data_ids, output_data_ids]");
		} else if (!jx_istype(item, JX_OBJECT))
			return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "task record must be an object");
		if (!compact && !allowed_keys(v, item, path, keys))
			return 0;
		uint64_t task_id = vine_datavine_ir_task_id(item);
		if (!task_id)
			return 0;
		if (task_id <= previous || (v->delta &&
							   task_id <= v->existing_maximum_task_id))
			return fail(v, VINE_DATAVINE_WORKFLOW_DUPLICATE, path, "task records must have unique ascending task_id values");
		previous = task_id;
		struct jx *inline_executor = compact ? 0 : jx_lookup(item, "executor");
		struct jx *executor = task_setting(item, defaults, "executor");
		struct jx *inputs = compact ? vine_datavine_ir_task_inputs(item)
						: required(v, item, "inputs", JX_ARRAY, path);
		struct jx *outputs = compact ? vine_datavine_ir_task_outputs(item)
						 : required(v, item, "output_data_ids", JX_ARRAY, path);
		char executor_path[160];
		snprintf(executor_path, sizeof(executor_path), "%s.executor", path);
		if (!executor)
			return fail(v, VINE_DATAVINE_WORKFLOW_REQUIRED, executor_path, "task requires executor or task_defaults.executor");
		if (!jx_istype(executor, JX_OBJECT) || !inputs || !outputs ||
				(inline_executor && !validate_executor(v, executor, executor_path)))
			return 0;
		uint64_t expected_position = 0;
		struct jx *input;
		void *input_iterator = 0;
		while ((input = jx_iterate_array(inputs, &input_iterator))) {
			if (compact) {
				if (!jx_istype(input, JX_INTEGER) || input->u.integer_value < 1)
					return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "compact inputs must be positive DataIDs");
			} else {
				static const char *const input_keys[] = {"position", "name", "data_id", 0};
				if (!jx_istype(input, JX_OBJECT) || !allowed_keys(v, input, path, input_keys))
					return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "invalid input binding");
				struct jx *position = jx_lookup(input, "position");
				struct jx *name = jx_lookup(input, "name");
				if (!!position == !!name)
					return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "input requires exactly one position or name");
				if (position && (!jx_istype(position, JX_INTEGER) || position->u.integer_value != (int64_t)expected_position++))
					return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "positional inputs must be contiguous and ordered");
				if (name && (!jx_istype(name, JX_STRING) || !name->u.string_value[0] || strlen(name->u.string_value) > 256))
					return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "invalid input name");
			}
			uint64_t data_id = vine_datavine_ir_input_data_id(input);
			struct jx *data = 0;
			int64_t producer = 0;
			if (!data_id || !lookup_data_record(v, data_id, &data, &producer))
				return fail(v, VINE_DATAVINE_WORKFLOW_REFERENCE, path, "input data_id is unknown");
			if (producer > 0) {
				if ((uint64_t)producer >= task_id ||
						((uint64_t)producer > v->existing_maximum_task_id &&
								!itable_lookup(v->tasks, (uint64_t)producer)))
					return fail(v, VINE_DATAVINE_WORKFLOW_REFERENCE, path, "dependency TaskID must precede consumer TaskID");
				v->edge_count++;
			}
		}
		if (!outputs->u.items)
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "task requires at least one output");
		uint64_t output_index = 0;
		struct jx *output;
		void *output_iterator = 0;
		while ((output = jx_iterate_array(outputs, &output_iterator))) {
			if (!jx_istype(output, JX_INTEGER) || output->u.integer_value < 1)
				return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, path, "output_data_ids must contain positive integers");
			uint64_t data_id = (uint64_t)output->u.integer_value;
			struct jx *data = itable_lookup(v->data, data_id);
			if (!data)
				return fail(v, VINE_DATAVINE_WORKFLOW_REFERENCE, path, "output data_id is unknown");
			if (!vine_datavine_ir_data_is_output(data) ||
					(uint64_t)vine_datavine_ir_data_producer(data) != task_id ||
					(uint64_t)vine_datavine_ir_data_output_index(data) != output_index)
				return fail(v, VINE_DATAVINE_WORKFLOW_REFERENCE, path, "output origin does not match task slot");
			if (!itable_insert(v->produced, data_id, item))
				return fail(v, VINE_DATAVINE_WORKFLOW_DUPLICATE, path, "output DataID is produced more than once");
			output_index++;
		}
		struct jx *output_files = jx_lookup(executor, "output_files");
		if ((output_files && jx_array_length(output_files) != (int)output_index) ||
				(!output_files && output_index != 1))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, path, "output_files must align with output_data_ids; plain stdout supports one output");
		struct jx *resources = compact ? 0 : jx_lookup(item, "resources");
		struct jx *retry = compact ? 0 : jx_lookup(item, "retry");
		struct jx *labels = compact ? 0 : jx_lookup(item, "labels");
		struct jx *priority = compact ? 0 : jx_lookup(item, "priority");
		if ((resources && !validate_resources(v, resources, path)) ||
				(retry && !validate_retry(v, retry, path)) ||
				(labels && !validate_string_map(v, labels, path)) ||
				(priority && !jx_istype(priority, JX_INTEGER)))
			return 0;
		if (!itable_insert(v->tasks, task_id, item))
			return fail(v, VINE_DATAVINE_WORKFLOW_DUPLICATE, path, "duplicate task_id");
		index++;
		if (index > v->maximum_tasks || index > WORKFLOW_MAX_TASKS)
			return fail(v, VINE_DATAVINE_WORKFLOW_LIMIT, "$.tasks", "too many task records");
		if (v->edge_count > v->maximum_edges || v->edge_count > WORKFLOW_MAX_EDGES)
			return fail(v, VINE_DATAVINE_WORKFLOW_LIMIT, "$.tasks", "too many dependency edges");
	}
	v->task_count = index;
	UINT64_T data_id;
	void *record;
	int data_iterator;
	ITABLE_ITERATE(v->data, data_iterator, data_id, record)
	{
		if (vine_datavine_ir_data_is_output(record) && !itable_lookup(v->produced, data_id))
			return fail(v, VINE_DATAVINE_WORKFLOW_REFERENCE, "$.data", "output data record has no matching task output");
	}
	return 1;
}

static int validate_requested(struct validation *v, struct jx *array)
{
	uint64_t previous = 0;
	uint64_t count = 0;
	struct jx *item;
	void *iterator = 0;
	while ((item = jx_iterate_array(array, &iterator))) {
		if (!jx_istype(item, JX_INTEGER) || item->u.integer_value < 1)
			return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, "$.requested_outputs", "requested output must be a positive DataID");
		uint64_t data_id = (uint64_t)item->u.integer_value;
		if (data_id <= previous)
			return fail(v, VINE_DATAVINE_WORKFLOW_DUPLICATE, "$.requested_outputs", "requested outputs must be unique and ascending");
		previous = data_id;
		struct jx *data = 0;
		int64_t producer = 0;
		if (!lookup_data_record(v, data_id, &data, &producer) || producer <= 0)
			return fail(v, VINE_DATAVINE_WORKFLOW_REFERENCE, "$.requested_outputs", "requested output is not a task output");
		count++;
	}
	v->requested_count = count;
	return 1;
}

static int validate_document(struct validation *v, struct jx *root,
		struct vine_datavine_workflow_summary *summary)
{
	static const char *const keys[] = {"schema", "workflow_id", "idempotency_key", "mode", "task_defaults", "data_defaults", "tasks", "data", "requested_outputs", "policy", "metadata", "parametric", 0};
	if (!jx_istype(root, JX_OBJECT))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, "$", "workflow must be an object");
	if (!allowed_keys(v, root, "$", keys))
		return 0;
	if (!nonempty_string(v, root, "schema", "$.schema", 64) ||
			strcmp(jx_lookup_string(root, "schema"), VINE_DATAVINE_WORKFLOW_SCHEMA_NAME))
		return fail(v, VINE_DATAVINE_WORKFLOW_SCHEMA, "$.schema", "unsupported workflow schema");
	if (!nonempty_string(v, root, "idempotency_key", "$.idempotency_key", VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX))
		return 0;
	struct jx *workflow_id = jx_lookup(root, "workflow_id");
	if (workflow_id && (!jx_istype(workflow_id, JX_STRING) ||
					   !workflow_id->u.string_value[0] ||
					   strlen(workflow_id->u.string_value) >
							   VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX))
		return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, "$.workflow_id", "invalid workflow_id");
	if (!nonempty_string(v, root, "mode", "$.mode", 16))
		return 0;
	const char *mode = jx_lookup_string(root, "mode");
	if (strcmp(mode, "sealed") && strcmp(mode, "streaming") &&
			strcmp(mode, "staged"))
		return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, "$.mode", "mode must be sealed, streaming, or staged");
	struct jx *tasks = required(v, root, "tasks", JX_ARRAY, "$.tasks");
	struct jx *data = required(v, root, "data", JX_ARRAY, "$.data");
	struct jx *requested = required(v, root, "requested_outputs", JX_ARRAY, "$.requested_outputs");
	struct jx *defaults = jx_lookup(root, "task_defaults");
	struct jx *data_defaults = jx_lookup(root, "data_defaults");
	if (!tasks || !data || !requested || !optional_object(v, root, "policy", "$.policy") ||
			!optional_object(v, root, "metadata", "$.metadata"))
		return 0;
	v->maximum_tasks = WORKFLOW_MAX_TASKS;
	v->maximum_edges = WORKFLOW_MAX_EDGES;
	struct jx *policy = jx_lookup(root, "policy");
	if (policy) {
		static const char *const policy_keys[] = {"maximum_tasks", "maximum_edges", "idata_backup", 0};
		if (!allowed_keys(v, policy, "$.policy", policy_keys))
			return 0;
		struct jx *maximum_tasks = jx_lookup(policy, "maximum_tasks");
		struct jx *maximum_edges = jx_lookup(policy, "maximum_edges");
		struct jx *idata_backup = jx_lookup(policy, "idata_backup");
		if (maximum_tasks && (!jx_istype(maximum_tasks, JX_INTEGER) || maximum_tasks->u.integer_value < 1))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, "$.policy.maximum_tasks", "invalid maximum_tasks");
		if (maximum_edges && (!jx_istype(maximum_edges, JX_INTEGER) || maximum_edges->u.integer_value < 0))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE, "$.policy.maximum_edges", "invalid maximum_edges");
		if (idata_backup && (!jx_istype(idata_backup, JX_STRING) ||
				(strcmp(idata_backup->u.string_value, "worker-local") &&
				 strcmp(idata_backup->u.string_value, "controller-background"))))
			return fail(v, VINE_DATAVINE_WORKFLOW_VALUE,
					"$.policy.idata_backup", "invalid idata_backup");
		if (maximum_tasks)
			v->maximum_tasks = (uint64_t)maximum_tasks->u.integer_value;
		if (maximum_edges)
			v->maximum_edges = (uint64_t)maximum_edges->u.integer_value;
	}
	if (!validate_data_defaults(v, data_defaults) ||
			!validate_data(v, data, data_defaults) ||
			!validate_task_defaults(v, defaults) ||
			!validate_tasks(v, tasks, defaults) || !validate_requested(v, requested))
		return 0;
	struct vine_datavine_parametric *parametric = 0;
	if (vine_datavine_parametric_present(root)) {
		parametric = vine_datavine_parametric_parse(root, v->error);
		if (!parametric)
			return 0;
	}
	if (summary) {
		if (parametric) {
			vine_datavine_parametric_summary(parametric, summary);
		} else {
			summary->tasks = v->task_count;
			summary->data = v->data_count;
			summary->edges = v->edge_count;
			summary->requested_outputs = v->requested_count;
			summary->streaming = strcmp(mode, "sealed") != 0;
		}
	}
	vine_datavine_parametric_delete(parametric);
	return 1;
}

static int validate_delta_document(struct validation *v, struct jx *root,
		struct vine_datavine_workflow_summary *summary)
{
	static const char *const keys[] = {"schema", "workflow_id", "idempotency_key", "task_defaults", "data_defaults", "tasks", "data", "requested_outputs", "metadata", 0};
	if (!jx_istype(root, JX_OBJECT))
		return fail(v, VINE_DATAVINE_WORKFLOW_TYPE, "$", "workflow delta must be an object");
	if (!allowed_keys(v, root, "$", keys) ||
			!nonempty_string(v, root, "schema", "$.schema", 64) ||
			strcmp(jx_lookup_string(root, "schema"),
					VINE_DATAVINE_WORKFLOW_DELTA_SCHEMA_NAME))
		return fail(v, VINE_DATAVINE_WORKFLOW_SCHEMA, "$.schema", "unsupported workflow delta schema");
	if (!nonempty_string(v, root, "workflow_id", "$.workflow_id", VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX) ||
			!nonempty_string(v, root, "idempotency_key", "$.idempotency_key", VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX) ||
			!optional_object(v, root, "metadata", "$.metadata"))
		return 0;
	struct jx *tasks = required(v, root, "tasks", JX_ARRAY, "$.tasks");
	struct jx *data = required(v, root, "data", JX_ARRAY, "$.data");
	struct jx *requested = required(v, root, "requested_outputs", JX_ARRAY, "$.requested_outputs");
	struct jx *defaults = jx_lookup(root, "task_defaults");
	struct jx *data_defaults = jx_lookup(root, "data_defaults");
	if (!tasks || !data || !requested || !tasks->u.items)
		return tasks && data && requested
					   ? fail(v, VINE_DATAVINE_WORKFLOW_VALUE, "$.tasks", "workflow delta must add at least one task")
					   : 0;
	if (!validate_data_defaults(v, data_defaults) ||
			!validate_data(v, data, data_defaults) ||
			!validate_task_defaults(v, defaults) ||
			!validate_tasks(v, tasks, defaults) ||
			!validate_requested(v, requested))
		return 0;
	if (v->existing_tasks + v->task_count > v->maximum_tasks)
		return fail(v, VINE_DATAVINE_WORKFLOW_LIMIT, "$.tasks", "workflow task capacity exceeded");
	if (v->existing_edges + v->edge_count > v->maximum_edges)
		return fail(v, VINE_DATAVINE_WORKFLOW_LIMIT, "$.tasks", "workflow edge capacity exceeded");
	if (summary) {
		summary->tasks = v->task_count;
		summary->data = v->data_count;
		summary->edges = v->edge_count;
		summary->requested_outputs = v->requested_count;
		summary->streaming = 1;
	}
	return 1;
}

static int canonical_digest(struct validation *v, struct jx *root,
		char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1])
{
	char *canonical = jx_canonicalize(root);
	if (!canonical)
		return fail(v, VINE_DATAVINE_WORKFLOW_PARSE, "$", "document is not strict canonical JSON");
	unsigned char binary[SHA1_DIGEST_LENGTH];
	sha1_buffer(canonical, strlen(canonical), binary);
	if (digest)
		snprintf(digest, VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1, "%s", sha1_string(binary));
	free(canonical);
	return 1;
}

static int validate_root(struct jx *root, struct validation *v,
		char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1],
		struct vine_datavine_workflow_summary *summary,
		struct vine_datavine_workflow_error *error)
{
	if (error)
		memset(error, 0, sizeof(*error));
	if (summary)
		memset(summary, 0, sizeof(*summary));
	v->error = error;
	v->tasks = itable_create(0);
	v->data = itable_create(0);
	v->produced = itable_create(0);
	int valid = v->tasks && v->data && v->produced &&
			(v->delta ? validate_delta_document(v, root, summary)
				  : validate_document(v, root, summary));
	if (!v->tasks || !v->data || !v->produced)
		valid = fail(v, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "could not allocate validation indices");
	if (valid)
		valid = canonical_digest(v, root, digest);
	if (v->tasks)
		itable_delete(v->tasks);
	if (v->data)
		itable_delete(v->data);
	if (v->produced)
		itable_delete(v->produced);
	return valid;
}

static int validate_json(const char *json, size_t size,
		struct validation *v, int limits_valid, const char *limit_message,
		char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1],
		struct vine_datavine_workflow_summary *summary,
		struct vine_datavine_workflow_error *error)
{
	if (!json || !size || size > WORKFLOW_MAX_JSON || !limits_valid) {
		v->error = error;
		return fail(v, VINE_DATAVINE_WORKFLOW_LIMIT, "$", limit_message);
	}
	struct jx *root = jx_parse_string_and_length(json, (int)size);
	if (!root) {
		v->error = error;
		return fail(v, VINE_DATAVINE_WORKFLOW_PARSE, "$", "invalid JSON document");
	}
	int valid = validate_root(root, v, digest, summary, error);
	jx_delete(root);
	return valid;
}

int vine_datavine_workflow_validate(const char *json, size_t size,
		char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1],
		struct vine_datavine_workflow_summary *summary,
		struct vine_datavine_workflow_error *error)
{
	struct validation v = {0};
	return validate_json(json, size, &v, 1, "workflow document size is invalid", digest, summary, error);
}

int vine_datavine_workflow_delta_validate_parsed(struct jx *root,
		uint64_t accepted_maximum_task_id,
		uint64_t accepted_maximum_data_id,
		uint64_t maximum_tasks, uint64_t maximum_edges,
		uint64_t accepted_tasks, uint64_t accepted_edges,
		vine_datavine_workflow_data_lookup_t lookup_data, void *lookup_context,
		char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1],
		struct vine_datavine_workflow_summary *summary,
		struct vine_datavine_workflow_error *error)
{
	struct validation v = {
			.maximum_tasks = maximum_tasks,
			.maximum_edges = maximum_edges,
			.existing_maximum_task_id = accepted_maximum_task_id,
			.existing_maximum_data_id = accepted_maximum_data_id,
			.existing_tasks = accepted_tasks,
			.existing_edges = accepted_edges,
			.lookup_data = lookup_data,
			.lookup_context = lookup_context,
			.delta = 1,
	};
	if (!root || !maximum_tasks || accepted_tasks > maximum_tasks || accepted_edges > maximum_edges) {
		v.error = error;
		return fail(&v, VINE_DATAVINE_WORKFLOW_LIMIT, "$", "workflow delta limits are invalid");
	}
	return validate_root(root, &v, digest, summary, error);
}

const char *vine_datavine_workflow_error_name(
		enum vine_datavine_workflow_error_code code)
{
	switch (code) {
	case VINE_DATAVINE_WORKFLOW_VALID:
		return "valid";
	case VINE_DATAVINE_WORKFLOW_PARSE:
		return "parse";
	case VINE_DATAVINE_WORKFLOW_SCHEMA:
		return "schema";
	case VINE_DATAVINE_WORKFLOW_TYPE:
		return "type";
	case VINE_DATAVINE_WORKFLOW_REQUIRED:
		return "required";
	case VINE_DATAVINE_WORKFLOW_VALUE:
		return "value";
	case VINE_DATAVINE_WORKFLOW_DUPLICATE:
		return "duplicate";
	case VINE_DATAVINE_WORKFLOW_REFERENCE:
		return "reference";
	case VINE_DATAVINE_WORKFLOW_LIMIT:
		return "limit";
	}
	return "unknown";
}
