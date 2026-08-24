/* Checked native evaluator for the frozen three-stage data-intensive DAG. */

#include "vine_datavine_parametric.h"

#include "jx.h"
#include "vine_datavine_ir.h"
#include "vine_datavine_scheduler.h"

#include <limits.h>
#include <openssl/sha.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int parametric_fail(struct vine_datavine_workflow_error *error,
		enum vine_datavine_workflow_error_code code, const char *path,
		const char *format, ...)
{
	if (error) {
		error->code = code;
		snprintf(error->path, sizeof(error->path), "%s", path);
		va_list arguments;
		va_start(arguments, format);
		vsnprintf(error->message, sizeof(error->message), format, arguments);
		va_end(arguments);
	}
	return 0;
}

static int checked_multiply(uint64_t left, uint64_t right, uint64_t *result)
{
	if (left && right > UINT64_MAX / left)
		return 0;
	*result = left * right;
	return 1;
}

static int checked_add(uint64_t left, uint64_t right, uint64_t *result)
{
	if (right > UINT64_MAX - left)
		return 0;
	*result = left + right;
	return 1;
}

static int exact_positive(struct jx *object, const char *name,
		uint64_t *value)
{
	struct jx *item = object ? jx_lookup(object, name) : 0;
	if (!item || !jx_istype(item, JX_INTEGER) || item->u.integer_value < 1)
		return 0;
	*value = (uint64_t)item->u.integer_value;
	return 1;
}

static int hexadecimal_sha256(const char *value)
{
	if (!value || strlen(value) != 64)
		return 0;
	for (size_t index = 0; index < 64; index++)
		if (!((value[index] >= '0' && value[index] <= '9') ||
				    (value[index] >= 'a' && value[index] <= 'f')))
			return 0;
	return 1;
}

static int only_keys(struct jx *object, const char *const *allowed)
{
	void *iterator = 0;
	const char *key;
	while ((key = jx_iterate_keys(object, &iterator))) {
		int found = 0;
		for (size_t index = 0; allowed[index]; index++)
			found |= !strcmp(key, allowed[index]);
		if (!found)
			return 0;
	}
	return 1;
}

int vine_datavine_parametric_present(struct jx *root)
{
	return root && jx_lookup(root, "parametric") != 0;
}

struct vine_datavine_parametric *vine_datavine_parametric_parse(
		struct jx *root, struct vine_datavine_workflow_error *error)
{
	static const char *const keys[] = {"kind", "seed", "dataset_root", "cohorts", "scale", "size_profile", "contract_sha256", 0};
	struct jx *value = root ? jx_lookup(root, "parametric") : 0;
	if (!value)
		return 0;
	if (!jx_istype(value, JX_OBJECT) || !only_keys(value, keys)) {
		parametric_fail(error, VINE_DATAVINE_WORKFLOW_SCHEMA, "$.parametric", "parametric declaration has unknown fields or wrong type");
		return 0;
	}
	const char *kind = jx_lookup_string(value, "kind");
	const char *dataset_root = jx_lookup_string(value, "dataset_root");
	const char *profile = jx_lookup_string(value, "size_profile");
	const char *contract = jx_lookup_string(value, "contract_sha256");
	struct jx *seed = jx_lookup(value, "seed");
	uint64_t cohorts = 0;
	uint64_t scale = 0;
	if (!kind || strcmp(kind, VINE_DATAVINE_PARAMETRIC_KIND) || !seed ||
			!jx_istype(seed, JX_INTEGER) ||
			(uint64_t)seed->u.integer_value != VINE_DATAVINE_PARAMETRIC_SEED ||
			!exact_positive(value, "cohorts", &cohorts) ||
			!exact_positive(value, "scale", &scale) || !dataset_root ||
			strncmp(dataset_root, "file:///", 8) || strlen(dataset_root) > 4096 ||
			!profile || (strcmp(profile, "tiny") && strcmp(profile, "full")) ||
			!hexadecimal_sha256(contract)) {
		parametric_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.parametric", "invalid data-intensive-v1 declaration");
		return 0;
	}
	struct jx *mode = jx_lookup(root, "mode");
	struct jx *tasks = jx_lookup(root, "tasks");
	struct jx *data = jx_lookup(root, "data");
	struct jx *requested = jx_lookup(root, "requested_outputs");
	if (!mode || !jx_istype(mode, JX_STRING) || strcmp(mode->u.string_value, "sealed") ||
			!tasks || jx_array_length(tasks) != 0 || !data ||
			jx_array_length(data) != 3 || !requested ||
			jx_array_length(requested) != 0) {
		parametric_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.parametric", "parametric workflows must be sealed with three payload records and no explicit graph records");
		return 0;
	}
	for (int index = 0; index < 3; index++) {
		struct jx *record = jx_array_index(data, index);
		struct jx *origin = record ? vine_datavine_ir_data_origin(record) : 0;
		const char *origin_kind = origin ? jx_lookup_string(origin, "kind") : 0;
		if (vine_datavine_ir_data_id(record) != (uint64_t)index + 1 ||
				!origin_kind || strcmp(origin_kind, "inline")) {
			parametric_fail(error, VINE_DATAVINE_WORKFLOW_VALUE, "$.data", "DataIDs 1..3 must be inline A/B/C source payloads");
			return 0;
		}
	}
	struct vine_datavine_parametric *family = calloc(1, sizeof(*family));
	if (!family) {
		parametric_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$.parametric", "could not allocate parametric descriptor");
		return 0;
	}
	family->cohorts = cohorts;
	family->scale = scale;
	int valid = checked_multiply(64, scale, &family->a_per_cohort) &&
		    checked_multiply(160, scale, &family->b_per_cohort) &&
		    checked_multiply(32, scale, &family->c_per_cohort) &&
		    checked_multiply(cohorts, family->a_per_cohort, &family->a_tasks) &&
		    checked_multiply(cohorts, family->b_per_cohort, &family->b_tasks) &&
		    checked_multiply(cohorts, family->c_per_cohort, &family->c_tasks) &&
		    checked_add(family->a_tasks, family->b_tasks, &family->tasks) &&
		    checked_add(family->tasks, family->c_tasks, &family->tasks) &&
		    checked_multiply(family->a_tasks,
				    VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS,
				    &family->source_files) &&
		    checked_add(family->source_files, family->tasks, &family->workflow_files) &&
		    checked_add(family->workflow_files, 3, &family->data_records) &&
		    checked_multiply(family->b_tasks, VINE_DATAVINE_PARAMETRIC_B_INPUTS, &family->scheduler_edges);
	uint64_t c_edges = 0;
	valid = valid && checked_multiply(family->c_tasks, VINE_DATAVINE_PARAMETRIC_C_INPUTS, &c_edges) &&
		checked_add(family->scheduler_edges, c_edges, &family->scheduler_edges);
	uint64_t a_span = 0;
	valid = valid && checked_multiply(family->a_tasks, VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS + 1, &a_span) &&
		checked_add(4, a_span, &family->b_data_first) &&
		checked_add(family->b_data_first, family->b_tasks, &family->c_data_first) && family->tasks <= INT64_MAX &&
		family->data_records <= INT64_MAX && family->tasks <= UINT64_C(10000000) &&
		family->data_records <= UINT64_C(20000000) &&
		family->scheduler_edges <= UINT64_C(100000000);
	struct jx *policy = jx_lookup(root, "policy");
	valid = valid && policy &&
		(uint64_t)jx_lookup_integer(policy, "maximum_tasks") == family->tasks &&
		(uint64_t)jx_lookup_integer(policy, "maximum_edges") == family->scheduler_edges;
	if (!valid) {
		vine_datavine_parametric_delete(family);
		parametric_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$.parametric", "expanded parametric counts overflow or disagree with policy");
		return 0;
	}
	family->dataset_root = strdup(dataset_root);
	if (!family->dataset_root) {
		vine_datavine_parametric_delete(family);
		parametric_fail(error, VINE_DATAVINE_WORKFLOW_LIMIT, "$.parametric", "could not retain dataset root");
		return 0;
	}
	size_t root_size = strlen(family->dataset_root);
	while (root_size > 8 && family->dataset_root[root_size - 1] == '/')
		family->dataset_root[--root_size] = 0;
	snprintf(family->size_profile, sizeof(family->size_profile), "%s", profile);
	snprintf(family->contract_sha256, sizeof(family->contract_sha256), "%s", contract);
	return family;
}

void vine_datavine_parametric_delete(struct vine_datavine_parametric *family)
{
	if (!family)
		return;
	free(family->dataset_root);
	free(family);
}

void vine_datavine_parametric_summary(
		const struct vine_datavine_parametric *family,
		struct vine_datavine_workflow_summary *summary)
{
	if (!family || !summary)
		return;
	summary->tasks = family->tasks;
	summary->data = family->data_records;
	summary->edges = family->scheduler_edges;
	summary->requested_outputs = family->c_tasks;
	summary->streaming = 0;
}

static uint64_t gcd_u64(uint64_t left, uint64_t right)
{
	while (right) {
		uint64_t remainder = left % right;
		left = right;
		right = remainder;
	}
	return left;
}

static uint64_t stable_u64(const char *format, ...)
{
	char payload[160];
	va_list arguments;
	va_start(arguments, format);
	int length = vsnprintf(payload, sizeof(payload), format, arguments);
	va_end(arguments);
	if (length < 0 || (size_t)length >= sizeof(payload))
		return 0;
	unsigned char digest[SHA256_DIGEST_LENGTH];
	SHA256((const unsigned char *)payload, (size_t)length, digest);
	uint64_t value = 0;
	for (int index = 0; index < 8; index++)
		value = (value << 8) | digest[index];
	return value;
}

static uint64_t a_data_id(const struct vine_datavine_parametric *family,
		uint64_t a_global)
{
	(void)family;
	return 4 + a_global * (VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS + 1) +
	       VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS;
}

int vine_datavine_parametric_task(
		const struct vine_datavine_parametric *family, uint64_t task_id,
		enum vine_datavine_parametric_stage *stage, uint64_t *inputs,
		size_t capacity, size_t *input_count, uint64_t *output_data_id)
{
	if (!family || !stage || !inputs || !input_count || !output_data_id ||
			task_id < 1 || task_id > family->tasks)
		return 0;
	uint64_t local = task_id - 1;
	if (local < family->a_tasks) {
		if (capacity < VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS)
			return 0;
		*stage = VINE_DATAVINE_PARAMETRIC_A;
		*input_count = VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS;
		uint64_t first = 4 + local *
						     (VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS + 1);
		for (size_t index = 0; index < *input_count; index++)
			inputs[index] = first + index;
		*output_data_id = first + VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS;
		return 1;
	}
	local -= family->a_tasks;
	if (local < family->b_tasks) {
		if (capacity < VINE_DATAVINE_PARAMETRIC_B_INPUTS)
			return 0;
		*stage = VINE_DATAVINE_PARAMETRIC_B;
		*input_count = VINE_DATAVINE_PARAMETRIC_B_INPUTS;
		uint64_t cohort = local / family->b_per_cohort;
		uint64_t b_local = local % family->b_per_cohort;
		uint64_t groups = family->a_per_cohort /
				  VINE_DATAVINE_PARAMETRIC_B_INPUTS;
		uint64_t round = b_local / groups;
		uint64_t group = b_local % groups;
		uint64_t multiplier = stable_u64("%llu:a:%llu:%llu:m",
						      (unsigned long long)VINE_DATAVINE_PARAMETRIC_SEED,
						      (unsigned long long)cohort,
						      (unsigned long long)round) %
						      family->a_per_cohort |
				      1;
		uint64_t offset = stable_u64("%llu:a:%llu:%llu:o",
						  (unsigned long long)VINE_DATAVINE_PARAMETRIC_SEED,
						  (unsigned long long)cohort,
						  (unsigned long long)round) %
				  family->a_per_cohort;
		for (size_t index = 0; index < *input_count; index++) {
			uint64_t position = group * VINE_DATAVINE_PARAMETRIC_B_INPUTS + index;
			uint64_t a_local = (multiplier * position + offset) %
					   family->a_per_cohort;
			inputs[index] = a_data_id(family,
					cohort * family->a_per_cohort + a_local);
		}
		*output_data_id = family->b_data_first + local;
		return 1;
	}
	local -= family->b_tasks;
	if (capacity < VINE_DATAVINE_PARAMETRIC_C_INPUTS)
		return 0;
	*stage = VINE_DATAVINE_PARAMETRIC_C;
	*input_count = VINE_DATAVINE_PARAMETRIC_C_INPUTS;
	uint64_t cohort = local / family->c_per_cohort;
	uint64_t c_local = local % family->c_per_cohort;
	uint64_t multiplier = stable_u64("%llu:b:%llu:m",
					      (unsigned long long)VINE_DATAVINE_PARAMETRIC_SEED,
					      (unsigned long long)cohort) %
			      family->b_per_cohort;
	while (gcd_u64(multiplier, family->b_per_cohort) != 1)
		multiplier = (multiplier + 1) % family->b_per_cohort;
	uint64_t offset = stable_u64("%llu:b:%llu:o",
					  (unsigned long long)VINE_DATAVINE_PARAMETRIC_SEED,
					  (unsigned long long)cohort) %
			  family->b_per_cohort;
	for (size_t index = 0; index < *input_count; index++)
		inputs[index] = family->b_data_first + cohort * family->b_per_cohort +
				(multiplier * (c_local * VINE_DATAVINE_PARAMETRIC_C_INPUTS + index) +
						offset) %
						family->b_per_cohort;
	*output_data_id = family->c_data_first + local;
	return 1;
}

int vine_datavine_parametric_output_producer(
		const struct vine_datavine_parametric *family, uint64_t data_id,
		uint64_t *task_id)
{
	if (!family || !task_id)
		return 0;
	if (data_id >= 4 + VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS &&
			data_id < family->b_data_first) {
		uint64_t relative = data_id - 4;
		uint64_t a_global = relative /
				    (VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS + 1);
		if (a_global < family->a_tasks && relative %
										  (VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS + 1) ==
								  VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS) {
			*task_id = a_global + 1;
			return 1;
		}
	}
	if (data_id >= family->b_data_first && data_id < family->c_data_first) {
		*task_id = family->a_tasks + (data_id - family->b_data_first) + 1;
		return 1;
	}
	if (data_id >= family->c_data_first &&
			data_id < family->c_data_first + family->c_tasks) {
		*task_id = family->a_tasks + family->b_tasks +
			   (data_id - family->c_data_first) + 1;
		return 1;
	}
	return 0;
}

uint32_t vine_datavine_parametric_output_consumers(
		const struct vine_datavine_parametric *family, uint64_t data_id)
{
	uint64_t producer = 0;
	if (!vine_datavine_parametric_output_producer(family, data_id, &producer))
		return 0;
	if (producer <= family->a_tasks)
		return VINE_DATAVINE_PARAMETRIC_B_REUSE;
	if (producer <= family->a_tasks + family->b_tasks)
		return 1;
	return 0;
}

int vine_datavine_parametric_requested(
		const struct vine_datavine_parametric *family, uint64_t data_id)
{
	return family && data_id >= family->c_data_first &&
	       data_id < family->c_data_first + family->c_tasks;
}

char *vine_datavine_parametric_source_uri(
		const struct vine_datavine_parametric *family, uint64_t data_id)
{
	if (!family || data_id < 4 || data_id >= family->b_data_first)
		return 0;
	uint64_t relative = data_id - 4;
	uint64_t a_global = relative /
			    (VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS + 1);
	uint64_t slot = relative % (VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS + 1);
	if (a_global >= family->a_tasks ||
			slot >= VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS)
		return 0;
	uint64_t cohort = a_global / family->a_per_cohort;
	uint64_t a_local = a_global % family->a_per_cohort;
	size_t capacity = strlen(family->dataset_root) + 96;
	char *uri = malloc(capacity);
	if (uri)
		snprintf(uri, capacity, "%s/sources/c%02llu/a%04llu/s%02llu.bin", family->dataset_root, (unsigned long long)cohort, (unsigned long long)a_local, (unsigned long long)slot);
	return uri;
}

static void put_little_u64(unsigned char bytes[8], uint64_t value)
{
	for (size_t index = 0; index < 8; index++) {
		bytes[index] = (unsigned char)value;
		value >>= 8;
	}
}

struct vine_datavine_scheduler *vine_datavine_parametric_scheduler_create(
		const struct vine_datavine_parametric *family)
{
	if (!family)
		return 0;
	struct vine_datavine_scheduler *scheduler = vine_datavine_scheduler_create(
			(int64_t)family->tasks, family->tasks, family->scheduler_edges);
	if (!scheduler)
		return 0;
	uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
	unsigned char parents[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS * 8];
	for (uint64_t task_id = 1; task_id <= family->tasks; task_id++) {
		enum vine_datavine_parametric_stage stage;
		size_t input_count = 0;
		uint64_t output = 0;
		int valid = vine_datavine_parametric_task(family, task_id, &stage, inputs, VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS, &input_count, &output);
		size_t parent_count = stage == VINE_DATAVINE_PARAMETRIC_A ? 0 : input_count;
		for (size_t index = 0; valid && index < parent_count; index++) {
			uint64_t producer = 0;
			valid = vine_datavine_parametric_output_producer(family,
						inputs[index],
						&producer) &&
				producer < task_id;
			if (valid)
				put_little_u64(parents + index * 8, producer);
		}
		if (!valid || !vine_datavine_scheduler_add_task(scheduler,
					      (int64_t)task_id,
					      (const char *)parents,
					      parent_count * 8)) {
			vine_datavine_scheduler_delete(scheduler);
			return 0;
		}
	}
	if (!vine_datavine_scheduler_seal(scheduler)) {
		vine_datavine_scheduler_delete(scheduler);
		return 0;
	}
	return scheduler;
}
