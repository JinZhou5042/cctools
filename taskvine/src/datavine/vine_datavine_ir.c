/* Compact/full Workflow IR record accessors. */

#include "vine_datavine_ir.h"

#include <string.h>

int vine_datavine_ir_task_compact(struct jx *task)
{
	return task && jx_istype(task, JX_ARRAY);
}

uint64_t vine_datavine_ir_task_id(struct jx *task)
{
	struct jx *value = vine_datavine_ir_task_compact(task)
					   ? jx_array_index(task, 0)
					   : jx_lookup(task, "task_id");
	return value && jx_istype(value, JX_INTEGER)
				   ? (uint64_t)value->u.integer_value
				   : 0;
}

struct jx *vine_datavine_ir_task_inputs(struct jx *task)
{
	return vine_datavine_ir_task_compact(task)
				   ? jx_array_index(task, 1)
				   : jx_lookup(task, "inputs");
}

struct jx *vine_datavine_ir_task_outputs(struct jx *task)
{
	return vine_datavine_ir_task_compact(task)
				   ? jx_array_index(task, 2)
				   : jx_lookup(task, "output_data_ids");
}

uint64_t vine_datavine_ir_input_data_id(struct jx *input)
{
	struct jx *value = jx_istype(input, JX_INTEGER)
					   ? input
					   : jx_lookup(input, "data_id");
	return value && jx_istype(value, JX_INTEGER)
				   ? (uint64_t)value->u.integer_value
				   : 0;
}

int vine_datavine_ir_data_compact(struct jx *record)
{
	return record && jx_istype(record, JX_ARRAY);
}

uint64_t vine_datavine_ir_data_id(struct jx *record)
{
	struct jx *value = vine_datavine_ir_data_compact(record)
					   ? jx_array_index(record, 0)
					   : jx_lookup(record, "data_id");
	return value && jx_istype(value, JX_INTEGER)
				   ? (uint64_t)value->u.integer_value
				   : 0;
}

int vine_datavine_ir_data_is_output(struct jx *record)
{
	if (!record)
		return 0;
	if (vine_datavine_ir_data_compact(record))
		return 1;
	struct jx *origin = jx_lookup(record, "origin");
	const char *kind = origin ? jx_lookup_string(origin, "kind") : 0;
	return kind && !strcmp(kind, "output");
}

int64_t vine_datavine_ir_data_producer(struct jx *record)
{
	if (!vine_datavine_ir_data_is_output(record))
		return 0;
	struct jx *value = vine_datavine_ir_data_compact(record)
					   ? jx_array_index(record, 1)
					   : jx_lookup(jx_lookup(record, "origin"), "task_id");
	return value && jx_istype(value, JX_INTEGER)
				   ? value->u.integer_value
				   : 0;
}

int32_t vine_datavine_ir_data_output_index(struct jx *record)
{
	if (!vine_datavine_ir_data_is_output(record))
		return -1;
	struct jx *value = vine_datavine_ir_data_compact(record)
					   ? jx_array_index(record, 2)
					   : jx_lookup(jx_lookup(record, "origin"), "output_index");
	return value && jx_istype(value, JX_INTEGER)
				   ? (int32_t)value->u.integer_value
				   : -1;
}

struct jx *vine_datavine_ir_data_origin(struct jx *record)
{
	return vine_datavine_ir_data_compact(record)
				   ? 0
				   : jx_lookup(record, "origin");
}

struct jx *vine_datavine_ir_data_codec(struct jx *record,
		struct jx *default_codec)
{
	struct jx *codec = vine_datavine_ir_data_compact(record)
					   ? 0
					   : jx_lookup(record, "codec");
	return codec ? codec : default_codec;
}
