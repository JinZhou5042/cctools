/* Compact/full Workflow IR record accessors. */

#ifndef VINE_DATAVINE_IR_H
#define VINE_DATAVINE_IR_H

#include "jx.h"

#include <stdint.h>

int vine_datavine_ir_task_compact(struct jx *task);
uint64_t vine_datavine_ir_task_id(struct jx *task);
struct jx *vine_datavine_ir_task_inputs(struct jx *task);
struct jx *vine_datavine_ir_task_outputs(struct jx *task);
uint64_t vine_datavine_ir_input_data_id(struct jx *input);

int vine_datavine_ir_data_compact(struct jx *record);
uint64_t vine_datavine_ir_data_id(struct jx *record);
int vine_datavine_ir_data_is_output(struct jx *record);
int64_t vine_datavine_ir_data_producer(struct jx *record);
int32_t vine_datavine_ir_data_output_index(struct jx *record);
struct jx *vine_datavine_ir_data_origin(struct jx *record);
struct jx *vine_datavine_ir_data_codec(struct jx *record,
		struct jx *default_codec);

#endif
