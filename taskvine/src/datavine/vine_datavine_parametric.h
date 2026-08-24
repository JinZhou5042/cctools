/* Checked native evaluator for DataVine parametric workflow families. */
#ifndef VINE_DATAVINE_PARAMETRIC_H
#define VINE_DATAVINE_PARAMETRIC_H

#include "vine_datavine_workflow.h"

#include <stddef.h>
#include <stdint.h>

struct jx;
struct vine_datavine_scheduler;

#define VINE_DATAVINE_PARAMETRIC_KIND "data-intensive-v1"
#define VINE_DATAVINE_PARAMETRIC_SEED UINT64_C(20260823)
#define VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS 36U
#define VINE_DATAVINE_PARAMETRIC_B_INPUTS 8U
#define VINE_DATAVINE_PARAMETRIC_C_INPUTS 5U
#define VINE_DATAVINE_PARAMETRIC_B_REUSE 20U

enum vine_datavine_parametric_stage {
	VINE_DATAVINE_PARAMETRIC_A = 1,
	VINE_DATAVINE_PARAMETRIC_B = 2,
	VINE_DATAVINE_PARAMETRIC_C = 3,
};

struct vine_datavine_parametric {
	uint64_t cohorts;
	uint64_t scale;
	uint64_t a_per_cohort;
	uint64_t b_per_cohort;
	uint64_t c_per_cohort;
	uint64_t a_tasks;
	uint64_t b_tasks;
	uint64_t c_tasks;
	uint64_t tasks;
	uint64_t source_files;
	uint64_t workflow_files;
	uint64_t data_records;
	uint64_t scheduler_edges;
	uint64_t b_data_first;
	uint64_t c_data_first;
	char *dataset_root;
	char size_profile[8];
	char contract_sha256[65];
};

/* Returns zero when the optional declaration is absent. */
int vine_datavine_parametric_present(struct jx *root);

/* Parse and validate without touching the dataset or mutating runtime state. */
struct vine_datavine_parametric *vine_datavine_parametric_parse(
		struct jx *root, struct vine_datavine_workflow_error *error);
void vine_datavine_parametric_delete(struct vine_datavine_parametric *family);

void vine_datavine_parametric_summary(
		const struct vine_datavine_parametric *family,
		struct vine_datavine_workflow_summary *summary);

int vine_datavine_parametric_task(
		const struct vine_datavine_parametric *family, uint64_t task_id,
		enum vine_datavine_parametric_stage *stage, uint64_t *inputs,
		size_t capacity, size_t *input_count, uint64_t *output_data_id);
int vine_datavine_parametric_output_producer(
		const struct vine_datavine_parametric *family, uint64_t data_id,
		uint64_t *task_id);
uint32_t vine_datavine_parametric_output_consumers(
		const struct vine_datavine_parametric *family, uint64_t data_id);
int vine_datavine_parametric_requested(
		const struct vine_datavine_parametric *family, uint64_t data_id);
char *vine_datavine_parametric_source_uri(
		const struct vine_datavine_parametric *family, uint64_t data_id);

/* Build the expanded logical scheduler directly in packed native storage. */
struct vine_datavine_scheduler *vine_datavine_parametric_scheduler_create(
		const struct vine_datavine_parametric *family);

#endif
