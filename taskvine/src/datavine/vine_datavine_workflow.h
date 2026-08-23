/* DataVine workflow API. */
#ifndef VINE_DATAVINE_WORKFLOW_H
#define VINE_DATAVINE_WORKFLOW_H

#include <stddef.h>
#include <stdint.h>

#define VINE_DATAVINE_WORKFLOW_SCHEMA_NAME "datavine.workflow/v1"
#define VINE_DATAVINE_WORKFLOW_DELTA_SCHEMA_NAME "datavine.workflow-delta/v1"
#define VINE_DATAVINE_COMMAND_EXECUTOR_VERSION "1"
#define VINE_DATAVINE_PYTHON_SOURCE_VERSION "source-v1"
#define VINE_DATAVINE_PYTHON_CALLABLE_VERSION "callable-v1"
#define VINE_DATAVINE_TASKVINE_EXECUTOR_VERSION "builtin-v1"
#define VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH 40
#define VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX 256U
#define VINE_DATAVINE_WORKFLOW_EVENT_BATCH_MAX 4096U

enum vine_datavine_workflow_error_code {
	VINE_DATAVINE_WORKFLOW_VALID = 0,
	VINE_DATAVINE_WORKFLOW_PARSE = 1,
	VINE_DATAVINE_WORKFLOW_SCHEMA = 2,
	VINE_DATAVINE_WORKFLOW_TYPE = 3,
	VINE_DATAVINE_WORKFLOW_REQUIRED = 4,
	VINE_DATAVINE_WORKFLOW_VALUE = 5,
	VINE_DATAVINE_WORKFLOW_DUPLICATE = 6,
	VINE_DATAVINE_WORKFLOW_REFERENCE = 7,
	VINE_DATAVINE_WORKFLOW_LIMIT = 8,
};

struct vine_datavine_workflow_error {
	enum vine_datavine_workflow_error_code code;
	char path[256];
	char message[256];
};

struct vine_datavine_workflow_summary {
	uint64_t tasks;
	uint64_t data;
	uint64_t edges;
	uint64_t requested_outputs;
	int streaming;
};

/*
Validate one complete Workflow IR v1 document and compute its canonical SHA-1
digest. Arrays that identify records must be sorted by ID, making the digest
independent of adaptor insertion order after adaptor normalization.
*/
int vine_datavine_workflow_validate(const char *json, size_t size,
		char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1],
		struct vine_datavine_workflow_summary *summary,
		struct vine_datavine_workflow_error *error);

const char *vine_datavine_workflow_error_name(
		enum vine_datavine_workflow_error_code code);

#endif
