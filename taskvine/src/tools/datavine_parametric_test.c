/* Native parametric evaluator/profile harness used by DataVine regression. */

#include "vine_datavine_parametric.h"
#include "vine_datavine_scheduler.h"

#include "copy_stream.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"

#include <openssl/evp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>

static uint64_t elapsed_nanoseconds(
		const struct timespec *started, const struct timespec *finished)
{
	return (uint64_t)((finished->tv_sec - started->tv_sec) * INT64_C(1000000000) +
			  finished->tv_nsec - started->tv_nsec);
}

static int read_document(const char *path, char **document, size_t *size)
{
	return (!strcmp(path, "-")
					       ? copy_stream_to_buffer(stdin, document, size)
					       : copy_file_to_buffer(path, document, size)) >= 0;
}

int main(int argc, char **argv)
{
	if (argc < 3 || argc > 4 ||
			(strcmp(argv[1], "profile") && strcmp(argv[1], "task") &&
					strcmp(argv[1], "digest"))) {
		fprintf(stderr, "usage: %s profile DOCUMENT\n"
				"       %s digest DOCUMENT\n"
				"       %s task DOCUMENT TASK_ID\n",
				argv[0],
				argv[0],
				argv[0]);
		return 2;
	}
	char *document = 0;
	size_t document_size = 0;
	if (!read_document(argv[2], &document, &document_size))
		return 2;
	struct vine_datavine_workflow_summary summary;
	struct vine_datavine_workflow_error error;
	char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1];
	int valid = vine_datavine_workflow_validate(document, document_size, digest, &summary, &error);
	struct jx *root = valid
					  ? jx_parse_string_and_length(document, (int)document_size)
					  : 0;
	struct vine_datavine_parametric *family = root
								  ? vine_datavine_parametric_parse(root, &error)
								  : 0;
	free(document);
	if (!valid || !root || !family) {
		fprintf(stderr, "invalid parametric workflow: %s %s\n", error.path, error.message);
		if (root)
			jx_delete(root);
		return 1;
	}
	struct jx *result = 0;
	if (!strcmp(argv[1], "task")) {
		if (argc != 4) {
			vine_datavine_parametric_delete(family);
			jx_delete(root);
			return 2;
		}
		char *end = 0;
		uint64_t task_id = strtoull(argv[3], &end, 10);
		uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
		size_t input_count = 0;
		uint64_t output = 0;
		enum vine_datavine_parametric_stage stage;
		valid = end && !*end && vine_datavine_parametric_task(family, task_id, &stage, inputs, VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS, &input_count, &output);
		struct jx *input_array = jx_array(0);
		for (size_t index = 0; valid && index < input_count; index++)
			jx_array_append(input_array, jx_integer((jx_int_t)inputs[index]));
		char *source_uri = valid && stage == VINE_DATAVINE_PARAMETRIC_A
						   ? vine_datavine_parametric_source_uri(family, inputs[0])
						   : 0;
		result = valid ? jx_objectv("task_id", jx_integer((jx_int_t)task_id), "stage", jx_integer(stage), "inputs", input_array, "output", jx_integer((jx_int_t)output), "requested", jx_boolean(vine_datavine_parametric_requested(family, output)), "first_source_uri", source_uri ? jx_string(source_uri) : jx_null(), NULL) : 0;
		free(source_uri);
		if (!result)
			jx_delete(input_array);
	} else if (!strcmp(argv[1], "profile")) {
		struct timespec started;
		struct timespec finished;
		clock_gettime(CLOCK_MONOTONIC, &started);
		struct vine_datavine_scheduler *scheduler =
				vine_datavine_parametric_scheduler_create(family);
		clock_gettime(CLOCK_MONOTONIC, &finished);
		struct rusage usage;
		getrusage(RUSAGE_SELF, &usage);
		int64_t first_ready = scheduler
						      ? vine_datavine_scheduler_take(scheduler)
						      : 0;
		result = scheduler ? jx_objectv("status", jx_string("PASS"), "tasks", jx_integer((jx_int_t)family->tasks), "data", jx_integer((jx_int_t)family->data_records), "edges", jx_integer((jx_int_t)family->scheduler_edges), "first_ready", jx_integer(first_ready), "build_nanoseconds", jx_integer((jx_int_t)elapsed_nanoseconds(&started, &finished)), "max_rss_kib", jx_integer((jx_int_t)usage.ru_maxrss), NULL) : 0;
		vine_datavine_scheduler_delete(scheduler);
	} else {
		EVP_MD_CTX *context = EVP_MD_CTX_new();
		valid = context && EVP_DigestInit_ex(context, EVP_sha256(), 0) == 1;
		uint64_t inputs[VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS];
		for (uint64_t task_id = 1; valid && task_id <= family->tasks; task_id++) {
			size_t input_count = 0;
			uint64_t output = 0;
			enum vine_datavine_parametric_stage stage;
			valid = vine_datavine_parametric_task(family, task_id, &stage, inputs, VINE_DATAVINE_PARAMETRIC_SOURCE_INPUTS, &input_count, &output);
			unsigned char encoded[17];
			for (int byte = 0; byte < 8; byte++) {
				encoded[byte] = (unsigned char)(task_id >> (56 - byte * 8));
				encoded[9 + byte] = (unsigned char)(input_count >> (56 - byte * 8));
			}
			encoded[8] = (unsigned char)stage;
			valid = valid && EVP_DigestUpdate(context, encoded, sizeof(encoded)) == 1;
			for (size_t index = 0; valid && index < input_count; index++) {
				unsigned char value[8];
				for (int byte = 0; byte < 8; byte++)
					value[byte] = (unsigned char)(inputs[index] >> (56 - byte * 8));
				valid = EVP_DigestUpdate(context, value, sizeof(value)) == 1;
			}
			unsigned char value[8];
			for (int byte = 0; byte < 8; byte++)
				value[byte] = (unsigned char)(output >> (56 - byte * 8));
			valid = valid && EVP_DigestUpdate(context, value, sizeof(value)) == 1;
		}
		unsigned char digest_bytes[EVP_MAX_MD_SIZE];
		unsigned int digest_size = 0;
		valid = valid && EVP_DigestFinal_ex(context, digest_bytes, &digest_size) == 1 && digest_size == 32;
		EVP_MD_CTX_free(context);
		char encoded_digest[65];
		static const char hex[] = "0123456789abcdef";
		for (size_t index = 0; valid && index < 32; index++) {
			encoded_digest[index * 2] = hex[digest_bytes[index] >> 4];
			encoded_digest[index * 2 + 1] = hex[digest_bytes[index] & 15];
		}
		encoded_digest[64] = 0;
		result = valid ? jx_objectv("status", jx_string("PASS"), "tasks", jx_integer((jx_int_t)family->tasks), "topology_sha256", jx_string(encoded_digest), NULL) : 0;
	}
	if (result) {
		jx_print_stream(result, stdout);
		printf("\n");
		jx_delete(result);
	}
	vine_datavine_parametric_delete(family);
	jx_delete(root);
	return result ? 0 : 1;
}
