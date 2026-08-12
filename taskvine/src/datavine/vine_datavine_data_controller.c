/* Concurrent file-backed DataVine data controller. */

#include "vine_datavine_data_controller.h"

#include "create_dir.h"
#include "hash_table.h"
#include "itable.h"
#include "jx.h"
#include "taskvine.h"
#include "vine_datavine_journal.h"
#include "vine_datavine_protocol.h"
#include "vine_datavine_workflow_store.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define DATA_READY_BATCH 1
#define DATA_RELEASE_BATCH 2
#define DATA_QUEUE_LIMIT 4096
#define DATA_COMMIT_COALESCE_NS 1000000L

struct data_result {
	struct vine_datavine_workflow_result_info info;
	char *path;
	struct vine_file *remote_file;
	int durable;
};

struct publication_output {
	struct vine_datavine_workflow_result_info info;
	char *path;
	struct vine_file *remote_file;
	int remote_only;
	char *plain_data;
	size_t plain_size;
};

struct vine_datavine_data_publication {
	pthread_mutex_t lock;
	pthread_cond_t changed;
	int done;
	int successful;
	struct vine_datavine_data_publication_metrics metrics;
};

struct publication_job {
	char *workflow_id;
	int64_t task_id;
	uint32_t attempt;
	struct publication_output *outputs;
	size_t count;
	struct vine_datavine_data_publication *publication;
	struct timespec queued_at;
	uint64_t decode_nanoseconds;
	uint64_t function_nanoseconds;
	uint64_t serialize_nanoseconds;
	uint64_t fsync_nanoseconds;
	struct vine_datavine_data_publication_metrics metrics;
	struct timespec commit_started;
	struct publication_output *durable;
	size_t durable_count;
	struct publication_job *next;
};

struct result_installation {
	struct data_result **pending;
	char **keys;
	size_t count;
};

struct vine_datavine_data_controller {
	pthread_mutex_t lock;
	pthread_mutex_t queue_lock;
	pthread_cond_t queued;
	pthread_cond_t space;
	pthread_cond_t prepared;
	struct hash_table *results;
	struct vine_datavine_journal *journal;
	char *root;
	pthread_t *threads;
	size_t thread_count;
	size_t queued_count;
	struct publication_job *head;
	struct publication_job *tail;
	struct publication_job *prepared_head;
	struct publication_job *prepared_tail;
	int commit_active;
	int stopping;
	int lock_initialized;
	int queue_lock_initialized;
	int queued_initialized;
	int space_initialized;
	int prepared_initialized;
};

static uint64_t elapsed_nanoseconds(
		const struct timespec *started, const struct timespec *finished)
{
	return (uint64_t)((finished->tv_sec - started->tv_sec) * INT64_C(1000000000) +
			  finished->tv_nsec - started->tv_nsec);
}

static void data_result_delete(void *value)
{
	struct data_result *result = value;
	if (result) {
		free(result->path);
		free(result);
	}
}

static char *result_key(const char *workflow_id, uint64_t data_id)
{
	size_t size = strlen(workflow_id) + 32;
	char *key = malloc(size);
	if (key)
		snprintf(key, size, "%s\037%llu", workflow_id, (unsigned long long)data_id);
	return key;
}

static int workflow_directory(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, char path[PATH_MAX])
{
	unsigned char digest[SHA256_DIGEST_LENGTH];
	char encoded[SHA256_DIGEST_LENGTH * 2 + 1];
	SHA256((const unsigned char *)workflow_id, strlen(workflow_id), digest);
	for (size_t index = 0; index < sizeof(digest); index++)
		snprintf(encoded + index * 2, 3, "%02x", digest[index]);
	if (snprintf(path, PATH_MAX, "%s/%s", controller->root, encoded) >=
			PATH_MAX)
		return 0;
	return create_dir(path, 0700);
}

static int output_path(struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id, uint32_t attempt,
		char path[PATH_MAX])
{
	char directory[PATH_MAX];
	if (!workflow_directory(controller, workflow_id, directory))
		return 0;
	return snprintf(path, PATH_MAX, "%s/%llu.%u.data", directory, (unsigned long long)data_id, attempt) < PATH_MAX;
}

static int output_path_in_directory(const char *directory, uint64_t data_id,
		uint32_t attempt, char path[PATH_MAX])
{
	return snprintf(path, PATH_MAX, "%s/%llu.%u.data", directory, (unsigned long long)data_id, attempt) < PATH_MAX;
}

static int write_all(int fd, const char *data, size_t size)
{
	while (size) {
		ssize_t written = write(fd, data, size);
		if (written > 0) {
			data += written;
			size -= (size_t)written;
		} else {
			return 0;
		}
	}
	return 1;
}

static int write_plain_output(struct publication_output *output)
{
	if (!output->plain_data)
		return 1;
	int fd = open(output->path,
			O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
			0600);
	int valid = fd >= 0 &&
		    write_all(fd, output->plain_data, output->plain_size) &&
		    fsync(fd) == 0;
	if (fd >= 0 && close(fd))
		valid = 0;
	return valid;
}

static int hash_file(const char *path, uint64_t *size, unsigned char digest[32])
{
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	EVP_MD_CTX *context = fd >= 0 ? EVP_MD_CTX_new() : 0;
	int valid = context && EVP_DigestInit_ex(context, EVP_sha256(), 0) == 1;
	uint64_t total = 0;
	char buffer[1024 * 1024];
	while (valid) {
		ssize_t count = read(fd, buffer, sizeof(buffer));
		if (count > 0) {
			valid = EVP_DigestUpdate(context, buffer, (size_t)count) == 1;
			total += (uint64_t)count;
		} else if (!count) {
			break;
		} else {
			valid = 0;
		}
	}
	unsigned int digest_size = 0;
	valid = valid && EVP_DigestFinal_ex(context, digest, &digest_size) == 1 &&
		digest_size == 32;
	if (context)
		EVP_MD_CTX_free(context);
	if (fd >= 0)
		close(fd);
	if (valid)
		*size = total;
	return valid;
}

static void digest_hex(const unsigned char digest[32], char encoded[65])
{
	for (size_t index = 0; index < 32; index++)
		snprintf(encoded + index * 2, 3, "%02x", digest[index]);
}

static int digest_bytes(const char encoded[65], unsigned char digest[32])
{
	if (strlen(encoded) != 64)
		return 0;
	for (size_t index = 0; index < 32; index++) {
		unsigned int value = 0;
		if (sscanf(encoded + index * 2, "%2x", &value) != 1)
			return 0;
		digest[index] = (unsigned char)value;
	}
	return 1;
}

static size_t encoded_size(const char *workflow_id,
		struct publication_output *outputs, size_t count)
{
	size_t size = 4 + strlen(workflow_id);
	for (size_t index = 0; index < count; index++)
		size += 72 + strlen(outputs[index].info.codec_name) +
			strlen(outputs[index].info.codec_version);
	return size;
}

static unsigned char *encode_publication(const char *workflow_id,
		struct publication_output *outputs, size_t count, size_t *payload_size)
{
	size_t workflow_size = strlen(workflow_id);
	if (!workflow_size || workflow_size > UINT16_MAX || !count ||
			count > UINT16_MAX)
		return 0;
	*payload_size = encoded_size(workflow_id, outputs, count);
	if (*payload_size > UINT32_MAX)
		return 0;
	unsigned char *payload = calloc(1, *payload_size);
	if (!payload)
		return 0;
	vine_datavine_put_u16(payload, (uint16_t)workflow_size);
	vine_datavine_put_u16(payload + 2, (uint16_t)count);
	memcpy(payload + 4, workflow_id, workflow_size);
	size_t offset = 4 + workflow_size;
	for (size_t index = 0; index < count; index++) {
		struct vine_datavine_workflow_result_info *info = &outputs[index].info;
		size_t codec_name_size = strlen(info->codec_name);
		size_t codec_version_size = strlen(info->codec_version);
		unsigned char digest[32];
		if (codec_name_size > UINT16_MAX || codec_version_size > UINT16_MAX ||
				!digest_bytes(info->sha256, digest)) {
			free(payload);
			return 0;
		}
		vine_datavine_put_u64(payload + offset, info->data_id);
		vine_datavine_put_u32(payload + offset + 8, info->attempt);
		vine_datavine_put_u64(payload + offset + 12, info->size);
		vine_datavine_put_u64(payload + offset + 20,
				(uint64_t)info->producer_task_id);
		vine_datavine_put_u32(payload + offset + 28,
				(uint32_t)info->producer_output_index);
		vine_datavine_put_u32(payload + offset + 32,
				(uint32_t)info->requested);
		memcpy(payload + offset + 36, digest, 32);
		vine_datavine_put_u16(payload + offset + 68,
				(uint16_t)codec_name_size);
		vine_datavine_put_u16(payload + offset + 70,
				(uint16_t)codec_version_size);
		offset += 72;
		memcpy(payload + offset, info->codec_name, codec_name_size);
		offset += codec_name_size;
		memcpy(payload + offset, info->codec_version, codec_version_size);
		offset += codec_version_size;
	}
	return payload;
}

static void installation_delete(struct result_installation *installation)
{
	if (!installation)
		return;
	for (size_t index = 0; index < installation->count; index++) {
		data_result_delete(installation->pending ? installation->pending[index] : 0);
		free(installation->keys ? installation->keys[index] : 0);
	}
	free(installation->pending);
	free(installation->keys);
	memset(installation, 0, sizeof(*installation));
}

static int installation_prepare(struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct publication_output *outputs,
		size_t count, struct result_installation *installation)
{
	memset(installation, 0, sizeof(*installation));
	installation->pending = calloc(count, sizeof(*installation->pending));
	installation->keys = calloc(count, sizeof(*installation->keys));
	installation->count = count;
	int valid = installation->pending && installation->keys;
	for (size_t index = 0; valid && index < count; index++) {
		installation->keys[index] = result_key(
				workflow_id, outputs[index].info.data_id);
		struct data_result *known = 0;
		if (installation->keys[index])
			known = hash_table_lookup(controller->results,
					installation->keys[index]);
		if (known) {
			valid = known->info.attempt <= outputs[index].info.attempt;
			if (!valid)
				break;
			if (known->info.attempt == outputs[index].info.attempt) {
				valid = !strcmp(known->info.sha256,
						outputs[index].info.sha256);
				continue;
			}
		}
		installation->pending[index] = calloc(
				1, sizeof(*installation->pending[index]));
		if (installation->pending[index]) {
			installation->pending[index]->info = outputs[index].info;
			installation->pending[index]->path = strdup(outputs[index].path);
			installation->pending[index]->remote_file = outputs[index].remote_file;
			installation->pending[index]->durable = 1;
		}
		valid = installation->keys[index] && installation->pending[index] &&
			installation->pending[index]->path;
	}
	if (!valid)
		installation_delete(installation);
	return valid;
}

static void installation_apply(struct vine_datavine_data_controller *controller,
		struct result_installation *installation)
{
	for (size_t index = 0; index < installation->count; index++) {
		struct data_result *known = hash_table_lookup(
				controller->results, installation->keys[index]);
		if (known && installation->pending[index] &&
				known->info.attempt < installation->pending[index]->info.attempt) {
			known = hash_table_remove(controller->results,
					installation->keys[index]);
			unlink(known->path);
			data_result_delete(known);
		}
		char *key = installation->keys[index];
		if (!hash_table_lookup(controller->results, key)) {
			if (!hash_table_insert(controller->results, key,
					installation->pending[index]))
				abort();
			installation->pending[index] = 0;
		}
	}
}

static int install_results(struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct publication_output *outputs,
		size_t count, int replay)
{
	struct result_installation installation;
	int valid = installation_prepare(controller, workflow_id, outputs, count, &installation);
	if (valid && !replay) {
		size_t payload_size = 0;
		unsigned char *payload = encode_publication(
				workflow_id, outputs, count, &payload_size);
		valid = payload && vine_datavine_journal_commit(controller->journal,
						   DATA_READY_BATCH,
						   payload,
						   payload_size);
		free(payload);
	}
	if (valid)
		installation_apply(controller, &installation);
	installation_delete(&installation);
	return valid;
}

static int install_soft_results(struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct publication_output *outputs,
		size_t count)
{
	int valid = 1;
	for (size_t index = 0; valid && index < count; index++) {
		char *key = result_key(workflow_id, outputs[index].info.data_id);
		struct data_result *known = key
							    ? hash_table_lookup(controller->results, key)
							    : 0;
		if (known) {
			valid = known->info.attempt <= outputs[index].info.attempt;
			if (!valid) {
				free(key);
				break;
			}
			if (known->info.attempt == outputs[index].info.attempt) {
				valid = !strcmp(known->info.sha256,
						outputs[index].info.sha256);
				free(key);
				continue;
			}
		}
		struct data_result *result = calloc(1, sizeof(*result));
		if (result) {
			result->info = outputs[index].info;
			result->remote_file = outputs[index].remote_file;
		}
		valid = key && result;
		if (valid && known) {
			known = hash_table_remove(controller->results, key);
			data_result_delete(known);
		}
		if (valid && !hash_table_insert(controller->results, key, result))
			abort();
		if (!valid)
			data_result_delete(result);
		free(key);
	}
	return valid;
}

static int replay_record(void *context, uint16_t opcode,
		const unsigned char *payload, size_t payload_size)
{
	struct vine_datavine_data_controller *controller = context;
	if (payload_size < 4)
		return 0;
	uint16_t workflow_size = vine_datavine_get_u16(payload);
	uint16_t count = vine_datavine_get_u16(payload + 2);
	if (!workflow_size || !count || 4U + workflow_size > payload_size)
		return 0;
	if (opcode == DATA_RELEASE_BATCH) {
		if (payload_size != 4U + workflow_size + (size_t)count * 8U)
			return 0;
		char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
		if (workflow_size > VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX)
			return 0;
		memcpy(workflow_id, payload + 4, workflow_size);
		workflow_id[workflow_size] = 0;
		for (uint16_t index = 0; index < count; index++) {
			uint64_t data_id = vine_datavine_get_u64(
					payload + 4 + workflow_size + (size_t)index * 8);
			char *key = result_key(workflow_id, data_id);
			struct data_result *removed = key
								      ? hash_table_remove(controller->results, key)
								      : 0;
			if (removed)
				unlink(removed->path);
			data_result_delete(removed);
			free(key);
		}
		return 1;
	}
	if (opcode != DATA_READY_BATCH)
		return 0;
	char *workflow_id = malloc((size_t)workflow_size + 1);
	struct publication_output *outputs = calloc(count, sizeof(*outputs));
	if (!workflow_id || !outputs) {
		free(workflow_id);
		free(outputs);
		return 0;
	}
	memcpy(workflow_id, payload + 4, workflow_size);
	workflow_id[workflow_size] = 0;
	size_t offset = 4 + workflow_size;
	int valid = 1;
	for (uint16_t index = 0; valid && index < count; index++) {
		if (offset > payload_size || payload_size - offset < 72) {
			valid = 0;
			break;
		}
		struct vine_datavine_workflow_result_info *info = &outputs[index].info;
		info->data_id = vine_datavine_get_u64(payload + offset);
		info->attempt = vine_datavine_get_u32(payload + offset + 8);
		info->size = vine_datavine_get_u64(payload + offset + 12);
		info->producer_task_id =
				(int64_t)vine_datavine_get_u64(payload + offset + 20);
		info->producer_output_index =
				(int32_t)vine_datavine_get_u32(payload + offset + 28);
		info->requested = (int)vine_datavine_get_u32(payload + offset + 32);
		digest_hex(payload + offset + 36, info->sha256);
		uint16_t name_size = vine_datavine_get_u16(payload + offset + 68);
		uint16_t version_size = vine_datavine_get_u16(payload + offset + 70);
		offset += 72;
		if (!name_size || !version_size || name_size >= sizeof(info->codec_name) ||
				version_size >= sizeof(info->codec_version) ||
				name_size + version_size > payload_size - offset) {
			valid = 0;
			break;
		}
		memcpy(info->codec_name, payload + offset, name_size);
		info->codec_name[name_size] = 0;
		offset += name_size;
		memcpy(info->codec_version, payload + offset, version_size);
		info->codec_version[version_size] = 0;
		offset += version_size;
		char path[PATH_MAX];
		outputs[index].path = output_path(controller, workflow_id, info->data_id, info->attempt, path)
						      ? strdup(path)
						      : 0;
		valid = outputs[index].path != 0;
	}
	valid = valid && offset == payload_size &&
		install_results(controller, workflow_id, outputs, count, 1);
	for (uint16_t index = 0; index < count; index++)
		free(outputs[index].path);
	free(outputs);
	free(workflow_id);
	return valid;
}

static int validate_catalog(struct vine_datavine_data_controller *controller)
{
	char *key;
	struct data_result *result;
	int iterator;
	HASH_TABLE_ITERATE(controller->results, iterator, key, result)
	{
		uint64_t size = 0;
		unsigned char digest[32];
		char encoded[65];
		if (!hash_file(result->path, &size, digest))
			return 0;
		digest_hex(digest, encoded);
		if (size != result->info.size || strcmp(encoded, result->info.sha256))
			return 0;
	}
	return 1;
}

static void publication_finish(
		struct vine_datavine_data_publication *publication, int successful,
		const struct vine_datavine_data_publication_metrics *metrics)
{
	pthread_mutex_lock(&publication->lock);
	if (metrics)
		publication->metrics = *metrics;
	publication->successful = successful;
	publication->done = 1;
	pthread_cond_broadcast(&publication->changed);
	pthread_mutex_unlock(&publication->lock);
}

static int prepare_job(struct vine_datavine_data_controller *controller,
		struct publication_job *job)
{
	job->durable = calloc(job->count, sizeof(*job->durable));
	struct publication_output *soft = calloc(job->count, sizeof(*soft));
	size_t soft_count = 0;
	int valid = job->durable && soft;
	for (size_t index = 0; valid && index < job->count; index++) {
		if (job->outputs[index].remote_only)
			soft[soft_count++] = job->outputs[index];
		else
			job->durable[job->durable_count++] = job->outputs[index];
	}
	job->metrics.outputs = job->count;
	job->metrics.remote_outputs = soft_count;
	job->metrics.durable_outputs = job->durable_count;
	if (soft_count) {
		pthread_mutex_lock(&controller->lock);
		valid = install_soft_results(controller, job->workflow_id, soft, soft_count);
		pthread_mutex_unlock(&controller->lock);
	}
	for (size_t index = 0; valid && index < job->durable_count; index++) {
		struct publication_output *output = &job->durable[index];
		unsigned char digest[32];
		valid = write_plain_output(output) &&
			hash_file(output->path, &output->info.size, digest);
		if (valid)
			digest_hex(digest, output->info.sha256);
	}
	for (size_t index = 0; index < soft_count; index++)
		job->metrics.output_bytes += soft[index].info.size;
	for (size_t index = 0; index < job->durable_count; index++)
		job->metrics.output_bytes += job->durable[index].info.size;
	free(soft);
	return valid;
}

static int batch_has_duplicate_results(struct publication_job *jobs)
{
	struct hash_table *seen = hash_table_create(0, 0);
	int duplicate = seen == 0;
	for (struct publication_job *job = jobs; !duplicate && job; job = job->next) {
		for (size_t index = 0; index < job->durable_count; index++) {
			char *key = result_key(job->workflow_id,
					job->durable[index].info.data_id);
			duplicate = !key || hash_table_lookup(seen, key);
			if (!duplicate && !hash_table_insert(seen, key, (void *)seen))
				abort();
			free(key);
			if (duplicate)
				break;
		}
	}
	if (seen)
		hash_table_delete(seen);
	return duplicate;
}

static void finish_job(struct publication_job *job, int successful)
{
	struct timespec finished;
	clock_gettime(CLOCK_MONOTONIC, &finished);
	job->metrics.commit_nanoseconds = elapsed_nanoseconds(
			&job->commit_started, &finished);
	publication_finish(job->publication, successful, &job->metrics);
}

static void commit_job_group(struct vine_datavine_data_controller *controller,
		struct publication_job *jobs)
{
	size_t count = 0;
	for (struct publication_job *job = jobs; job; job = job->next)
		count++;
	struct publication_job **accepted = calloc(count, sizeof(*accepted));
	struct result_installation *installations = calloc(count, sizeof(*installations));
	unsigned char **payloads = calloc(count, sizeof(*payloads));
	size_t *payload_sizes = calloc(count, sizeof(*payload_sizes));
	int *success = calloc(count, sizeof(*success));
	int allocated = accepted && installations && payloads && payload_sizes && success;
	size_t accepted_count = 0;

	pthread_mutex_lock(&controller->lock);
	for (struct publication_job *job = jobs; allocated && job; job = job->next) {
		struct result_installation *installation =
				&installations[accepted_count];
		if (!installation_prepare(controller, job->workflow_id, job->durable, job->durable_count, installation))
			continue;
		payloads[accepted_count] = encode_publication(job->workflow_id,
				job->durable,
				job->durable_count,
				&payload_sizes[accepted_count]);
		if (!payloads[accepted_count]) {
			installation_delete(installation);
			continue;
		}
		accepted[accepted_count++] = job;
	}
	int committed = allocated && accepted_count;
	for (size_t index = 0; committed && index < accepted_count; index++) {
		if (index + 1 < accepted_count)
			committed = vine_datavine_journal_enqueue(controller->journal,
					DATA_READY_BATCH, payloads[index], payload_sizes[index]);
		else
			committed = vine_datavine_journal_commit(controller->journal,
					DATA_READY_BATCH, payloads[index], payload_sizes[index]);
	}
	if (committed) {
		accepted[0]->metrics.journal_commits++;
		for (size_t index = 0; index < accepted_count; index++) {
			installation_apply(controller, &installations[index]);
			success[index] = 1;
		}
	}
	pthread_mutex_unlock(&controller->lock);

	for (size_t index = 0; index < accepted_count; index++) {
		installation_delete(&installations[index]);
		free(payloads[index]);
	}
	for (struct publication_job *job = jobs; job; job = job->next) {
		int job_success = 0;
		for (size_t index = 0; index < accepted_count; index++) {
			if (accepted[index] == job) {
				job_success = success[index];
				break;
			}
		}
		finish_job(job, job_success);
	}
	free(success);
	free(payload_sizes);
	free(payloads);
	free(installations);
	free(accepted);
}

static void commit_prepared_jobs(struct vine_datavine_data_controller *controller,
		struct publication_job *jobs)
{
	if (!batch_has_duplicate_results(jobs)) {
		commit_job_group(controller, jobs);
		return;
	}
	while (jobs) {
		struct publication_job *next = jobs->next;
		jobs->next = 0;
		commit_job_group(controller, jobs);
		jobs->next = next;
		jobs = next;
	}
}

static void job_delete(struct publication_job *job)
{
	if (!job)
		return;
	for (size_t index = 0; index < job->count; index++) {
		free(job->outputs[index].path);
		free(job->outputs[index].plain_data);
	}
	free(job->durable);
	free(job->outputs);
	free(job->workflow_id);
	free(job);
}

static void *data_worker(void *argument)
{
	struct vine_datavine_data_controller *controller = argument;
	for (;;) {
		pthread_mutex_lock(&controller->queue_lock);
		while (!controller->stopping && !controller->head)
			pthread_cond_wait(&controller->queued, &controller->queue_lock);
		if (controller->stopping && !controller->head) {
			pthread_mutex_unlock(&controller->queue_lock);
			break;
		}
		struct publication_job *job = controller->head;
		controller->head = job->next;
		if (!controller->head)
			controller->tail = 0;
		controller->queued_count--;
		pthread_cond_broadcast(&controller->space);
		pthread_mutex_unlock(&controller->queue_lock);
		clock_gettime(CLOCK_MONOTONIC, &job->commit_started);
		job->metrics.queue_nanoseconds = elapsed_nanoseconds(
				&job->queued_at, &job->commit_started);
		job->metrics.decode_nanoseconds = job->decode_nanoseconds;
		job->metrics.function_nanoseconds = job->function_nanoseconds;
		job->metrics.serialize_nanoseconds = job->serialize_nanoseconds;
		job->metrics.fsync_nanoseconds = job->fsync_nanoseconds;
		if (!prepare_job(controller, job)) {
			finish_job(job, 0);
			job_delete(job);
			continue;
		}
		if (!job->durable_count) {
			finish_job(job, 1);
			job_delete(job);
			continue;
		}

		pthread_mutex_lock(&controller->queue_lock);
		job->next = 0;
		if (controller->prepared_tail)
			controller->prepared_tail->next = job;
	else
		controller->prepared_head = job;
	controller->prepared_tail = job;
	pthread_cond_signal(&controller->prepared);
	if (controller->commit_active) {
			pthread_mutex_unlock(&controller->queue_lock);
			continue;
		}
		controller->commit_active = 1;
		for (;;) {
			if (!controller->stopping) {
				struct timespec deadline;
				clock_gettime(CLOCK_REALTIME, &deadline);
				deadline.tv_nsec += DATA_COMMIT_COALESCE_NS;
				if (deadline.tv_nsec >= INT64_C(1000000000)) {
					deadline.tv_sec++;
					deadline.tv_nsec -= INT64_C(1000000000);
				}
				while (!controller->stopping &&
						pthread_cond_timedwait(&controller->prepared,
								&controller->queue_lock, &deadline) != ETIMEDOUT) {
				}
			}
			struct publication_job *batch = controller->prepared_head;
			controller->prepared_head = 0;
			controller->prepared_tail = 0;
			pthread_mutex_unlock(&controller->queue_lock);
			commit_prepared_jobs(controller, batch);
			while (batch) {
				struct publication_job *next = batch->next;
				job_delete(batch);
				batch = next;
			}
			pthread_mutex_lock(&controller->queue_lock);
			if (!controller->prepared_head) {
				controller->commit_active = 0;
				pthread_mutex_unlock(&controller->queue_lock);
				break;
			}
		}
	}
	return 0;
}

struct vine_datavine_data_controller *vine_datavine_data_controller_open(
		const char *workflow_journal_path, size_t threads)
{
	if (!workflow_journal_path || !workflow_journal_path[0] || !threads)
		return 0;
	struct vine_datavine_data_controller *controller =
			calloc(1, sizeof(*controller));
	if (!controller)
		return 0;
	size_t root_size = strlen(workflow_journal_path) + 6;
	controller->root = malloc(root_size);
	if (controller->root)
		snprintf(controller->root, root_size, "%s.data", workflow_journal_path);
	char catalog[PATH_MAX];
	int valid = controller->root && create_dir(controller->root, 0700) &&
		    snprintf(catalog, sizeof(catalog), "%s/catalog", controller->root) <
				    (int)sizeof(catalog);
	if (!pthread_mutex_init(&controller->lock, 0))
		controller->lock_initialized = 1;
	if (controller->lock_initialized &&
			!pthread_mutex_init(&controller->queue_lock, 0))
		controller->queue_lock_initialized = 1;
	if (controller->queue_lock_initialized &&
			!pthread_cond_init(&controller->queued, 0))
		controller->queued_initialized = 1;
	if (controller->queued_initialized &&
			!pthread_cond_init(&controller->space, 0))
		controller->space_initialized = 1;
	if (controller->space_initialized &&
			!pthread_cond_init(&controller->prepared, 0))
		controller->prepared_initialized = 1;
	valid = valid && controller->prepared_initialized;
	controller->results = hash_table_create(0, 0);
	controller->journal = valid ? vine_datavine_journal_open(catalog) : 0;
	controller->threads = calloc(threads, sizeof(*controller->threads));
	valid = valid && controller->results && controller->journal &&
		controller->threads &&
		vine_datavine_journal_replay(controller->journal, replay_record, controller) &&
		validate_catalog(controller);
	if (!valid) {
		vine_datavine_data_controller_close(controller);
		return 0;
	}
	for (size_t index = 0; index < threads; index++) {
		if (pthread_create(&controller->threads[index], 0, data_worker, controller)) {
			vine_datavine_data_controller_close(controller);
			return 0;
		}
		controller->thread_count++;
	}
	return controller;
}

void vine_datavine_data_controller_close(
		struct vine_datavine_data_controller *controller)
{
	if (!controller)
		return;
	if (controller->queue_lock_initialized) {
		pthread_mutex_lock(&controller->queue_lock);
		controller->stopping = 1;
		if (controller->queued_initialized)
			pthread_cond_broadcast(&controller->queued);
		if (controller->space_initialized)
			pthread_cond_broadcast(&controller->space);
		if (controller->prepared_initialized)
			pthread_cond_broadcast(&controller->prepared);
		pthread_mutex_unlock(&controller->queue_lock);
	}
	for (size_t index = 0; index < controller->thread_count; index++)
		pthread_join(controller->threads[index], 0);
	while (controller->head) {
		struct publication_job *next = controller->head->next;
		publication_finish(controller->head->publication, 0, 0);
		job_delete(controller->head);
		controller->head = next;
	}
	if (controller->journal)
		vine_datavine_journal_close(controller->journal);
	if (controller->results) {
		hash_table_clear(controller->results, data_result_delete);
		hash_table_delete(controller->results);
	}
	free(controller->threads);
	free(controller->root);
	if (controller->prepared_initialized)
		pthread_cond_destroy(&controller->prepared);
	if (controller->space_initialized)
		pthread_cond_destroy(&controller->space);
	if (controller->queued_initialized)
		pthread_cond_destroy(&controller->queued);
	if (controller->queue_lock_initialized)
		pthread_mutex_destroy(&controller->queue_lock);
	if (controller->lock_initialized)
		pthread_mutex_destroy(&controller->lock);
	free(controller);
}

static int retained_output(struct itable *consumers, struct itable *requested,
		uint64_t data_id, int retain_all)
{
	return retain_all || itable_lookup(consumers, data_id) ||
	       itable_lookup(requested, data_id);
}

int vine_datavine_data_controller_bind_outputs(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id,
		struct vine_task *physical, struct jx *task, struct itable *files,
		struct itable *consumers,
		struct itable *requested, uint32_t attempt, int retain_all)
{
	struct jx *executor = jx_lookup(task, "executor");
	struct jx *output_files = jx_lookup(executor, "output_files");
	struct jx *output_ids = jx_lookup(task, "output_data_ids");
	int worker_local = !strcmp(jx_lookup_string(executor, "kind"), "python") &&
			   !strcmp(jx_lookup_string(executor, "version"), "callable-v1");
	if (!output_files)
		return jx_array_length(output_ids) == 1;
	if (jx_array_length(output_files) != jx_array_length(output_ids))
		return 0;
	char directory[PATH_MAX];
	int directory_ready = 0;
	for (int index = 0; index < jx_array_length(output_files); index++) {
		uint64_t data_id = (uint64_t)jx_array_index(
				output_ids, index)
						   ->u.integer_value;
		if (!retained_output(consumers, requested, data_id, retain_all))
			continue;
		char path[PATH_MAX];
		int remote_only = worker_local && !itable_lookup(requested, data_id);
		if (!remote_only && !directory_ready) {
			directory_ready = workflow_directory(controller, workflow_id, directory);
			if (!directory_ready)
				return 0;
		}
		struct vine_file *file = remote_only
							 ? vine_declare_temp(manager)
							 : (output_path_in_directory(directory, data_id, attempt, path)
											   ? vine_declare_file(manager, path, VINE_CACHE_LEVEL_WORKFLOW, 0)
											   : 0);
		const char *remote_name = jx_array_index(
				output_files, index)
							  ->u.string_value;
		struct vine_file *previous = itable_remove(files, data_id);
		if (previous)
			vine_undeclare_file(manager, previous);
		if (!file || !vine_task_add_output(physical, file, remote_name, 0) ||
				!itable_insert(files, data_id, file))
			return 0;
	}
	return 1;
}

static struct vine_datavine_data_publication *publication_create(void)
{
	struct vine_datavine_data_publication *publication =
			calloc(1, sizeof(*publication));
	if (!publication)
		return 0;
	if (pthread_mutex_init(&publication->lock, 0)) {
		free(publication);
		return 0;
	}
	if (pthread_cond_init(&publication->changed, 0)) {
		pthread_mutex_destroy(&publication->lock);
		free(publication);
		return 0;
	}
	return publication;
}

static int fill_output_metadata(struct publication_output *output,
		struct jx *data_record, struct itable *requested, uint64_t data_id,
		uint32_t attempt, const char *path)
{
	struct jx *origin = data_record ? jx_lookup(data_record, "origin") : 0;
	struct jx *codec = data_record ? jx_lookup(data_record, "codec") : 0;
	const char *codec_name = codec ? jx_lookup_string(codec, "name") : 0;
	const char *codec_version = codec ? jx_lookup_string(codec, "version") : 0;
	if (!origin || strcmp(jx_lookup_string(origin, "kind"), "output") ||
			!codec_name || !codec_version ||
			strlen(codec_name) >= sizeof(output->info.codec_name) ||
			strlen(codec_version) >= sizeof(output->info.codec_version))
		return 0;
	output->info.data_id = data_id;
	output->info.attempt = attempt;
	output->info.producer_task_id = jx_lookup_integer(origin, "task_id");
	output->info.producer_output_index =
			(int32_t)jx_lookup_integer(origin, "output_index");
	output->info.requested = itable_lookup(requested, data_id) != 0;
	snprintf(output->info.codec_name, sizeof(output->info.codec_name), "%s", codec_name);
	snprintf(output->info.codec_version,
			sizeof(output->info.codec_version),
			"%s",
			codec_version);
	output->path = path ? strdup(path) : 0;
	return !path || output->path;
}

static int parse_output_manifest(const char *manifest,
		struct publication_output *outputs, size_t count,
		uint64_t *decode_nanoseconds, uint64_t *function_nanoseconds,
		uint64_t *serialize_nanoseconds,
		uint64_t *fsync_nanoseconds)
{
	if (!manifest || strncmp(manifest, "DVM1\n", 5))
		return 0;
	char *end = 0;
	unsigned long declared = strtoul(manifest + 5, &end, 10);
	if (!end || *end++ != '\n' || declared != count)
		return 0;
	for (size_t index = 0; index < count; index++) {
		unsigned long long size = strtoull(end, &end, 10);
		if (!end || *end++ != ' ')
			return 0;
		if (strlen(end) < 65 || end[64] != '\n')
			return 0;
		memcpy(outputs[index].info.sha256, end, 64);
		outputs[index].info.sha256[64] = 0;
		unsigned char digest[32];
		if (!digest_bytes(outputs[index].info.sha256, digest))
			return 0;
		outputs[index].info.size = (uint64_t)size;
		end += 65;
	}
	if (!*end)
		return 1;
	unsigned long long decode_ns = 0;
	unsigned long long function_ns = 0;
	unsigned long long serialize_ns = 0;
	unsigned long long fsync_ns = 0;
	char trailing = 0;
	int fields = sscanf(end, "M %llu %llu %llu %llu\n%c", &decode_ns, &function_ns, &serialize_ns, &fsync_ns, &trailing);
	if (fields != 4) {
		decode_ns = 0;
		fields = sscanf(end, "M %llu %llu %llu\n%c", &function_ns, &serialize_ns, &fsync_ns, &trailing);
		if (fields != 3)
			return 0;
	}
	*decode_nanoseconds = (uint64_t)decode_ns;
	*function_nanoseconds = (uint64_t)function_ns;
	*serialize_nanoseconds = (uint64_t)serialize_ns;
	*fsync_nanoseconds = (uint64_t)fsync_ns;
	return 1;
}

struct vine_datavine_data_publication *
vine_datavine_data_controller_publish_async(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct jx *task, struct itable *data,
		struct itable *files, struct itable *consumers, struct itable *requested,
		uint32_t attempt, int retain_all, struct vine_task *completed)
{
	if (!controller || !workflow_id || !task || !data || !files || !consumers ||
			!requested || !attempt)
		return 0;
	struct vine_datavine_data_publication *publication = publication_create();
	struct publication_job *job = calloc(1, sizeof(*job));
	struct jx *output_ids = jx_lookup(task, "output_data_ids");
	struct jx *output_files = jx_lookup(jx_lookup(task, "executor"),
			"output_files");
	struct jx *executor = jx_lookup(task, "executor");
	int worker_local = output_files &&
			   !strcmp(jx_lookup_string(executor, "kind"), "python") &&
			   !strcmp(jx_lookup_string(executor, "version"), "callable-v1");
	const char *plain_output = output_files ? 0 : vine_task_get_stdout(completed);
	size_t plain_output_size = plain_output ? strlen(plain_output) : 0;
	int total = jx_array_length(output_ids);
	if (!publication || !job || total < 1 ||
			(output_files && jx_array_length(output_files) != total)) {
		vine_datavine_data_publication_delete(publication);
		free(job);
		return 0;
	}
	job->workflow_id = strdup(workflow_id);
	job->task_id = jx_lookup_integer(task, "task_id");
	job->attempt = attempt;
	job->outputs = calloc((size_t)total, sizeof(*job->outputs));
	job->publication = publication;
	struct publication_output *manifest_outputs = worker_local
								      ? calloc((size_t)total, sizeof(*manifest_outputs))
								      : 0;
	int valid = job->workflow_id && job->outputs &&
		    (!worker_local || (manifest_outputs && parse_output_manifest(
									   vine_task_get_stdout(completed), manifest_outputs, (size_t)total, &job->decode_nanoseconds, &job->function_nanoseconds, &job->serialize_nanoseconds, &job->fsync_nanoseconds)));
	char directory[PATH_MAX];
	int directory_ready = 0;
	for (int index = 0; valid && index < total; index++) {
		uint64_t data_id = (uint64_t)jx_array_index(
				output_ids, index)
						   ->u.integer_value;
		if (!retained_output(consumers, requested, data_id, retain_all))
			continue;
		char path[PATH_MAX];
		struct publication_output *output = &job->outputs[job->count++];
		int durable_worker_output = worker_local &&
					    itable_lookup(requested, data_id) != 0;
		if ((!worker_local || durable_worker_output) && !directory_ready)
			directory_ready = workflow_directory(controller, workflow_id, directory);
		valid = ((!worker_local || durable_worker_output)
							? directory_ready && output_path_in_directory(
											     directory, data_id, attempt, path)
							: 1) &&
			fill_output_metadata(output, itable_lookup(data, data_id), requested, data_id, attempt, worker_local && !durable_worker_output ? 0 : path);
		if (valid && worker_local) {
			output->info.size = manifest_outputs[index].info.size;
			memcpy(output->info.sha256, manifest_outputs[index].info.sha256, sizeof(output->info.sha256));
			output->remote_file = itable_lookup(files, data_id);
			valid = output->remote_file != 0;
			output->remote_only = !durable_worker_output;
		}
		if (valid && !output_files) {
			if (total != 1 || (!plain_output && plain_output_size)) {
				valid = 0;
				break;
			}
			output->plain_data = malloc(plain_output_size + 1);
			if (!output->plain_data) {
				valid = 0;
				break;
			}
			memcpy(output->plain_data,
					plain_output ? plain_output : "",
					plain_output_size);
			output->plain_size = plain_output_size;
		}
	}
	free(manifest_outputs);
	if (!valid) {
		job_delete(job);
		vine_datavine_data_publication_delete(publication);
		return 0;
	}
	if (!job->count) {
		publication_finish(publication, 1, 0);
		job_delete(job);
		return publication;
	}
	pthread_mutex_lock(&controller->queue_lock);
	while (!controller->stopping &&
			controller->queued_count >= DATA_QUEUE_LIMIT)
		pthread_cond_wait(&controller->space, &controller->queue_lock);
	if (controller->stopping) {
		pthread_mutex_unlock(&controller->queue_lock);
		job_delete(job);
		vine_datavine_data_publication_delete(publication);
		return 0;
	}
	if (controller->tail)
		controller->tail->next = job;
	else
		controller->head = job;
	controller->tail = job;
	clock_gettime(CLOCK_MONOTONIC, &job->queued_at);
	controller->queued_count++;
	pthread_cond_signal(&controller->queued);
	pthread_mutex_unlock(&controller->queue_lock);
	return publication;
}

int vine_datavine_data_publication_ready(
		struct vine_datavine_data_publication *publication)
{
	if (!publication)
		return 0;
	pthread_mutex_lock(&publication->lock);
	int ready = publication->done;
	pthread_mutex_unlock(&publication->lock);
	return ready;
}

int vine_datavine_data_publication_wait(
		struct vine_datavine_data_publication *publication)
{
	if (!publication)
		return 0;
	pthread_mutex_lock(&publication->lock);
	while (!publication->done)
		pthread_cond_wait(&publication->changed, &publication->lock);
	int successful = publication->successful;
	pthread_mutex_unlock(&publication->lock);
	return successful;
}

int vine_datavine_data_publication_get_metrics(
		struct vine_datavine_data_publication *publication,
		struct vine_datavine_data_publication_metrics *metrics)
{
	if (!publication || !metrics)
		return 0;
	pthread_mutex_lock(&publication->lock);
	int ready = publication->done;
	if (ready)
		*metrics = publication->metrics;
	pthread_mutex_unlock(&publication->lock);
	return ready;
}

void vine_datavine_data_publication_delete(
		struct vine_datavine_data_publication *publication)
{
	if (!publication)
		return;
	pthread_mutex_destroy(&publication->lock);
	pthread_cond_destroy(&publication->changed);
	free(publication);
}

int vine_datavine_data_controller_fetch_result(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		char **data, size_t *size)
{
	if (!controller || !workflow_id || !data || !size || !data_id)
		return 0;
	*data = 0;
	*size = 0;
	char *key = result_key(workflow_id, data_id);
	pthread_mutex_lock(&controller->lock);
	struct data_result *result = key
						     ? hash_table_lookup(controller->results, key)
						     : 0;
	char *path = result && result->durable ? strdup(result->path) : 0;
	uint64_t expected_size = result ? result->info.size : 0;
	pthread_mutex_unlock(&controller->lock);
	free(key);
	if (!path || expected_size > SIZE_MAX) {
		free(path);
		return 0;
	}
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	free(path);
	char *buffer = fd >= 0 ? malloc((size_t)expected_size + 1) : 0;
	size_t used = 0;
	while (buffer && used < (size_t)expected_size) {
		ssize_t count = read(fd, buffer + used, (size_t)expected_size - used);
		if (count > 0)
			used += (size_t)count;
		else {
			free(buffer);
			buffer = 0;
		}
	}
	if (fd >= 0)
		close(fd);
	if (!buffer)
		return 0;
	buffer[used] = 0;
	*data = buffer;
	*size = used;
	return 1;
}

int vine_datavine_data_controller_result_info(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		struct vine_datavine_workflow_result_info *result)
{
	if (!controller || !workflow_id || !data_id || !result)
		return 0;
	char *key = result_key(workflow_id, data_id);
	pthread_mutex_lock(&controller->lock);
	struct data_result *stored = key
						     ? hash_table_lookup(controller->results, key)
						     : 0;
	if (stored && stored->durable)
		*result = stored->info;
	pthread_mutex_unlock(&controller->lock);
	free(key);
	return stored && stored->durable;
}

int vine_datavine_data_controller_result_active(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id)
{
	if (!controller || !workflow_id || !data_id)
		return 0;
	char *key = result_key(workflow_id, data_id);
	pthread_mutex_lock(&controller->lock);
	int active = key && hash_table_lookup(controller->results, key);
	pthread_mutex_unlock(&controller->lock);
	free(key);
	return active;
}

struct vine_file *vine_datavine_data_controller_restore_file(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id,
		uint64_t data_id)
{
	if (!controller || !manager || !workflow_id || !data_id)
		return 0;
	char *key = result_key(workflow_id, data_id);
	pthread_mutex_lock(&controller->lock);
	struct data_result *stored = key
						     ? hash_table_lookup(controller->results, key)
						     : 0;
	struct vine_file *remote_file = stored ? stored->remote_file : 0;
	char *path = stored && stored->durable ? strdup(stored->path) : 0;
	pthread_mutex_unlock(&controller->lock);
	free(key);
	struct vine_file *file = remote_file
						 ? remote_file
				 : path
						 ? vine_declare_file(manager, path, VINE_CACHE_LEVEL_WORKFLOW, 0)
						 : 0;
	free(path);
	return file;
}

int vine_datavine_data_controller_finish_workflow(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id)
{
	if (!controller || !manager || !workflow_id || !workflow_id[0])
		return 0;
	pthread_mutex_lock(&controller->lock);
	size_t prefix_size = strlen(workflow_id);
	size_t capacity = (size_t)hash_table_size(controller->results);
	uint64_t *drop = capacity ? malloc(capacity * sizeof(*drop)) : 0;
	uint64_t *durable_drop = capacity
						 ? malloc(capacity * sizeof(*durable_drop))
						 : 0;
	char *key;
	struct data_result *result;
	int iterator;
	size_t count = 0;
	size_t durable_count = 0;
	int valid = !capacity || (drop && durable_drop);
	HASH_TABLE_ITERATE(controller->results, iterator, key, result)
	{
		if (strncmp(key, workflow_id, prefix_size) ||
				key[prefix_size] != '\037')
			continue;
		if (result->remote_file) {
			vine_undeclare_file(manager, result->remote_file);
			result->remote_file = 0;
		}
		if (valid && !result->info.requested) {
			drop[count++] = result->info.data_id;
			if (result->durable)
				durable_drop[durable_count++] = result->info.data_id;
		}
	}
	if (valid && durable_count) {
		size_t workflow_size = strlen(workflow_id);
		size_t payload_size = 4 + workflow_size + durable_count * 8;
		unsigned char *payload = payload_size <= UINT32_MAX
							 ? malloc(payload_size)
							 : 0;
		valid = payload && workflow_size <= UINT16_MAX &&
			durable_count <= UINT16_MAX;
		if (valid) {
			vine_datavine_put_u16(payload, (uint16_t)workflow_size);
			vine_datavine_put_u16(payload + 2, (uint16_t)durable_count);
			memcpy(payload + 4, workflow_id, workflow_size);
			for (size_t index = 0; index < durable_count; index++)
				vine_datavine_put_u64(payload + 4 + workflow_size + index * 8,
						durable_drop[index]);
			valid = vine_datavine_journal_commit(controller->journal,
					DATA_RELEASE_BATCH,
					payload,
					payload_size);
		}
		free(payload);
	}
	if (valid) {
		for (size_t index = 0; index < count; index++) {
			char *drop_key = result_key(workflow_id, drop[index]);
			struct data_result *removed = drop_key
								      ? hash_table_remove(controller->results, drop_key)
								      : 0;
			if (removed && removed->path)
				unlink(removed->path);
			data_result_delete(removed);
			free(drop_key);
		}
	}
	free(durable_drop);
	free(drop);
	pthread_mutex_unlock(&controller->lock);
	return valid;
}
