/* Worker-local DataVine data plane. Manager file metadata is forbidden here. */

#include "vine_datavine_agent.h"

#include "vine_cache.h"
#include "vine_datavine_protocol.h"
#include "vine_process.h"

#include "create_dir.h"
#include "copy_stream.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "full_io.h"
#include "link.h"
#include "stringtools.h"
#include "timestamp.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <openssl/evp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define LOCAL_EMPTY 0U
#define LOCAL_FETCHING 1U
#define LOCAL_READY 2U
#define LOCAL_DIRTY 4U
#define LOCAL_REQUESTED 8U
#define LOCAL_GENERATED 16U
#define LOCAL_MIN_CAPACITY 1024U
#define LOCAL_PREPARE_BUDGET_US 25000U
#define LOCAL_PEER_RETRY_MIN_US 100000U
#define LOCAL_PEER_RETRY_MAX_US 1600000U

struct local_object {
	uint64_t data_id;
	uint64_t size;
	uint64_t object_token;
	uint64_t source_session_epoch;
	unsigned char digest[32];
	uint32_t generation;
	uint32_t source_worker_slot;
	uint32_t flags;
	uint8_t fetch_failures;
	timestamp_t fetch_retry_after;
};

struct agent_workflow {
	uint64_t workflow_slot;
	unsigned char workflow_key[32];
	char controller_host[VINE_DATAVINE_AGENT_HOST_MAX];
	uint16_t controller_port;
	uint32_t worker_slot;
	uint64_t session_epoch;
	uint64_t request_id;
	uint64_t sequence;
	uint64_t release_acknowledged;
	timestamp_t last_heartbeat;
	int fd;
	struct local_object *objects;
	size_t capacity;
	size_t count;
	struct agent_workflow *next;
};

struct task_spec {
	const unsigned char *bytes;
	size_t size;
	uint64_t workflow_slot;
	const unsigned char *workflow_key;
	char controller_host[VINE_DATAVINE_AGENT_HOST_MAX];
	uint16_t controller_port;
	uint32_t input_count;
	uint32_t output_count;
	const unsigned char *inputs;
	const unsigned char *outputs;
	const unsigned char *strings;
	size_t strings_size;
};

struct persistence_job {
	struct agent_workflow *workflow;
	struct local_object *object;
	char *path;
	struct persistence_job *next;
};

static struct vine_cache *agent_cache;
static char agent_transfer_host[VINE_DATAVINE_AGENT_HOST_MAX];
static uint16_t agent_transfer_port;
static struct agent_workflow *agent_workflows;
static struct persistence_job *persistence_head;
static struct persistence_job *persistence_tail;
static int agent_waiting_data;
static unsigned int agent_diagnostic_failures;

static void agent_diagnostic_failure(const char *operation, uint64_t data_id,
		const char *path, int error)
{
	/* Failure diagnostics are deliberately bounded: one broken origin may be
	 * referenced by thousands of waiting tasks on the same worker. */
	if (agent_diagnostic_failures++ >= 32)
		return;
	debug(D_NOTICE,
			"DataVine Worker Agent %s failed for data %llu path %s: %s",
			operation, (unsigned long long)data_id, path ? path : "-",
			error ? strerror(error) : "invalid task specification");
}

static uint64_t mix64(uint64_t value)
{
	value ^= value >> 30;
	value *= UINT64_C(0xbf58476d1ce4e5b9);
	value ^= value >> 27;
	value *= UINT64_C(0x94d049bb133111eb);
	return value ^ (value >> 31);
}

static int spec_parse(const struct vine_process *process, struct task_spec *spec)
{
	memset(spec, 0, sizeof(*spec));
	if (!process || !process->task || !process->task->auxiliary_payload_length)
		return 0;
	const unsigned char *bytes =
			(const unsigned char *)process->task->auxiliary_payload;
	size_t size = process->task->auxiliary_payload_length;
	if (size < VINE_DATAVINE_TASK_SPEC_HEADER ||
			memcmp(bytes, VINE_DATAVINE_TASK_SPEC_MAGIC, 4) ||
			vine_datavine_get_u16(bytes + 4) != 2 ||
			vine_datavine_get_u16(bytes + 6) ||
			vine_datavine_get_u32(bytes + 128) ||
			vine_datavine_get_u32(bytes + 132) ||
			vine_datavine_get_u64(bytes + 136))
		return -1;
	uint16_t host_size = vine_datavine_get_u16(bytes + 50);
	uint32_t inputs = vine_datavine_get_u32(bytes + 116);
	uint32_t outputs = vine_datavine_get_u32(bytes + 120);
	uint32_t strings_size = vine_datavine_get_u32(bytes + 124);
	size_t records = (size_t)inputs * VINE_DATAVINE_TASK_SPEC_INPUT +
			(size_t)outputs * VINE_DATAVINE_TASK_SPEC_OUTPUT;
	if (!host_size || host_size >= VINE_DATAVINE_AGENT_HOST_MAX ||
			!vine_datavine_get_u16(bytes + 48) ||
			records > SIZE_MAX - VINE_DATAVINE_TASK_SPEC_HEADER ||
			strings_size > SIZE_MAX - VINE_DATAVINE_TASK_SPEC_HEADER - records ||
			size != VINE_DATAVINE_TASK_SPEC_HEADER + records + strings_size)
		return -1;
	spec->bytes = bytes;
	spec->size = size;
	spec->workflow_slot = vine_datavine_get_u64(bytes + 40);
	spec->workflow_key = bytes + 8;
	spec->controller_port = vine_datavine_get_u16(bytes + 48);
	memcpy(spec->controller_host, bytes + 52, host_size);
	spec->controller_host[host_size] = 0;
	spec->input_count = inputs;
	spec->output_count = outputs;
	spec->inputs = bytes + VINE_DATAVINE_TASK_SPEC_HEADER;
	spec->outputs = spec->inputs +
			(size_t)inputs * VINE_DATAVINE_TASK_SPEC_INPUT;
	spec->strings = spec->outputs +
			(size_t)outputs * VINE_DATAVINE_TASK_SPEC_OUTPUT;
	spec->strings_size = strings_size;
	return spec->workflow_slot ? 1 : -1;
}

static int string_field(const struct task_spec *spec, uint32_t offset,
		uint32_t length, const char **value)
{
	if (!length || offset > spec->strings_size ||
			length > spec->strings_size - offset ||
			memchr(spec->strings + offset, 0, length))
		return 0;
	*value = (const char *)spec->strings + offset;
	return 1;
}

static int local_reserve(struct agent_workflow *workflow, size_t capacity)
{
	if (workflow->capacity >= capacity)
		return 1;
	size_t next_capacity = workflow->capacity ? workflow->capacity :
			LOCAL_MIN_CAPACITY;
	while (next_capacity < capacity)
		next_capacity *= 2;
	struct local_object *next = calloc(next_capacity, sizeof(*next));
	if (!next)
		return 0;
	for (size_t index = 0; index < workflow->capacity; index++) {
		struct local_object object = workflow->objects[index];
		if (!object.data_id)
			continue;
		size_t slot = (size_t)mix64(object.data_id) & (next_capacity - 1);
		while (next[slot].data_id)
			slot = (slot + 1) & (next_capacity - 1);
		next[slot] = object;
	}
	free(workflow->objects);
	workflow->objects = next;
	workflow->capacity = next_capacity;
	return 1;
}

static struct local_object *local_get(struct agent_workflow *workflow,
		uint64_t data_id, int create)
{
	if (!workflow || !data_id ||
			(create && workflow->count * 10 >= workflow->capacity * 7 &&
			 !local_reserve(workflow, workflow->capacity ?
					 workflow->capacity * 2 : LOCAL_MIN_CAPACITY)) ||
			(!workflow->capacity && !local_reserve(workflow, LOCAL_MIN_CAPACITY)))
		return 0;
	size_t slot = (size_t)mix64(data_id) & (workflow->capacity - 1);
	while (workflow->objects[slot].data_id &&
			workflow->objects[slot].data_id != data_id)
		slot = (slot + 1) & (workflow->capacity - 1);
	if (!workflow->objects[slot].data_id && create) {
		workflow->objects[slot].data_id = data_id;
		workflow->count++;
	}
	return workflow->objects[slot].data_id ? &workflow->objects[slot] : 0;
}

static struct agent_workflow *workflow_get(const struct task_spec *spec)
{
	for (struct agent_workflow *workflow = agent_workflows; workflow;
			workflow = workflow->next) {
		if (workflow->workflow_slot == spec->workflow_slot) {
			if (memcmp(workflow->workflow_key, spec->workflow_key, 32) ||
					strcmp(workflow->controller_host,
						spec->controller_host) ||
					workflow->controller_port != spec->controller_port)
				return 0;
			return workflow;
		}
	}
	struct agent_workflow *workflow = calloc(1, sizeof(*workflow));
	if (!workflow)
		return 0;
	workflow->workflow_slot = spec->workflow_slot;
	memcpy(workflow->workflow_key, spec->workflow_key, 32);
	snprintf(workflow->controller_host, sizeof(workflow->controller_host),
			"%s", spec->controller_host);
	workflow->controller_port = spec->controller_port;
	workflow->session_epoch = mix64((uint64_t)getpid() ^
			(uint64_t)timestamp_get() ^ spec->workflow_slot);
	if (!workflow->session_epoch)
		workflow->session_epoch = 1;
	workflow->request_id = 1;
	workflow->fd = -1;
	workflow->next = agent_workflows;
	agent_workflows = workflow;
	return workflow;
}

static void connection_close(struct agent_workflow *workflow)
{
	int was_connected = workflow->fd >= 0;
	if (was_connected)
		close(workflow->fd);
	workflow->fd = -1;
	/* Controller invalidates a disconnected session in O(replicas-on-worker).
	 * A reconnect therefore re-advertises every generated/peer replica in one
	 * or more bounded batches. Immutable URI origins are not Controller-owned. */
	if (was_connected) {
		for (size_t index = 0; index < workflow->capacity; index++)
			if ((workflow->objects[index].flags &
					(LOCAL_READY | LOCAL_GENERATED)) ==
					(LOCAL_READY | LOCAL_GENERATED))
				workflow->objects[index].flags |= LOCAL_DIRTY;
	}
}

static int socket_connect(const char *host, uint16_t port)
{
	char service[8];
	snprintf(service, sizeof(service), "%u", port);
	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_family = AF_UNSPEC;
	struct addrinfo *addresses = 0;
	if (getaddrinfo(host, service, &hints, &addresses))
		return -1;
	int fd = -1;
	for (struct addrinfo *address = addresses; address;
			address = address->ai_next) {
		fd = socket(address->ai_family, address->ai_socktype | SOCK_CLOEXEC,
				address->ai_protocol);
		if (fd >= 0 && connect(fd, address->ai_addr, address->ai_addrlen) == 0)
			break;
		if (fd >= 0)
			close(fd);
		fd = -1;
	}
	freeaddrinfo(addresses);
	if (fd >= 0) {
		struct timeval timeout = {.tv_sec = 2};
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
	}
	return fd;
}

static int rpc_exchange(struct agent_workflow *workflow, uint16_t opcode,
		const void *payload, uint32_t payload_size, unsigned char **body,
		uint32_t *body_size)
{
	unsigned char header[VINE_DATAVINE_RPC_REQUEST_HEADER];
	unsigned char response[VINE_DATAVINE_RPC_RESPONSE_HEADER];
	uint64_t request = workflow->request_id++;
	vine_datavine_put_u32(header, VINE_DATAVINE_RPC_MAGIC);
	vine_datavine_put_u16(header + 4, VINE_DATAVINE_RPC_VERSION);
	vine_datavine_put_u16(header + 6, opcode);
	vine_datavine_put_u32(header + 8, payload_size);
	vine_datavine_put_u64(header + 12, request);
	if (full_write(workflow->fd, header, sizeof(header)) != sizeof(header) ||
			(payload_size && full_write(workflow->fd, payload, payload_size) !=
					(ssize_t)payload_size) ||
			full_read(workflow->fd, response, sizeof(response)) != sizeof(response) ||
			vine_datavine_get_u32(response) != VINE_DATAVINE_RPC_MAGIC ||
			vine_datavine_get_u16(response + 4) != VINE_DATAVINE_RPC_VERSION ||
			vine_datavine_get_u16(response + 6) != opcode ||
			vine_datavine_get_u64(response + 16) != request ||
			vine_datavine_get_u32(response + 8) != VINE_DATAVINE_RPC_OK) {
		connection_close(workflow);
		return 0;
	}
	uint32_t size = vine_datavine_get_u32(response + 12);
	if (size > VINE_DATAVINE_RPC_MAX_PAYLOAD) {
		connection_close(workflow);
		return 0;
	}
	unsigned char *result = size ? malloc(size) : 0;
	if (size && (!result || full_read(workflow->fd, result, size) != size)) {
		free(result);
		connection_close(workflow);
		return 0;
	}
	*body = result;
	*body_size = size;
	return 1;
}

static int connection_open(struct agent_workflow *workflow)
{
	if (workflow->fd >= 0)
		return 1;
	workflow->fd = socket_connect(workflow->controller_host,
			workflow->controller_port);
	if (workflow->fd < 0)
		return 0;
	unsigned char hello[VINE_DATAVINE_AGENT_HELLO_SIZE] =
			VINE_DATAVINE_AGENT_HELLO_MAGIC;
	memcpy(hello + 4, workflow->workflow_key, 32);
	vine_datavine_put_u32(hello + 36, workflow->worker_slot);
	vine_datavine_put_u64(hello + 40, workflow->session_epoch);
	vine_datavine_put_u16(hello + 48, agent_transfer_port);
	size_t host_size = strlen(agent_transfer_host);
	vine_datavine_put_u16(hello + 50, (uint16_t)host_size);
	memcpy(hello + 52, agent_transfer_host, host_size);
	unsigned char *reply = 0;
	uint32_t reply_size = 0;
	int valid = rpc_exchange(workflow, VINE_DATAVINE_RPC_AGENT_HELLO,
			hello, sizeof(hello), &reply, &reply_size) && reply_size == 16 &&
			vine_datavine_get_u64(reply) == workflow->workflow_slot &&
			vine_datavine_get_u32(reply + 8);
	if (valid)
		workflow->worker_slot = vine_datavine_get_u32(reply + 8);
	free(reply);
	if (!valid)
		connection_close(workflow);
	return valid;
}

static void batch_header(unsigned char *header,
		const struct agent_workflow *workflow, uint64_t sequence,
		uint32_t count)
{
	memset(header, 0, VINE_DATAVINE_AGENT_BATCH_HEADER);
	vine_datavine_put_u64(header, workflow->workflow_slot);
	vine_datavine_put_u32(header + 8, workflow->worker_slot);
	vine_datavine_put_u64(header + 16, workflow->session_epoch);
	vine_datavine_put_u64(header + 24, sequence);
	vine_datavine_put_u32(header + 32, count);
}

static int cache_name(char name[128], uint64_t workflow_slot,
		uint64_t data_id, uint32_t generation, uint64_t object_token)
{
	return snprintf(name, 128, "datavine-v2-%016llx-%016llx-%08x-%016llx",
			(unsigned long long)workflow_slot,
			(unsigned long long)data_id, generation,
			(unsigned long long)object_token) < 128;
}

static int hash_file(const char *path, uint64_t *size,
		unsigned char digest[32])
{
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	struct stat info;
	EVP_MD_CTX *context = fd >= 0 && fstat(fd, &info) == 0 &&
			S_ISREG(info.st_mode) ? EVP_MD_CTX_new() : 0;
	int valid = context && EVP_DigestInit_ex(context, EVP_sha256(), 0) == 1;
	unsigned char buffer[1 << 16];
	while (valid) {
		ssize_t count = read(fd, buffer, sizeof(buffer));
		if (count > 0)
			valid = EVP_DigestUpdate(context, buffer, (size_t)count) == 1;
		else if (!count)
			break;
		else if (errno != EINTR)
			valid = 0;
	}
	unsigned int digest_size = 0;
	valid = valid && EVP_DigestFinal_ex(context, digest, &digest_size) == 1 &&
			digest_size == 32;
	if (valid)
		*size = (uint64_t)info.st_size;
	EVP_MD_CTX_free(context);
	if (fd >= 0)
		close(fd);
	return valid;
}

static int persistence_enqueue(struct agent_workflow *workflow,
		struct local_object *object, const char *path, size_t path_size)
{
	for (struct persistence_job *job = persistence_head; job; job = job->next)
		if (job->workflow == workflow && job->object == object)
			return 1;
	struct persistence_job *job = calloc(1, sizeof(*job));
	if (!job)
		return 0;
	job->path = malloc(path_size + 1);
	if (!job->path) {
		free(job);
		return 0;
	}
	memcpy(job->path, path, path_size);
	job->path[path_size] = 0;
	job->workflow = workflow;
	job->object = object;
	if (persistence_tail)
		persistence_tail->next = job;
	else
		persistence_head = job;
	persistence_tail = job;
	return 1;
}

static int persistence_copy(struct persistence_job *job)
{
	char cache[128];
	if (!cache_name(cache, job->workflow->workflow_slot,
			job->object->data_id, job->object->generation,
			job->object->object_token))
		return 0;
	char *source_path = vine_cache_data_path(agent_cache, cache);
	char temporary[4096];
	if (!source_path || snprintf(temporary, sizeof(temporary),
			"%s.part.%ld.%llu", job->path, (long)getpid(),
			(unsigned long long)job->object->object_token) >=
				(int)sizeof(temporary)) {
		free(source_path);
		return 0;
	}
	int input = open(source_path, O_RDONLY | O_CLOEXEC);
	int output = input >= 0 ? open(temporary,
			O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600) : -1;
	int valid = output >= 0;
	unsigned char buffer[1 << 16];
	while (valid) {
		ssize_t count = read(input, buffer, sizeof(buffer));
		if (count > 0)
			valid = full_write(output, buffer, (size_t)count) == count;
		else if (!count)
			break;
		else if (errno != EINTR)
			valid = 0;
	}
	valid = valid && fsync(output) == 0;
	if (input >= 0)
		close(input);
	if (output >= 0 && close(output))
		valid = 0;
	if (valid && rename(temporary, job->path))
		valid = 0;
	if (!valid)
		unlink(temporary);
	free(source_path);
	return valid;
}

static int persistence_notify(struct persistence_job *job)
{
	if (!connection_open(job->workflow))
		return 0;
	unsigned char payload[56] = {0};
	vine_datavine_put_u64(payload, job->object->data_id);
	vine_datavine_put_u32(payload + 8, job->object->generation);
	vine_datavine_put_u64(payload + 16, job->object->size);
	memcpy(payload + 24, job->object->digest, 32);
	unsigned char *reply = 0;
	uint32_t reply_size = 0;
	int valid = rpc_exchange(job->workflow,
			VINE_DATAVINE_RPC_AGENT_PERSISTED, payload, sizeof(payload),
			&reply, &reply_size) && !reply_size;
	free(reply);
	return valid;
}

static void persistence_progress(void)
{
	struct persistence_job *job = persistence_head;
	if (!job || (job->object->flags & LOCAL_DIRTY))
		return;
	uint64_t size = 0;
	unsigned char digest[32];
	int copied = hash_file(job->path, &size, digest) &&
			size == job->object->size &&
			!memcmp(digest, job->object->digest, 32);
	if (!copied)
		copied = persistence_copy(job);
	if (!copied || !persistence_notify(job))
		return;
	persistence_head = job->next;
	if (!persistence_head)
		persistence_tail = 0;
	free(job->path);
	free(job);
}

static int sandbox_link(struct vine_process *process, uint64_t data_id,
		const char *cache_path)
{
	char directory[4096];
	char target[4096];
	if (snprintf(directory, sizeof(directory), "%s/datavine/data",
			process->sandbox) >= (int)sizeof(directory) ||
			snprintf(target, sizeof(target), "%s/%llu", directory,
					(unsigned long long)data_id) >= (int)sizeof(target) ||
			!create_dir(directory, 0700))
		return 0;
	if (!link(cache_path, target) || errno == EEXIST)
		return 1;
	return symlink(cache_path, target) == 0;
}

static int sandbox_copy(struct vine_process *process, uint64_t data_id,
		const char *source)
{
	char directory[4096];
	char target[4096];
	char temporary[4096];
	if (snprintf(directory, sizeof(directory), "%s/datavine/data",
			process->sandbox) >= (int)sizeof(directory) ||
			snprintf(target, sizeof(target), "%s/%llu", directory,
					(unsigned long long)data_id) >= (int)sizeof(target) ||
			snprintf(temporary, sizeof(temporary), "%s.part.%ld", target,
					(long)getpid()) >= (int)sizeof(temporary)) {
		agent_diagnostic_failure("construct-path", data_id, source,
				ENAMETOOLONG);
		return 0;
	}
	if (!create_dir(directory, 0700)) {
		int error = errno;
		agent_diagnostic_failure("create-sandbox", data_id, directory, error);
		return 0;
	}
	if (!access(target, R_OK))
		return 2;
	int input = open(source, O_RDONLY | O_CLOEXEC);
	if (input < 0) {
		int error = errno;
		agent_diagnostic_failure("open-source", data_id, source, error);
		return 0;
	}
	int output = open(temporary,
			O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
	if (output < 0) {
		int error = errno;
		close(input);
		agent_diagnostic_failure("open-sandbox", data_id, temporary, error);
		return 0;
	}
	int valid = output >= 0;
	int error = 0;
	unsigned char buffer[1 << 16];
	while (valid) {
		ssize_t count = read(input, buffer, sizeof(buffer));
		if (!count)
			break;
		if (count < 0 && errno == EINTR)
			continue;
		if (count < 0) {
			error = errno;
			valid = 0;
			break;
		}
		if (full_write(output, buffer, (size_t)count) != count) {
			error = errno ? errno : EIO;
			valid = 0;
		}
	}
	if (close(input)) {
		error = errno;
		valid = 0;
	}
	if (close(output)) {
		error = errno;
		valid = 0;
	}
	if (valid && rename(temporary, target)) {
		error = errno;
		valid = 0;
	}
	if (!valid)
		unlink(temporary);
	if (!valid)
		agent_diagnostic_failure("copy-source", data_id, source,
				error ? error : EIO);
	return valid ? 1 : 0;
}

static int safe_output_name(const char *name, size_t length)
{
	if (!length || name[0] == '/' || name[length - 1] == '/' ||
			memchr(name, 0, length))
		return 0;
	size_t component = 0;
	for (size_t index = 0; index <= length; index++) {
		if (index == length || name[index] == '/') {
			if ((index - component == 1 && name[component] == '.') ||
					(index - component == 2 && name[component] == '.' &&
					 name[component + 1] == '.'))
				return 0;
			component = index + 1;
		}
	}
	return 1;
}

static int resolve_one(struct agent_workflow *workflow, uint64_t data_id,
		uint32_t generation, struct local_object *local)
{
	if (local->fetch_retry_after > timestamp_get())
		return 2;
	if (!connection_open(workflow))
		return 2;
	unsigned char request[VINE_DATAVINE_AGENT_BATCH_HEADER +
			VINE_DATAVINE_AGENT_RESOLVE_RECORD];
	batch_header(request, workflow, 0, 1);
	vine_datavine_put_u64(request + VINE_DATAVINE_AGENT_BATCH_HEADER, data_id);
	vine_datavine_put_u32(request + VINE_DATAVINE_AGENT_BATCH_HEADER + 8,
			generation);
	vine_datavine_put_u32(request + VINE_DATAVINE_AGENT_BATCH_HEADER + 12, 0);
	unsigned char *reply = 0;
	uint32_t reply_size = 0;
	if (!rpc_exchange(workflow, VINE_DATAVINE_RPC_AGENT_RESOLVE,
			request, sizeof(request), &reply, &reply_size) ||
			reply_size != VINE_DATAVINE_AGENT_RESOLVE_REPLY) {
		free(reply);
		return 0;
	}
	uint32_t status = vine_datavine_get_u32(reply);
	if (status != VINE_DATAVINE_AGENT_WIRE_AVAILABLE) {
		free(reply);
		return status == VINE_DATAVINE_AGENT_WIRE_PENDING ? 2 : 0;
	}
	uint16_t port = vine_datavine_get_u16(reply + 80);
	uint16_t host_size = vine_datavine_get_u16(reply + 82);
	if (!port || !host_size || host_size >= VINE_DATAVINE_AGENT_HOST_MAX) {
		free(reply);
		return 0;
	}
	char host[VINE_DATAVINE_AGENT_HOST_MAX];
	memcpy(host, reply + 84, host_size);
	host[host_size] = 0;
	local->generation = vine_datavine_get_u32(reply + 4);
	local->size = vine_datavine_get_u64(reply + 16);
	memcpy(local->digest, reply + 24, 32);
	local->source_worker_slot = vine_datavine_get_u32(reply + 56);
	local->source_session_epoch = vine_datavine_get_u64(reply + 64);
	local->object_token = vine_datavine_get_u64(reply + 72);
	char name[128];
	char source[512];
	if (!local->generation || !local->source_worker_slot ||
			!local->source_session_epoch || !local->object_token ||
			!cache_name(name, workflow->workflow_slot, data_id,
					local->generation, local->object_token) ||
			snprintf(source, sizeof(source), "worker://%s:%u/%s", host,
					port, name) >= (int)sizeof(source) ||
			!vine_cache_add_transfer(agent_cache, name, source,
					VINE_CACHE_LEVEL_WORKFLOW, 0600, local->size,
					VINE_CACHE_FLAGS_ON_TASK)) {
		free(reply);
		return 0;
	}
	local->flags = LOCAL_FETCHING | LOCAL_GENERATED;
	free(reply);
	return 2;
}

static int transfer_source_reported_fault(const char *name)
{
	char *path = vine_cache_error_path(agent_cache, name);
	char *message = 0;
	size_t size = 0;
	int exact = path && copy_file_to_buffer(path, &message, &size) > 0 &&
			message && strstr(message, "Remote worker reported error for '");
	if (exact && agent_diagnostic_failures++ < 32)
		debug(D_NOTICE, "DataVine Worker Agent exact source fault %s: %s",
				name, message);
	if (path)
		unlink(path);
	free(message);
	free(path);
	return exact;
}

static void clear_transfer_tuple(struct local_object *local)
{
	local->generation = 0;
	local->size = 0;
	local->object_token = 0;
	local->source_worker_slot = 0;
	local->source_session_epoch = 0;
	memset(local->digest, 0, sizeof(local->digest));
	local->flags = 0;
}

static void report_fault(struct agent_workflow *workflow,
		struct local_object *local, uint32_t flags)
{
	if (!workflow || !local || !local->data_id || !local->generation ||
			!local->object_token || !connection_open(workflow))
		return;
	unsigned char payload[VINE_DATAVINE_AGENT_BATCH_HEADER +
			VINE_DATAVINE_AGENT_FAULT_RECORD];
	uint64_t sequence = workflow->sequence + 1;
	batch_header(payload, workflow, sequence, 1);
	unsigned char *record = payload + VINE_DATAVINE_AGENT_BATCH_HEADER;
	memset(record, 0, VINE_DATAVINE_AGENT_FAULT_RECORD);
	vine_datavine_put_u64(record, local->data_id);
	vine_datavine_put_u32(record + 8, local->generation);
	vine_datavine_put_u32(record + 12, flags);
	vine_datavine_put_u64(record + 16, local->object_token);
	if (flags == VINE_DATAVINE_AGENT_FAULT_REMOTE) {
		vine_datavine_put_u32(record + 24, local->source_worker_slot);
		vine_datavine_put_u64(record + 32, local->source_session_epoch);
	}
	unsigned char *reply = 0;
	uint32_t reply_size = 0;
	if (rpc_exchange(workflow, VINE_DATAVINE_RPC_AGENT_DATA_FAULT,
			payload, sizeof(payload), &reply, &reply_size) && !reply_size)
		workflow->sequence = sequence;
	free(reply);
}

static void forget_object(struct agent_workflow *workflow,
		struct local_object *local, const char *name, uint32_t fault_flags)
{
	report_fault(workflow, local, fault_flags);
	vine_cache_remove(agent_cache, name, 0);
	clear_transfer_tuple(local);
	local->fetch_failures = 0;
	local->fetch_retry_after = 0;
}

static int fetching_ready(struct agent_workflow *workflow,
		struct local_object *local)
{
	char name[128];
	if (!cache_name(name, workflow->workflow_slot, local->data_id,
			local->generation, local->object_token))
		return -1;
	vine_cache_status_t status = vine_cache_ensure(agent_cache, name);
	if (status == VINE_CACHE_STATUS_PENDING ||
			status == VINE_CACHE_STATUS_PROCESSING ||
			status == VINE_CACHE_STATUS_TRANSFERRED)
		return 0;
	if (status != VINE_CACHE_STATUS_READY) {
		/* A remote transfer-protocol error proves that the named source tuple
		 * could not serve its object. A refusal, timeout, or truncated connection
		 * proves only transport pressure: keep Controller truth unchanged and
		 * retry locally with bounded exponential backoff. Worker disconnects are
		 * removed independently by Controller session liveness. */
		if (transfer_source_reported_fault(name)) {
			forget_object(workflow, local, name,
					VINE_DATAVINE_AGENT_FAULT_REMOTE);
			return 0;
		}
		if (local->fetch_failures < 31)
			local->fetch_failures++;
		uint64_t delay = LOCAL_PEER_RETRY_MIN_US;
		unsigned int shifts = local->fetch_failures > 1
				? local->fetch_failures - 1 : 0;
		if (shifts > 4)
			shifts = 4;
		delay <<= shifts;
		if (delay > LOCAL_PEER_RETRY_MAX_US)
			delay = LOCAL_PEER_RETRY_MAX_US;
		vine_cache_remove(agent_cache, name, 0);
		clear_transfer_tuple(local);
		local->fetch_retry_after = timestamp_get() + delay;
		return 0;
	}
	char *path = vine_cache_data_path(agent_cache, name);
	uint64_t size = 0;
	unsigned char digest[32];
	int valid = path && hash_file(path, &size, digest) && size == local->size &&
			!memcmp(digest, local->digest, 32);
	free(path);
	if (!valid) {
		forget_object(workflow, local, name,
				VINE_DATAVINE_AGENT_FAULT_REMOTE);
		return 0;
	}
	local->flags = LOCAL_READY | LOCAL_DIRTY | LOCAL_GENERATED;
	local->source_worker_slot = 0;
	local->source_session_epoch = 0;
	local->fetch_failures = 0;
	local->fetch_retry_after = 0;
	return 1;
}

static int prepare_generated(struct vine_process *process,
		struct agent_workflow *workflow, uint64_t data_id, uint32_t generation)
{
	struct local_object *local = local_get(workflow, data_id, 1);
	if (!local)
		return -1;
	if ((local->flags & LOCAL_READY) &&
			(!generation || local->generation == generation)) {
		char name[128];
		if (!cache_name(name, workflow->workflow_slot, data_id,
					local->generation, local->object_token))
			return -1;
		char *path = vine_cache_data_path(agent_cache, name);
		int valid = path && sandbox_link(process, data_id, path);
		free(path);
		if (valid)
			return 1;
		/* Local eviction, ENOSPC fallout, or an unexpected unlink is the same
		 * precise replica fault as a failed peer pull. */
		forget_object(workflow, local, name, 0);
		return 0;
	}
	if (local->flags & LOCAL_FETCHING) {
		int status = fetching_ready(workflow, local);
		if (status <= 0)
			return status;
		return prepare_generated(process, workflow, data_id, generation);
	}
	return resolve_one(workflow, data_id, generation, local) == 2 ? 0 : -1;
}

static int prepare_local_file(struct vine_process *process, uint64_t data_id,
		const char *uri, size_t uri_size)
{
	/* A SharedFS source needs a local sequential stage before the task's random
	 * reads, but not a cache record or one curl process per file. Copy it
	 * atomically into the sandbox in this Worker Data Agent process. URI identity
	 * is sufficient; correctness must not depend on a separate consumer hint. */
	if (uri_size < 8 || memcmp(uri, "file:///", 8) ||
			memchr(uri, '%', uri_size))
		return -1;
	size_t path_size = uri_size - 7;
	char path[4096];
	if (path_size >= sizeof(path))
		return -1;
	memcpy(path, uri + 7, path_size);
	path[path_size] = 0;
	int copied = sandbox_copy(process, data_id, path);
	/* Distinguish a fresh copy so the caller can yield after its time budget
	 * while still grouping fast small files into one event-loop turn. */
	return copied == 2 ? 1 : copied == 1 ? 2 : -1;
}

static int prepare_uri(struct vine_process *process,
		struct agent_workflow *workflow, uint64_t data_id,
		const char *uri, size_t uri_size)
{
	struct local_object *local = local_get(workflow, data_id, 1);
	if (!local)
		return -1;
	/* A requested intermediate may be represented by its durable URI after
	 * Controller admission even though this worker already owns the identical
	 * generated object.  Reuse the admitted local identity before assigning the
	 * deterministic token used for a newly fetched origin. */
	if (local->flags & LOCAL_READY) {
		char ready_name[128];
		if (!cache_name(ready_name, workflow->workflow_slot, data_id,
				local->generation, local->object_token))
			return -1;
		char *ready_path = vine_cache_data_path(agent_cache, ready_name);
		int linked = ready_path && sandbox_link(process, data_id, ready_path);
		free(ready_path);
		return linked ? 1 : -1;
	}
	local->generation = 1;
	local->object_token = mix64(workflow->workflow_slot ^ data_id);
	if (!local->object_token)
		local->object_token = 1;
	char name[128];
	if (!cache_name(name, workflow->workflow_slot, data_id, 1,
			local->object_token))
		return -1;
	if (!(local->flags & (LOCAL_READY | LOCAL_FETCHING))) {
		char *source = malloc(uri_size + 1);
		if (!source)
			return -1;
		memcpy(source, uri, uri_size);
		source[uri_size] = 0;
		int added = vine_cache_add_transfer(agent_cache, name, source,
				VINE_CACHE_LEVEL_WORKFLOW, 0600, 0,
				VINE_CACHE_FLAGS_ON_TASK);
		free(source);
		if (!added)
			return -1;
		local->flags = LOCAL_FETCHING;
	}
	if (local->flags & LOCAL_FETCHING) {
		vine_cache_status_t status = vine_cache_ensure(agent_cache, name);
		if (status == VINE_CACHE_STATUS_FAILED ||
				status == VINE_CACHE_STATUS_UNKNOWN)
			return -1;
		if (status != VINE_CACHE_STATUS_READY)
			return 0;
		local->flags = LOCAL_READY;
	}
	char *path = vine_cache_data_path(agent_cache, name);
	int valid = path && sandbox_link(process, data_id, path);
	free(path);
	return valid ? 1 : -1;
}

int vine_datavine_agent_initialize(struct vine_cache *cache,
		const char *transfer_host, uint16_t transfer_port)
{
	if (!cache || !transfer_host || !transfer_host[0] ||
			strlen(transfer_host) >= sizeof(agent_transfer_host) ||
			!transfer_port)
		return 0;
	agent_cache = cache;
	snprintf(agent_transfer_host, sizeof(agent_transfer_host), "%s",
			transfer_host);
	agent_transfer_port = transfer_port;
	return 1;
}

enum vine_datavine_agent_prepare_status vine_datavine_agent_prepare(
		struct vine_process *process)
{
	struct task_spec spec;
	int parsed = spec_parse(process, &spec);
	if (!parsed)
		return VINE_DATAVINE_AGENT_NOT_TASK;
	if (parsed < 0) {
		agent_diagnostic_failure("parse-task-spec", 0, 0, 0);
		return VINE_DATAVINE_AGENT_FAILED;
	}
	if (!agent_cache) {
		agent_diagnostic_failure("initialize-cache", 0, 0, 0);
		return VINE_DATAVINE_AGENT_FAILED;
	}
	struct agent_workflow *workflow = workflow_get(&spec);
	if (!workflow) {
		agent_diagnostic_failure("open-workflow", 0, spec.controller_host, 0);
		return VINE_DATAVINE_AGENT_FAILED;
	}
	timestamp_t prepare_started = timestamp_get();
	for (uint32_t index = 0; index < spec.input_count; index++) {
		const unsigned char *record = spec.inputs +
				(size_t)index * VINE_DATAVINE_TASK_SPEC_INPUT;
		uint64_t data_id = vine_datavine_get_u64(record);
		uint32_t generation = vine_datavine_get_u32(record + 8);
		uint32_t kind = vine_datavine_get_u32(record + 12);
		uint32_t offset = vine_datavine_get_u32(record + 16);
		uint32_t length = vine_datavine_get_u32(record + 20);
		int ready = -1;
		if (!data_id) {
			agent_diagnostic_failure("empty-data-id", 0, 0, 0);
			return VINE_DATAVINE_AGENT_FAILED;
		}
		if (kind == VINE_DATAVINE_TASK_INPUT_GENERATED && !offset && !length) {
			ready = prepare_generated(process, workflow, data_id, generation);
		} else if (kind == VINE_DATAVINE_TASK_INPUT_LOCAL_FILE) {
			const char *uri = 0;
			if (string_field(&spec, offset, length, &uri))
				ready = prepare_local_file(process, data_id, uri, length);
		} else if (kind == VINE_DATAVINE_TASK_INPUT_URI ||
				kind == VINE_DATAVINE_TASK_INPUT_URI_EPHEMERAL) {
			const char *uri = 0;
			if (string_field(&spec, offset, length, &uri))
				ready = prepare_uri(process, workflow, data_id, uri, length);
		}
		if (ready < 0) {
			agent_diagnostic_failure("prepare-input", data_id, 0, 0);
			return VINE_DATAVINE_AGENT_FAILED;
		}
		if (!ready) {
			agent_waiting_data = 1;
			return VINE_DATAVINE_AGENT_WAIT;
		}
		if (ready == 2 &&
				timestamp_get() - prepare_started >= LOCAL_PREPARE_BUDGET_US) {
			agent_waiting_data = 1;
			return VINE_DATAVINE_AGENT_WAIT;
		}
	}
	return VINE_DATAVINE_AGENT_READY;
}

enum vine_datavine_agent_commit_status vine_datavine_agent_commit(
		struct vine_process *process)
{
	struct task_spec spec;
	if (spec_parse(process, &spec) != 1)
		return process && process->task &&
			!process->task->auxiliary_payload_length;
	struct agent_workflow *workflow = workflow_get(&spec);
	if (!workflow)
		return VINE_DATAVINE_AGENT_COMMIT_IO_FAILED;
	for (uint32_t index = 0; index < spec.output_count; index++) {
		const unsigned char *record = spec.outputs +
				(size_t)index * VINE_DATAVINE_TASK_SPEC_OUTPUT;
		uint64_t data_id = vine_datavine_get_u64(record);
		uint32_t generation = vine_datavine_get_u32(record + 8);
		uint32_t flags = vine_datavine_get_u32(record + 12);
		uint32_t offset = vine_datavine_get_u32(record + 16);
		uint32_t length = vine_datavine_get_u32(record + 20);
		uint32_t durable_offset = vine_datavine_get_u32(record + 24);
		uint32_t durable_length = vine_datavine_get_u32(record + 28);
		const char *name = 0;
		if (!data_id || !generation ||
				flags & ~(VINE_DATAVINE_TASK_OUTPUT_RETAIN |
					VINE_DATAVINE_TASK_OUTPUT_REQUESTED) ||
				!string_field(&spec, offset, length, &name) ||
				!safe_output_name(name, length))
			return 0;
		if (!(flags & VINE_DATAVINE_TASK_OUTPUT_RETAIN))
			continue;
		const char *durable_path = 0;
		if ((flags & VINE_DATAVINE_TASK_OUTPUT_REQUESTED) &&
				!string_field(&spec, durable_offset, durable_length,
						&durable_path))
			return 0;
		if (!(flags & VINE_DATAVINE_TASK_OUTPUT_REQUESTED) &&
				(durable_offset || durable_length))
			return 0;
		char relative[4096];
		char path[4096];
		if (length >= sizeof(relative))
			return 0;
		memcpy(relative, name, length);
		relative[length] = 0;
		if (snprintf(path, sizeof(path), "%s/%s", process->sandbox,
				relative) >= (int)sizeof(path))
			return 0;
		uint64_t size = 0;
		unsigned char digest[32];
		errno = 0;
		if (!hash_file(path, &size, digest)) {
			int error = errno;
			return error == ENOENT
					? VINE_DATAVINE_AGENT_COMMIT_INVALID
					: VINE_DATAVINE_AGENT_COMMIT_IO_FAILED;
		}
		struct local_object *local = local_get(workflow, data_id, 1);
		if (!local)
			return VINE_DATAVINE_AGENT_COMMIT_IO_FAILED;
		if ((local->flags & LOCAL_READY) && local->generation != generation) {
			/* Controller generations are authoritative. Recovery may land on a
			 * Worker that still has an unadmitted older generation after its last
			 * advertised replica was lost. Replace that stale local object; never
			 * let a delayed task overwrite a newer generation. */
			if (local->generation > generation)
				return VINE_DATAVINE_AGENT_COMMIT_INVALID;
			char stale[128];
			if (!cache_name(stale, workflow->workflow_slot, data_id,
					local->generation, local->object_token))
				return VINE_DATAVINE_AGENT_COMMIT_INVALID;
			vine_cache_remove(agent_cache, stale, 0);
			local->flags = 0;
		}
		if (local->flags & LOCAL_READY) {
			if (local->size != size ||
					memcmp(local->digest, digest, 32))
				return 0;
			unlink(path);
			local->flags |= LOCAL_DIRTY;
			if ((flags & VINE_DATAVINE_TASK_OUTPUT_REQUESTED) &&
					!persistence_enqueue(workflow, local, durable_path,
						durable_length))
				return VINE_DATAVINE_AGENT_COMMIT_IO_FAILED;
			continue;
		}
		local->generation = generation;
		local->size = size;
		memcpy(local->digest, digest, 32);
		local->object_token = mix64(workflow->session_epoch ^ data_id ^
				((uint64_t)generation << 32) ^ timestamp_get());
		if (!local->object_token)
			local->object_token = 1;
		char cache[128];
		struct stat info;
		if (!cache_name(cache, workflow->workflow_slot, data_id, generation,
				local->object_token))
			return VINE_DATAVINE_AGENT_COMMIT_INVALID;
		errno = 0;
		if (stat(path, &info))
			return errno == ENOENT ? VINE_DATAVINE_AGENT_COMMIT_INVALID
					: VINE_DATAVINE_AGENT_COMMIT_IO_FAILED;
		if (!vine_cache_add_file(agent_cache, cache, path,
					VINE_CACHE_LEVEL_WORKFLOW, info.st_mode & 0777, size,
					info.st_mtime, process->execution_start,
					timestamp_get() - process->execution_start, 0))
			return VINE_DATAVINE_AGENT_COMMIT_IO_FAILED;
		local->flags = LOCAL_READY | LOCAL_DIRTY | LOCAL_GENERATED |
				((flags & VINE_DATAVINE_TASK_OUTPUT_REQUESTED) ?
				 LOCAL_REQUESTED : 0);
		if ((flags & VINE_DATAVINE_TASK_OUTPUT_REQUESTED) &&
				!persistence_enqueue(workflow, local, durable_path,
					durable_length))
			return VINE_DATAVINE_AGENT_COMMIT_IO_FAILED;
	}
	/* Unique immutable sources are task-scoped. Removing the cache base is
	 * safe here because the sandbox hardlink remains pinned until normal task
	 * cleanup; shared origins and all generated data use Controller lifecycle. */
	for (uint32_t index = 0; index < spec.input_count; index++) {
		const unsigned char *record = spec.inputs +
				(size_t)index * VINE_DATAVINE_TASK_SPEC_INPUT;
		if (vine_datavine_get_u32(record + 12) !=
				VINE_DATAVINE_TASK_INPUT_URI_EPHEMERAL)
			continue;
		uint64_t data_id = vine_datavine_get_u64(record);
		struct local_object *local = local_get(workflow, data_id, 0);
		if (!local || !(local->flags & LOCAL_READY))
			continue;
		char name[128];
		if (cache_name(name, workflow->workflow_slot, data_id,
				local->generation, local->object_token))
			vine_cache_remove(agent_cache, name, 0);
		local->flags = 0;
	}
	return VINE_DATAVINE_AGENT_COMMIT_READY;
}

static void workflow_progress(struct agent_workflow *workflow)
{
	struct local_object *batch[VINE_DATAVINE_AGENT_MAX_BATCH];
	uint32_t count = 0;
	for (size_t index = 0; index < workflow->capacity &&
			count < VINE_DATAVINE_AGENT_MAX_BATCH; index++) {
		if ((workflow->objects[index].flags &
				(LOCAL_READY | LOCAL_DIRTY)) ==
				(LOCAL_READY | LOCAL_DIRTY))
			batch[count++] = &workflow->objects[index];
	}
	if (count && connection_open(workflow)) {
		size_t size = VINE_DATAVINE_AGENT_BATCH_HEADER +
			(size_t)count * VINE_DATAVINE_AGENT_PUBLISH_RECORD;
		unsigned char *payload = malloc(size);
		if (!payload)
			return;
		uint64_t sequence = workflow->sequence + 1;
		batch_header(payload, workflow, sequence, count);
		for (uint32_t index = 0; index < count; index++) {
			unsigned char *record = payload + VINE_DATAVINE_AGENT_BATCH_HEADER +
				(size_t)index * VINE_DATAVINE_AGENT_PUBLISH_RECORD;
			memset(record, 0, VINE_DATAVINE_AGENT_PUBLISH_RECORD);
			vine_datavine_put_u64(record, batch[index]->data_id);
			vine_datavine_put_u32(record + 8, batch[index]->generation);
			vine_datavine_put_u32(record + 12,
				(batch[index]->flags & LOCAL_REQUESTED) ? 1 : 0);
			vine_datavine_put_u64(record + 16, batch[index]->size);
			vine_datavine_put_u64(record + 24, batch[index]->object_token);
			memcpy(record + 32, batch[index]->digest, 32);
		}
		unsigned char *reply = 0;
		uint32_t reply_size = 0;
		if (rpc_exchange(workflow, VINE_DATAVINE_RPC_AGENT_DATA_READY,
			payload, (uint32_t)size, &reply, &reply_size) && !reply_size) {
			workflow->sequence = sequence;
			for (uint32_t index = 0; index < count; index++)
				batch[index]->flags &= ~LOCAL_DIRTY;
		}
		free(reply);
		free(payload);
	}
	timestamp_t now = timestamp_get();
	if (now - workflow->last_heartbeat < 100000 ||
			!connection_open(workflow))
		return;
	unsigned char heartbeat[8];
	vine_datavine_put_u64(heartbeat, workflow->release_acknowledged);
	unsigned char *reply = 0;
	uint32_t reply_size = 0;
	if (!rpc_exchange(workflow, VINE_DATAVINE_RPC_AGENT_HEARTBEAT,
			heartbeat, sizeof(heartbeat), &reply, &reply_size) ||
			reply_size < 16) {
		free(reply);
		return;
	}
	uint32_t releases = vine_datavine_get_u32(reply + 8);
	if (vine_datavine_get_u32(reply + 12) ||
			releases > VINE_DATAVINE_AGENT_MAX_BATCH ||
			reply_size != 16 +
				(size_t)releases * VINE_DATAVINE_AGENT_RELEASE_RECORD) {
		free(reply);
		connection_close(workflow);
		return;
	}
	for (uint32_t index = 0; index < releases; index++) {
		const unsigned char *record = reply + 16 +
				(size_t)index * VINE_DATAVINE_AGENT_RELEASE_RECORD;
		uint64_t data_id = vine_datavine_get_u64(record);
		uint32_t generation = vine_datavine_get_u32(record + 8);
		uint64_t token = vine_datavine_get_u64(record + 16);
		struct local_object *local = local_get(workflow, data_id, 0);
		if (!data_id || !generation || vine_datavine_get_u32(record + 12) ||
				!token)
			continue;
		if (local && local->generation == generation &&
				local->object_token == token) {
			char name[128];
			if (cache_name(name, workflow->workflow_slot, data_id,
					generation, token))
				vine_cache_remove(agent_cache, name, 0);
			local->flags = 0;
		}
	}
	if (releases)
		workflow->release_acknowledged = vine_datavine_get_u64(reply);
	workflow->last_heartbeat = now;
	free(reply);
}

void vine_datavine_agent_progress(void)
{
	for (struct agent_workflow *workflow = agent_workflows; workflow;
			workflow = workflow->next)
		workflow_progress(workflow);
	persistence_progress();
}

int vine_datavine_agent_waiting(void)
{
	int waiting = agent_waiting_data;
	agent_waiting_data = 0;
	return waiting;
}

void vine_datavine_agent_shutdown(void)
{
	while (persistence_head) {
		struct persistence_job *next = persistence_head->next;
		free(persistence_head->path);
		free(persistence_head);
		persistence_head = next;
	}
	persistence_tail = 0;
	while (agent_workflows) {
		struct agent_workflow *next = agent_workflows->next;
		connection_close(agent_workflows);
		free(agent_workflows->objects);
		free(agent_workflows);
		agent_workflows = next;
	}
	agent_cache = 0;
	agent_transfer_host[0] = 0;
	agent_transfer_port = 0;
}
