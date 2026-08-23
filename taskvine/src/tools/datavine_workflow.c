/* Validate and fingerprint a language-neutral DataVine Workflow IR document. */

#include "vine_datavine_workflow.h"
#include "vine_datavine_workflow_store.h"
#include "vine_datavine_rpc.h"
#include "vine_datavine_data_controller.h"

#include "taskvine.h"

#include "copy_stream.h"
#include "b64.h"
#include "buffer.h"
#include "jx.h"
#include "jx_print.h"

#include <errno.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

static volatile sig_atomic_t stopping;

static int sibling_executable(const char *name, char result[PATH_MAX])
{
	ssize_t size = readlink("/proc/self/exe", result, PATH_MAX - 1);
	if (size < 1 || size >= PATH_MAX)
		return 0;
	result[size] = 0;
	char *slash = strrchr(result, '/');
	if (!slash)
		return 0;
	size_t prefix = (size_t)(slash + 1 - result);
	if (strlen(name) >= PATH_MAX - prefix)
		return 0;
	strcpy(slash + 1, name);
	return access(result, X_OK) == 0;
}

static int native_executor_path(char result[PATH_MAX])
{
	const char *override = getenv("DATAVINE_EXECUTOR_PATH");
	return (override && realpath(override, result) && access(result, X_OK) == 0) ||
	       sibling_executable("datavine_executor", result);
}

static int python_executor_path(char result[PATH_MAX])
{
	const char *override = getenv("DATAVINE_PYTHON_EXECUTOR_PATH");
	return (override && realpath(override, result) && access(result, X_OK) == 0) ||
	       sibling_executable("datavine_python_executor", result);
}

struct rpc_client {
	int fd;
	uint64_t request_id;
};

static int transfer_all(int fd, unsigned char *buffer, size_t size, int writing)
{
	while (size) {
		ssize_t count = writing ? send(fd, buffer, size, MSG_NOSIGNAL)
					: recv(fd, buffer, size, 0);
		if (count > 0) {
			buffer += count;
			size -= (size_t)count;
		} else if (count < 0 && errno == EINTR) {
			continue;
		} else {
			return 0;
		}
	}
	return 1;
}

static int rpc_exchange(struct rpc_client *client, uint16_t opcode,
		const unsigned char *payload, size_t payload_size,
		uint32_t *status, unsigned char **body, size_t *body_size)
{
	if (payload_size > UINT32_MAX)
		return 0;
	unsigned char request[VINE_DATAVINE_RPC_REQUEST_HEADER] = {0};
	vine_datavine_put_u32(request, VINE_DATAVINE_RPC_MAGIC);
	request[5] = VINE_DATAVINE_RPC_VERSION;
	request[6] = (unsigned char)(opcode >> 8);
	request[7] = (unsigned char)opcode;
	vine_datavine_put_u32(request + 8, (uint32_t)payload_size);
	vine_datavine_put_u64(request + 12, ++client->request_id);
	if (!transfer_all(client->fd, request, sizeof(request), 1) ||
			(payload_size && !transfer_all(client->fd,
							 (unsigned char *)payload,
							 payload_size,
							 1)))
		return 0;
	unsigned char response[VINE_DATAVINE_RPC_RESPONSE_HEADER];
	if (!transfer_all(client->fd, response, sizeof(response), 0) ||
			vine_datavine_get_u32(response) != VINE_DATAVINE_RPC_MAGIC ||
			response[5] != VINE_DATAVINE_RPC_VERSION ||
			((uint16_t)response[6] << 8 | response[7]) != opcode ||
			vine_datavine_get_u64(response + 16) != client->request_id)
		return 0;
	*status = vine_datavine_get_u32(response + 8);
	*body_size = vine_datavine_get_u32(response + 12);
	if (*body_size > VINE_DATAVINE_RPC_MAX_PAYLOAD)
		return 0;
	*body = *body_size ? malloc(*body_size) : 0;
	return !*body_size || (*body && transfer_all(client->fd, *body, *body_size, 0));
}

static int rpc_open(struct rpc_client *client, const char *endpoint,
		const char *token)
{
	const char *prefix = "tcp://";
	if (strncmp(endpoint, prefix, strlen(prefix)))
		return 0;
	const char *address = endpoint + strlen(prefix);
	const char *colon = strrchr(address, ':');
	if (!colon || colon == address || !colon[1])
		return 0;
	size_t host_size = (size_t)(colon - address);
	if (host_size > 255)
		return 0;
	char host[256];
	memcpy(host, address, host_size);
	host[host_size] = 0;
	char *port_end = 0;
	long port = strtol(colon + 1, &port_end, 10);
	if (*port_end || port < 1 || port > 65535)
		return 0;
	struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
	struct addrinfo *addresses = 0;
	char service[16];
	snprintf(service, sizeof(service), "%ld", port);
	if (getaddrinfo(host, service, &hints, &addresses))
		return 0;
	client->fd = -1;
	for (struct addrinfo *item = addresses; item; item = item->ai_next) {
		client->fd = socket(item->ai_family, item->ai_socktype, item->ai_protocol);
		if (client->fd >= 0 &&
				connect(client->fd, item->ai_addr, item->ai_addrlen) == 0)
			break;
		if (client->fd >= 0)
			close(client->fd);
		client->fd = -1;
	}
	freeaddrinfo(addresses);
	if (client->fd < 0)
		return 0;
	int enabled = 1;
	setsockopt(client->fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
	uint32_t status = 0;
	unsigned char *body = 0;
	size_t body_size = 0;
	int valid = rpc_exchange(client, VINE_DATAVINE_RPC_AUTH, (const unsigned char *)token, strlen(token), &status, &body, &body_size) &&
		    status == VINE_DATAVINE_RPC_OK;
	free(body);
	if (!valid) {
		close(client->fd);
		client->fd = -1;
	}
	return valid;
}

static void stop_service(int signal_number)
{
	(void)signal_number;
	stopping = 1;
}

static const char *workflow_state(uint32_t state)
{
	switch (state) {
	case VINE_DATAVINE_WORKFLOW_OPEN:
		return "open";
	case VINE_DATAVINE_WORKFLOW_SEALED:
		return "sealed";
	case VINE_DATAVINE_WORKFLOW_CANCELLED:
		return "cancelled";
	case VINE_DATAVINE_WORKFLOW_RUNNING:
		return "running";
	case VINE_DATAVINE_WORKFLOW_RUNNING_OPEN:
		return "running_open";
	case VINE_DATAVINE_WORKFLOW_OPEN_QUIESCENT:
		return "open_quiescent";
	case VINE_DATAVINE_WORKFLOW_COMPLETED:
		return "completed";
	case VINE_DATAVINE_WORKFLOW_FAILED:
		return "failed";
	default:
		return "unknown";
	}
}

static int print_workflow_info(const unsigned char *body, size_t size)
{
	if (size < 104 || memcmp(body, "DWI1", 4))
		return 0;
	uint16_t id_size = (uint16_t)((body[60] << 8) | body[61]);
	if (size != 104U + id_size)
		return 0;
	char workflow_id[VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX + 1];
	if (id_size > VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX)
		return 0;
	memcpy(workflow_id, body + 104, id_size);
	workflow_id[id_size] = 0;
	char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1];
	memcpy(digest, body + 64, VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH);
	digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH] = 0;
	struct jx *result = jx_objectv(
			"workflow_id", jx_string(workflow_id), "digest", jx_string(digest), "generation", jx_integer(vine_datavine_get_u64(body + 8)), "event_id", jx_integer(vine_datavine_get_u64(body + 16)), "state", jx_string(workflow_state(vine_datavine_get_u32(body + 4))), "tasks", jx_integer(vine_datavine_get_u64(body + 24)), "data", jx_integer(vine_datavine_get_u64(body + 32)), "edges", jx_integer(vine_datavine_get_u64(body + 40)), "requested_outputs", jx_integer(vine_datavine_get_u64(body + 48)), "streaming", jx_boolean(vine_datavine_get_u32(body + 56)), NULL);
	if (!result)
		return 0;
	jx_print_stream(result, stdout);
	printf("\n");
	jx_delete(result);
	return 1;
}

static void print_rpc_error(uint32_t status, const unsigned char *body, size_t size)
{
	if (size >= 8) {
		uint16_t path_size = (uint16_t)((body[4] << 8) | body[5]);
		uint16_t message_size = (uint16_t)((body[6] << 8) | body[7]);
		if (size == 8U + path_size + message_size) {
			fprintf(stderr, "datavine workflow: RPC status %u at %.*s: %.*s\n", status, path_size, body + 8, message_size, body + 8 + path_size);
			return;
		}
	}
	fprintf(stderr, "datavine workflow: RPC status %u\n", status);
}

static int read_document(const char *path, char **document, size_t *size)
{
	return (!strcmp(path, "-")
					       ? copy_stream_to_buffer(stdin, document, size)
					       : copy_file_to_buffer(path, document, size)) >= 0;
}

static unsigned char *identifier_payload(const char *workflow_id,
		size_t header_size, size_t suffix_size, size_t *payload_size)
{
	size_t id_size = strlen(workflow_id);
	if (!id_size || id_size > VINE_DATAVINE_WORKFLOW_IDENTIFIER_MAX ||
			header_size > SIZE_MAX - id_size ||
			header_size + id_size > SIZE_MAX - suffix_size)
		return 0;
	*payload_size = header_size + id_size + suffix_size;
	unsigned char *payload = calloc(1, *payload_size);
	if (payload) {
		vine_datavine_put_u16(payload, (uint16_t)id_size);
		memcpy(payload + header_size, workflow_id, id_size);
	}
	return payload;
}

static int workflow_rpc(int argc, char **argv)
{
	const char *operation = argv[1];
	if (argc < 4)
		return 2;
	const char *endpoint = argv[2];
	const char *token = argv[3];
	struct rpc_client client = {.fd = -1};
	if (!rpc_open(&client, endpoint, token)) {
		fprintf(stderr, "datavine workflow: could not connect or authenticate to %s\n", endpoint);
		return 3;
	}
	uint16_t opcode = 0;
	unsigned char *payload = 0;
	size_t payload_size = 0;
	int output_kind = 0;
	if (!strcmp(operation, "capabilities") && argc == 4) {
		opcode = VINE_DATAVINE_RPC_WORKFLOW_CAPABILITIES;
		output_kind = 1;
	} else if (!strcmp(operation, "submit") && argc == 5) {
		char *document = 0;
		if (!read_document(argv[4], &document, &payload_size))
			goto invalid;
		payload = (unsigned char *)document;
		opcode = VINE_DATAVINE_RPC_WORKFLOW_SUBMIT;
		output_kind = 2;
	} else if (!strcmp(operation, "append") && argc == 7) {
		char *end = 0;
		uint64_t generation = strtoull(argv[5], &end, 10);
		char *document = 0;
		size_t document_size = 0;
		if (*end || !read_document(argv[6], &document, &document_size))
			goto invalid;
		payload = identifier_payload(argv[4], 12, document_size, &payload_size);
		if (!payload) {
			free(document);
			goto invalid;
		}
		vine_datavine_put_u64(payload + 4, generation);
		memcpy(payload + payload_size - document_size, document, document_size);
		free(document);
		opcode = VINE_DATAVINE_RPC_WORKFLOW_APPEND;
		output_kind = 2;
	} else if (!strcmp(operation, "seal") && argc == 6) {
		char *end = 0;
		uint64_t generation = strtoull(argv[5], &end, 10);
		if (*end)
			goto invalid;
		payload = identifier_payload(argv[4], 12, 0, &payload_size);
		if (!payload)
			goto invalid;
		vine_datavine_put_u64(payload + 4, generation);
		opcode = VINE_DATAVINE_RPC_WORKFLOW_SEAL;
		output_kind = 2;
	} else if ((!strcmp(operation, "status") || !strcmp(operation, "cancel")) &&
			argc == 5) {
		payload = identifier_payload(argv[4], 2, 0, &payload_size);
		if (!payload)
			goto invalid;
		opcode = !strcmp(operation, "status")
					 ? VINE_DATAVINE_RPC_WORKFLOW_DESCRIBE
					 : VINE_DATAVINE_RPC_WORKFLOW_CANCEL;
		output_kind = 2;
	} else if (!strcmp(operation, "watch") && (argc == 5 || argc == 6)) {
		char *end = 0;
		uint64_t after = argc == 6 ? strtoull(argv[5], &end, 10) : 0;
		if (argc == 6 && *end)
			goto invalid;
		payload = identifier_payload(argv[4], 12, 0, &payload_size);
		if (!payload)
			goto invalid;
		vine_datavine_put_u16(payload + 2, 64);
		vine_datavine_put_u64(payload + 4, after);
		opcode = VINE_DATAVINE_RPC_WORKFLOW_WATCH;
		output_kind = 3;
	} else if ((!strcmp(operation, "result") ||
				   !strcmp(operation, "result-info")) &&
			argc == 6) {
		char *end = 0;
		uint64_t data_id = strtoull(argv[5], &end, 10);
		if (!data_id || *end)
			goto invalid;
		payload = identifier_payload(argv[4], 12, 0, &payload_size);
		if (!payload)
			goto invalid;
		vine_datavine_put_u64(payload + 4, data_id);
		opcode = !strcmp(operation, "result")
					 ? VINE_DATAVINE_RPC_WORKFLOW_FETCH_RESULT
					 : VINE_DATAVINE_RPC_WORKFLOW_RESULT_INFO;
		output_kind = !strcmp(operation, "result") ? 4 : 1;
	} else {
		goto invalid;
	}
	uint32_t status = 0;
	unsigned char *body = 0;
	size_t body_size = 0;
	int exchanged = rpc_exchange(&client, opcode, payload, payload_size, &status, &body, &body_size);
	free(payload);
	close(client.fd);
	if (!exchanged) {
		fprintf(stderr, "datavine workflow: transport failure\n");
		return 3;
	}
	if (status != VINE_DATAVINE_RPC_OK) {
		print_rpc_error(status, body, body_size);
		free(body);
		return 4;
	}
	int valid = 1;
	if (output_kind == 1) {
		fwrite(body, 1, body_size, stdout);
		printf("\n");
	} else if (output_kind == 2) {
		valid = print_workflow_info(body, body_size);
	} else if (output_kind == 3) {
		if (body_size < 4 || body_size != 4 +
								     (size_t)vine_datavine_get_u32(body) * 76) {
			valid = 0;
		} else {
			printf("{\"count\":%u,\"events\":[",
					vine_datavine_get_u32(body));
			for (uint32_t i = 0; i < vine_datavine_get_u32(body); i++) {
				const unsigned char *event = body + 4 + i * 76;
				if (i)
					printf(",");
				printf("{\"event_id\":%llu,\"generation\":%llu,\"type\":%u,"
				       "\"task_id\":%llu,\"attempt\":%u,\"result\":%d}",
						(unsigned long long)vine_datavine_get_u64(event + 4),
						(unsigned long long)vine_datavine_get_u64(event + 12),
						vine_datavine_get_u32(event),
						(unsigned long long)vine_datavine_get_u64(event + 60),
						vine_datavine_get_u32(event + 68),
						(int32_t)vine_datavine_get_u32(event + 72));
			}
			printf("]}\n");
		}
	} else if (output_kind == 4) {
		buffer_t encoded;
		buffer_init(&encoded);
		valid = b64_encode(body, body_size, &encoded) == 0;
		if (valid)
			printf("{\"workflow_id\":\"%s\",\"data_id\":%s,\"base64\":\"%s\"}\n",
					argv[4],
					argv[5],
					buffer_tostring(&encoded));
		buffer_free(&encoded);
	}
	free(body);
	if (!valid)
		fprintf(stderr, "datavine workflow: malformed RPC response\n");
	return valid ? 0 : 3;

invalid:
	free(payload);
	close(client.fd);
	return 2;
}

static void usage(const char *name)
{
	fprintf(stderr,
			"usage: %s validate [workflow.json|-]\n"
			"       %s serve JOURNAL TOKEN [PORT]\n"
			"       %s submit ENDPOINT TOKEN WORKFLOW.json|-\n"
			"       %s append ENDPOINT TOKEN WORKFLOW_ID GENERATION WORKFLOW.json|-\n"
			"       %s seal ENDPOINT TOKEN WORKFLOW_ID GENERATION\n"
			"       %s status|cancel ENDPOINT TOKEN WORKFLOW_ID\n"
			"       %s watch ENDPOINT TOKEN WORKFLOW_ID [AFTER_EVENT_ID]\n"
			"       %s result ENDPOINT TOKEN WORKFLOW_ID DATA_ID\n"
			"       %s result-info ENDPOINT TOKEN WORKFLOW_ID DATA_ID\n"
			"       %s capabilities ENDPOINT TOKEN\n",
			name,
			name,
			name,
			name,
			name,
			name,
			name,
			name,
			name,
			name);
}

static int serve(int argc, char **argv)
{
	if (argc < 4 || argc > 5) {
		usage(argv[0]);
		return 2;
	}
	int port = argc == 5 ? atoi(argv[4]) : 0;
	if (port < 0 || port > 65535) {
		fprintf(stderr, "datavine_workflow: invalid port\n");
		return 2;
	}
	char default_profile[PATH_MAX];
	if (!getenv("DATAVINE_PROFILE_PATH") &&
			!getenv("DATAVINE_WORKFLOW_METRICS")) {
		if (snprintf(default_profile, sizeof(default_profile), "%s.profile", argv[2]) >= (int)sizeof(default_profile) ||
				setenv("DATAVINE_PROFILE_PATH", default_profile, 0) != 0) {
			fprintf(stderr, "datavine_workflow: could not configure profile path\n");
			return 1;
		}
	}
	char advertised_host[256];
	const char *configured_host = getenv("DATAVINE_ADVERTISE_HOST");
	if (configured_host && configured_host[0]) {
		if (snprintf(advertised_host, sizeof(advertised_host), "%s", configured_host) >= (int)sizeof(advertised_host)) {
			fprintf(stderr, "datavine_workflow: advertised host is too long\n");
			return 2;
		}
	} else if (gethostname(advertised_host, sizeof(advertised_host)) != 0) {
		fprintf(stderr, "datavine_workflow: could not determine hostname\n");
		return 1;
	}
	advertised_host[sizeof(advertised_host) - 1] = 0;
	/* Metadata RPC and publication are non-blocking control-plane work.  One
	 * event-loop thread is the default; deployments may opt into more while
	 * the compatibility path is being retired. */
	int service_threads = 1;
	const char *configured_threads = getenv("DATAVINE_SERVICE_THREADS");
	if (configured_threads && configured_threads[0]) {
		char *end = 0;
		long value = strtol(configured_threads, &end, 10);
		if (!end || *end || value < 1 || value > 256) {
			fprintf(stderr, "datavine_workflow: invalid DATAVINE_SERVICE_THREADS\n");
			return 2;
		}
		service_threads = (int)value;
	}
	struct vine_datavine_rpc_server *server = vine_datavine_rpc_server_create(
			"0.0.0.0", port, argv[3], service_threads, argv[2]);
	if (!server) {
		fprintf(stderr, "datavine_workflow: could not start native service\n");
		return 1;
	}
	const char *runtime_info_path = getenv("DATAVINE_RUNTIME_INFO_PATH");
	if (runtime_info_path && runtime_info_path[0])
		vine_set_runtime_info_path(runtime_info_path);
	struct vine_manager *manager = vine_create(-1);
	if (!manager) {
		fprintf(stderr, "datavine_workflow: could not start TaskVine manager\n");
		vine_datavine_rpc_server_delete(server);
		return 1;
	}
	char native_executor[PATH_MAX];
	char python_executor[PATH_MAX];
	if (!native_executor_path(native_executor) ||
			!python_executor_path(python_executor)) {
		fprintf(stderr, "datavine_workflow: could not locate datavine_executor\n");
		vine_delete(manager);
		vine_datavine_rpc_server_delete(server);
		return 1;
	}
	int object_service_ready =
			vine_datavine_data_controller_configure_object_service(
					vine_datavine_rpc_server_data_controller(server),
					advertised_host,
					vine_datavine_rpc_server_port(server),
					argv[3]);
	struct vine_datavine_workflow_runtime *runtime = object_service_ready
									 ? vine_datavine_workflow_runtime_start(
											   vine_datavine_rpc_server_workflow_store(server),
											   vine_datavine_rpc_server_data_controller(server),
											   manager,
											   native_executor,
											   python_executor)
									 : 0;
	if (!runtime) {
		fprintf(stderr, "datavine_workflow: could not start native workflow runtime\n");
		vine_delete(manager);
		vine_datavine_rpc_server_delete(server);
		return 1;
	}
	signal(SIGINT, stop_service);
	signal(SIGTERM, stop_service);
	printf("{\"endpoint\":\"tcp://%s:%d\",\"manager_port\":%d,\"pid\":%ld}\n",
			advertised_host,
			vine_datavine_rpc_server_port(server),
			vine_port(manager),
			(long)getpid());
	fflush(stdout);
	vine_datavine_workflow_runtime_run(runtime, &stopping);
	vine_datavine_workflow_runtime_stop(runtime);
	vine_delete(manager);
	vine_datavine_rpc_server_delete(server);
	return 0;
}

int main(int argc, char **argv)
{
	if (argc >= 2 && !strcmp(argv[1], "workflow")) {
		argv++;
		argc--;
	}
	if (argc >= 2 && !strcmp(argv[1], "serve"))
		return serve(argc, argv);
	if (argc >= 2 && (!strcmp(argv[1], "submit") ||
					 !strcmp(argv[1], "append") || !strcmp(argv[1], "seal") ||
					 !strcmp(argv[1], "status") || !strcmp(argv[1], "watch") ||
					 !strcmp(argv[1], "cancel") || !strcmp(argv[1], "result") ||
					 !strcmp(argv[1], "result-info") ||
					 !strcmp(argv[1], "capabilities"))) {
		int result = workflow_rpc(argc, argv);
		if (result == 2)
			usage(argv[0]);
		return result;
	}
	if (argc < 2 || argc > 3 || strcmp(argv[1], "validate")) {
		usage(argv[0]);
		return 2;
	}
	char *document = 0;
	size_t size = 0;
	const char *path = argc == 3 ? argv[2] : "-";
	int64_t count = !strcmp(path, "-")
					? copy_stream_to_buffer(stdin, &document, &size)
					: copy_file_to_buffer(path, &document, &size);
	if (count < 0) {
		fprintf(stderr, "datavine_workflow: could not read %s\n", path);
		return 2;
	}
	char digest[VINE_DATAVINE_WORKFLOW_DIGEST_LENGTH + 1] = {0};
	struct vine_datavine_workflow_summary summary;
	struct vine_datavine_workflow_error error;
	int valid = vine_datavine_workflow_validate(document, size, digest, &summary, &error);
	free(document);
	struct jx *result;
	if (valid) {
		result = jx_objectv(
				"valid", jx_boolean(1), "schema", jx_string(VINE_DATAVINE_WORKFLOW_SCHEMA_NAME), "digest_algorithm", jx_string("sha1"), "digest", jx_string(digest), "tasks", jx_integer((jx_int_t)summary.tasks), "data", jx_integer((jx_int_t)summary.data), "edges", jx_integer((jx_int_t)summary.edges), "requested_outputs", jx_integer((jx_int_t)summary.requested_outputs), "streaming", jx_boolean(summary.streaming), NULL);
	} else {
		result = jx_objectv(
				"valid", jx_boolean(0), "error", jx_string(vine_datavine_workflow_error_name(error.code)), "path", jx_string(error.path), "message", jx_string(error.message), NULL);
	}
	jx_print_stream(result, stdout);
	printf("\n");
	jx_delete(result);
	return valid ? 0 : 1;
}
