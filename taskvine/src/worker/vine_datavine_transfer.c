/* Worker-side DataVine content transfer. */

#include "vine_datavine_transfer.h"
#include "vine_datavine_protocol.h"
#include "domain_name_cache.h"
#include "full_io.h"
#include "link.h"
#include "stringtools.h"

#include <errno.h>
#include <fcntl.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

struct object_source {
	char host[256];
	int port;
	char digest[65];
	char signature[65];
};

static pthread_mutex_t connection_lock = PTHREAD_MUTEX_INITIALIZER;
static struct link *shared_connection;
static char shared_host[256];
static int shared_port;
static uint64_t next_request_id = 1;

static int lowercase_hex(const char *value, size_t size)
{
	for (size_t index = 0; index < size; index++) {
		if (!((value[index] >= '0' && value[index] <= '9') ||
					(value[index] >= 'a' && value[index] <= 'f')))
			return 0;
	}
	return 1;
}

static int parse_source(const char *source, struct object_source *parsed)
{
	static const char prefix[] = "datavine://";
	if (!source || strncmp(source, prefix, sizeof(prefix) - 1))
		return 0;
	const char *authority = source + sizeof(prefix) - 1;
	const char *path = strchr(authority, '/');
	if (!path || strlen(path) != 130 || path[65] != '/')
		return 0;
	const char *port_text = 0;
	size_t host_size = 0;
	if (authority[0] == '[') {
		const char *closing = strchr(authority + 1, ']');
		if (!closing || closing + 1 >= path || closing[1] != ':')
			return 0;
		host_size = (size_t)(closing - authority - 1);
		port_text = closing + 2;
		authority++;
	} else {
		const char *colon = 0;
		for (const char *cursor = authority; cursor < path; cursor++) {
			if (*cursor == ':')
				colon = cursor;
		}
		if (!colon)
			return 0;
		host_size = (size_t)(colon - authority);
		port_text = colon + 1;
	}
	if (!host_size || host_size >= sizeof(parsed->host) || port_text >= path)
		return 0;
	char port_buffer[8];
	size_t port_size = (size_t)(path - port_text);
	if (!port_size || port_size >= sizeof(port_buffer))
		return 0;
	memcpy(port_buffer, port_text, port_size);
	port_buffer[port_size] = 0;
	char trailing = 0;
	if (sscanf(port_buffer, "%d%c", &parsed->port, &trailing) != 1 ||
			parsed->port < 1 || parsed->port > 65535)
		return 0;
	memcpy(parsed->host, authority, host_size);
	parsed->host[host_size] = 0;
	memcpy(parsed->digest, path + 1, 64);
	parsed->digest[64] = 0;
	memcpy(parsed->signature, path + 66, 64);
	parsed->signature[64] = 0;
	return lowercase_hex(parsed->digest, 64) &&
		   lowercase_hex(parsed->signature, 64);
}

static int request(struct link *connection, uint16_t opcode,
		const void *payload, uint32_t payload_size, uint64_t request_id,
		unsigned char response[VINE_DATAVINE_RPC_RESPONSE_HEADER],
		time_t deadline)
{
	unsigned char header[VINE_DATAVINE_RPC_REQUEST_HEADER];
	vine_datavine_put_u32(header, VINE_DATAVINE_RPC_MAGIC);
	vine_datavine_put_u16(header + 4, VINE_DATAVINE_RPC_VERSION);
	vine_datavine_put_u16(header + 6, opcode);
	vine_datavine_put_u32(header + 8, payload_size);
	vine_datavine_put_u64(header + 12, request_id);
	return link_putlstring(connection, (const char *)header, sizeof(header), deadline) ==
				   (ssize_t)sizeof(header) &&
		   link_putlstring(connection, payload, payload_size, deadline) ==
				   (ssize_t)payload_size &&
		   link_read(connection, (char *)response, VINE_DATAVINE_RPC_RESPONSE_HEADER, deadline) ==
				   VINE_DATAVINE_RPC_RESPONSE_HEADER &&
		   vine_datavine_get_u32(response) == VINE_DATAVINE_RPC_MAGIC &&
		   vine_datavine_get_u16(response + 4) == VINE_DATAVINE_RPC_VERSION &&
		   vine_datavine_get_u16(response + 6) == opcode &&
		   vine_datavine_get_u64(response + 16) == request_id;
}

static void connection_reset(void)
{
	if (shared_connection) {
		link_close(shared_connection);
		shared_connection = 0;
	}
	shared_host[0] = 0;
	shared_port = 0;
}

static struct link *connection_get(const struct object_source *parsed,
		time_t deadline)
{
	if (shared_connection &&
			(strcmp(shared_host, parsed->host) || shared_port != parsed->port))
		connection_reset();
	if (shared_connection)
		return shared_connection;
	char address[LINK_ADDRESS_MAX];
	if (!domain_name_cache_lookup(parsed->host, address))
		return 0;
	shared_connection = link_connect(address, parsed->port, deadline);
	if (shared_connection) {
		strcpy(shared_host, parsed->host);
		shared_port = parsed->port;
	}
	return shared_connection;
}

static int transfer_once(const struct object_source *parsed,
		const char *destination, time_t deadline)
{
	struct link *connection = connection_get(parsed, deadline);
	if (!connection)
		return 0;
	unsigned char response[VINE_DATAVINE_RPC_RESPONSE_HEADER];
	unsigned char ticket[128];
	memcpy(ticket, parsed->digest, 64);
	memcpy(ticket + 64, parsed->signature, 64);
	uint64_t request_id = next_request_id++;
	int valid = request(connection, VINE_DATAVINE_RPC_OBJECT_GET,
			ticket, sizeof(ticket), request_id, response, deadline) &&
			vine_datavine_get_u32(response + 8) == VINE_DATAVINE_RPC_OK;
	uint32_t size = valid ? vine_datavine_get_u32(response + 12) : 0;
	if (size > VINE_DATAVINE_RPC_MAX_PAYLOAD)
		valid = 0;
	int fd = valid ? open(destination,
				 O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
				 0600)
			   : -1;
	if (fd < 0)
		valid = 0;
	EVP_MD_CTX *digest = valid ? EVP_MD_CTX_new() : 0;
	if (!digest || EVP_DigestInit_ex(digest, EVP_sha256(), 0) != 1)
		valid = 0;
	uint32_t remaining = size;
	while (valid && remaining) {
		unsigned char buffer[1 << 16];
		size_t chunk = remaining < sizeof(buffer) ? remaining : sizeof(buffer);
		ssize_t count = link_read(connection, (char *)buffer, chunk, deadline);
		if (count != (ssize_t)chunk ||
				full_write(fd, buffer, chunk) != (ssize_t)chunk ||
				EVP_DigestUpdate(digest, buffer, chunk) != 1) {
			valid = 0;
			break;
		}
		remaining -= (uint32_t)chunk;
	}
	unsigned char actual[EVP_MAX_MD_SIZE];
	unsigned int actual_size = 0;
	if (valid && (EVP_DigestFinal_ex(digest, actual, &actual_size) != 1 ||
				actual_size != 32))
		valid = 0;
	if (valid) {
		char encoded[65];
		static const char hexadecimal[] = "0123456789abcdef";
		for (size_t index = 0; index < 32; index++) {
			encoded[index * 2] = hexadecimal[actual[index] >> 4];
			encoded[index * 2 + 1] = hexadecimal[actual[index] & 15];
		}
		encoded[64] = 0;
		valid = !strcmp(encoded, parsed->digest);
	}
	EVP_MD_CTX_free(digest);
	if (fd >= 0 && close(fd) != 0)
		valid = 0;
	if (!valid) {
		unlink(destination);
		connection_reset();
	}
	return valid;
}

static int transfer_sharedfs(const char *source, const char *destination)
{
	static const char prefix[] = "datavine-file://";
	if (strncmp(source, prefix, sizeof(prefix) - 1))
		return 0;
	const char *path = source + sizeof(prefix) - 1;
	const char *digest = strrchr(path, '/');
	if (!path[0] || path[0] != '/' || !digest || strlen(++digest) != 64 ||
			!lowercase_hex(digest, 64))
		return 0;
	int input = open(path, O_RDONLY | O_CLOEXEC);
	int output = input >= 0 ? open(destination,
			O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600) : -1;
	EVP_MD_CTX *context = output >= 0 ? EVP_MD_CTX_new() : 0;
	int valid = context && EVP_DigestInit_ex(context, EVP_sha256(), 0) == 1;
	unsigned char buffer[1 << 16];
	while (valid) {
		ssize_t count = read(input, buffer, sizeof(buffer));
		if (count > 0) {
			valid = full_write(output, buffer, (size_t)count) == count &&
				EVP_DigestUpdate(context, buffer, (size_t)count) == 1;
		} else if (!count) {
			break;
		} else if (errno != EINTR) {
			valid = 0;
		}
	}
	unsigned char actual[EVP_MAX_MD_SIZE];
	unsigned int actual_size = 0;
	valid = valid && EVP_DigestFinal_ex(context, actual, &actual_size) == 1 &&
		actual_size == 32;
	if (context)
		EVP_MD_CTX_free(context);
	if (input >= 0)
		close(input);
	if (output >= 0 && close(output))
		valid = 0;
	if (valid) {
		char encoded[65];
		static const char hexadecimal[] = "0123456789abcdef";
		for (size_t index = 0; index < 32; index++) {
			encoded[index * 2] = hexadecimal[actual[index] >> 4];
			encoded[index * 2 + 1] = hexadecimal[actual[index] & 15];
		}
		encoded[64] = 0;
		valid = !strcmp(encoded, digest);
	}
	if (!valid)
		unlink(destination);
	return valid;
}

int vine_datavine_transfer_get(const char *source, const char *destination,
		char **error_message)
{
	*error_message = 0;
	if (!strncmp(source, "datavine-file://", 16)) {
		int valid = transfer_sharedfs(source, destination);
		if (!valid)
			*error_message = string_format("DataVine SharedFS object transfer failed");
		return valid;
	}
	struct object_source parsed;
	if (!parse_source(source, &parsed)) {
		*error_message = string_format("invalid DataVine object URI");
		return 0;
	}
	pthread_mutex_lock(&connection_lock);
	time_t deadline = time(0) + 300;
	int valid = transfer_once(&parsed, destination, deadline);
	if (!valid)
		valid = transfer_once(&parsed, destination, deadline);
	pthread_mutex_unlock(&connection_lock);
	if (!valid) {
		*error_message = string_format("DataVine object transfer failed for %.12s", parsed.digest);
	}
	return valid;
}
