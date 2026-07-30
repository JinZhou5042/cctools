/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_datavine.h"
#include "vine_manager.h"

#include "buffer.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "link.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DATAVINE_HTTP_LINE_MAX 4096

static int parse_http_endpoint(const char *endpoint, char **host, int *port)
{
	if (!endpoint || strncmp(endpoint, "http://", 7)) {
		return 0;
	}

	const char *authority = endpoint + 7;
	const char *slash = strchr(authority, '/');
	size_t authority_length = slash ? (size_t)(slash - authority) : strlen(authority);
	char *copy = xxmalloc(authority_length + 1);
	memcpy(copy, authority, authority_length);
	copy[authority_length] = 0;

	char *colon = strrchr(copy, ':');
	if (!colon || colon == copy || !colon[1]) {
		free(copy);
		return 0;
	}
	*colon = 0;
	char *end = 0;
	long parsed_port = strtol(colon + 1, &end, 10);
	if (!end || *end || parsed_port < 1 || parsed_port > 65535) {
		free(copy);
		return 0;
	}

	*host = copy;
	*port = (int)parsed_port;
	return 1;
}

static int post_json(struct vine_manager *q, const char *path, const char *body)
{
	if (!q->datavine_controller_host || !q->datavine_controller_token) {
		return 0;
	}

	char address[LINK_ADDRESS_MAX];
	if (!domain_name_cache_lookup(q->datavine_controller_host, address)) {
		return 0;
	}

	time_t stoptime = time(0) + 1;
	struct link *link = link_connect(address, q->datavine_controller_port, stoptime);
	if (!link) {
		return 0;
	}

	buffer_t request;
	buffer_init(&request);
	buffer_abortonfailure(&request, 1);
	buffer_printf(&request, "POST %s HTTP/1.1\r\n", path);
	buffer_printf(&request, "Host: %s:%d\r\n", q->datavine_controller_host, q->datavine_controller_port);
	buffer_printf(&request, "X-DataVine-Token: %s\r\n", q->datavine_controller_token);
	buffer_putliteral(&request, "Content-Type: application/json\r\n");
	buffer_printf(&request, "Content-Length: %zu\r\n", strlen(body));
	buffer_putliteral(&request, "Connection: close\r\n\r\n");
	buffer_putstring(&request, body);

	ssize_t written = link_putstring(link, buffer_tostring(&request), stoptime);
	buffer_free(&request);
	if (written <= 0) {
		link_close(link);
		return 0;
	}

	char line[DATAVINE_HTTP_LINE_MAX];
	int status = 0;
	if (link_readline(link, line, sizeof(line), stoptime)) {
		string_chomp(line);
		sscanf(line, "HTTP/%*d.%*d %d", &status);
	}
	link_close(link);
	return status >= 200 && status < 300;
}

int vine_datavine_configure(struct vine_manager *q, const char *endpoint, const char *token)
{
	if (!q || !endpoint || !token || !token[0]) {
		return 0;
	}

	char *host = 0;
	int port = 0;
	if (!parse_http_endpoint(endpoint, &host, &port)) {
		return 0;
	}

	free(q->datavine_controller_endpoint);
	free(q->datavine_controller_host);
	free(q->datavine_controller_token);
	q->datavine_controller_endpoint = xxstrdup(endpoint);
	q->datavine_controller_host = host;
	q->datavine_controller_port = port;
	q->datavine_controller_token = xxstrdup(token);
	return 1;
}

int vine_datavine_acquire_transfer(struct vine_manager *q, const char *data_id, const char *source_worker_id,
		const char *destination_worker_id, const char *transfer_id)
{
	if (!q || !data_id || !source_worker_id || !destination_worker_id || !transfer_id) {
		return 0;
	}
	char *body = string_format(
			"{\"data_id\":\"%s\",\"source_worker_id\":\"%s\",\"destination_worker_id\":\"%s\","
			"\"transfer_id\":\"taskvine:%s\"}",
			data_id,
			source_worker_id,
			destination_worker_id,
			transfer_id);
	int result = post_json(q, "/v1/replicas/acquire-observed", body);
	free(body);
	return result;
}

int vine_datavine_release_transfer(struct vine_manager *q, const char *transfer_id, int success)
{
	if (!q || !transfer_id) {
		return 0;
	}
	char *body = string_format("{\"lease_id\":\"taskvine:%s\",\"success\":%s}", transfer_id, success ? "true" : "false");
	int result = post_json(q, "/v1/replicas/release", body);
	free(body);
	return result;
}
