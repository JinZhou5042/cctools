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
#include "jx.h"
#include "jx_parse.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <string.h>
#include <strings.h>
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

static char *post_json_response(struct vine_manager *q, const char *path, const char *body)
{
	if (!q->datavine_controller_host || !q->datavine_controller_token) {
		return NULL;
	}

	buffer_t request;
	buffer_init(&request);
	buffer_abortonfailure(&request, 1);
	buffer_printf(&request, "POST %s HTTP/1.1\r\n", path);
	buffer_printf(&request, "Host: %s:%d\r\n", q->datavine_controller_host, q->datavine_controller_port);
	buffer_printf(&request, "X-DataVine-Token: %s\r\n", q->datavine_controller_token);
	buffer_putliteral(&request, "Content-Type: application/json\r\n");
	buffer_printf(&request, "Content-Length: %zu\r\n", strlen(body));
	buffer_putliteral(&request, "Connection: keep-alive\r\n\r\n");
	buffer_putstring(&request, body);

	for (int attempt = 0; attempt < 2; attempt++) {
		time_t stoptime = time(0) + 1;
		if (!q->datavine_controller_link) {
			char address[LINK_ADDRESS_MAX];
			if (!domain_name_cache_lookup(
					q->datavine_controller_host, address)) {
				break;
			}
			q->datavine_controller_link = link_connect(
					address,
					q->datavine_controller_port,
					stoptime);
			if (!q->datavine_controller_link) {
				continue;
			}
			link_tune(
					q->datavine_controller_link,
					LINK_TUNE_INTERACTIVE);
		}

		struct link *link = q->datavine_controller_link;
		if (link_putstring(
				link, buffer_tostring(&request), stoptime) <= 0) {
			link_close(link);
			q->datavine_controller_link = NULL;
			continue;
		}

		char line[DATAVINE_HTTP_LINE_MAX];
		int status = 0;
		size_t content_length = 0;
		int connection_close = 0;
		if (link_readline(link, line, sizeof(line), stoptime)) {
			string_chomp(line);
			sscanf(line, "HTTP/%*d.%*d %d", &status);
		}
		while (status
				&& link_readline(
						link, line, sizeof(line), stoptime)) {
			string_chomp(line);
			if (!line[0]) {
				break;
			}
			if (!strncasecmp(line, "Content-Length:", 15)) {
				sscanf(line + 15, "%zu", &content_length);
			} else if (!strcasecmp(line, "Connection: close")) {
				connection_close = 1;
			}
		}
		if (!status || content_length == 0
				|| content_length >= 1024 * 1024) {
			link_close(link);
			q->datavine_controller_link = NULL;
			continue;
		}

		char *response = xxmalloc(content_length + 1);
		if (link_read(link, response, content_length, stoptime)
				!= (ssize_t)content_length) {
			free(response);
			link_close(link);
			q->datavine_controller_link = NULL;
			continue;
		}
		response[content_length] = 0;
		if (connection_close) {
			link_close(link);
			q->datavine_controller_link = NULL;
		}
		if (status >= 200 && status < 300) {
			buffer_free(&request);
			return response;
		}
		free(response);
		break;
	}
	buffer_free(&request);
	return NULL;
}

static int post_json(struct vine_manager *q, const char *path, const char *body)
{
	char *response = post_json_response(q, path, body);
	if (!response) {
		return 0;
	}
	free(response);
	return 1;
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

	link_close(q->datavine_controller_link);
	q->datavine_controller_link = NULL;
	free(q->datavine_controller_endpoint);
	free(q->datavine_controller_host);
	free(q->datavine_controller_token);
	q->datavine_controller_endpoint = xxstrdup(endpoint);
	q->datavine_controller_host = host;
	q->datavine_controller_port = port;
	q->datavine_controller_token = xxstrdup(token);
	return 1;
}

int vine_datavine_resolve_transfer(struct vine_manager *q, const char *data_id,
		const char *destination_worker_id, const char *excluded_worker_id,
		const char *transfer_id, char **source_worker_id)
{
	if (!q || !data_id || !destination_worker_id || !transfer_id
			|| !source_worker_id) {
		return 0;
	}
	*source_worker_id = NULL;
	char *body = string_format(
			"{\"data_id\":\"%s\",\"destination_worker_id\":\"%s\","
			"\"transfer_id\":\"taskvine:%s\",\"excluded_worker_ids\":[%s%s%s]}",
			data_id,
			destination_worker_id,
			transfer_id,
			excluded_worker_id ? "\"" : "",
			excluded_worker_id ? excluded_worker_id : "",
			excluded_worker_id ? "\"" : "");
	char *response = post_json_response(
			q, "/v1/replicas/resolve-source", body);
	free(body);
	if (!response) {
		return 0;
	}
	struct jx *object = jx_parse_string(response);
	free(response);
	if (!object) {
		return 0;
	}
	struct jx *source = jx_lookup(object, "source");
	const char *selected = source
			? jx_lookup_string(source, "worker_id")
			: NULL;
	if (selected && selected[0]) {
		*source_worker_id = xxstrdup(selected);
	}
	jx_delete(object);
	return *source_worker_id != NULL;
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
