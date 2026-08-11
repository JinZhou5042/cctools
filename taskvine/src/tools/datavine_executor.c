/* Minimal language-neutral persistent executor for native DataVine builtins. */

#include "vine_datavine_protocol.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#define FRAME_LIMIT (64U * 1024U * 1024U)

static int write_all(int fd, const void *data, size_t size)
{
	const unsigned char *cursor = data;
	while (size) {
		ssize_t count = write(fd, cursor, size);
		if (count > 0) {
			cursor += count;
			size -= (size_t)count;
		} else if (count < 0 && errno == EINTR) {
			continue;
		} else {
			return 0;
		}
	}
	return 1;
}

static int read_all(int fd, void *data, size_t size)
{
	unsigned char *cursor = data;
	while (size) {
		ssize_t count = read(fd, cursor, size);
		if (count > 0) {
			cursor += count;
			size -= (size_t)count;
		} else if (count < 0 && errno == EINTR) {
			continue;
		} else {
			return 0;
		}
	}
	return 1;
}

static int read_size(int fd, size_t *result)
{
	char line[32];
	size_t used = 0;
	while (used + 1 < sizeof(line)) {
		char byte;
		ssize_t count = read(fd, &byte, 1);
		if (count < 0 && errno == EINTR)
			continue;
		if (count != 1)
			return 0;
		if (byte == '\n') {
			line[used] = 0;
			char *end = 0;
			unsigned long long parsed = strtoull(line, &end, 10);
			if (!used || *end || parsed > FRAME_LIMIT)
				return 0;
			*result = (size_t)parsed;
			return 1;
		}
		if (byte < '0' || byte > '9')
			return 0;
		line[used++] = byte;
	}
	return 0;
}

static int send_configuration(int fd, int task_id, pid_t worker_pid)
{
	char body[192];
	int body_size = snprintf(body, sizeof(body),
			"{\"name\":\"datavine-native-v1\",\"taskid\":%d,\"exec_mode\":\"direct\"}",
			task_id);
	char header[32];
	int header_size = snprintf(header, sizeof(header), "%d\n", body_size);
	return body_size > 0 && (size_t)body_size < sizeof(body) &&
		write_all(fd, header, (size_t)header_size) &&
		write_all(fd, body, (size_t)body_size) &&
		kill(worker_pid, SIGCHLD) == 0;
}

static int send_result(int fd, uint64_t task_id, int exit_code,
		const void *payload, size_t payload_size, pid_t worker_pid)
{
	char result_header[96];
	int result_header_size = snprintf(result_header, sizeof(result_header),
			"%llu %d %zu\n", (unsigned long long)task_id,
			exit_code, payload_size);
	size_t frame_size = (size_t)result_header_size + payload_size;
	char frame_header[32];
	int frame_header_size = snprintf(frame_header, sizeof(frame_header),
			"%zu\n", frame_size);
	return result_header_size > 0 &&
		write_all(fd, frame_header, (size_t)frame_header_size) &&
		write_all(fd, result_header, (size_t)result_header_size) &&
		(!payload_size || write_all(fd, payload, payload_size)) &&
		kill(worker_pid, SIGCHLD) == 0;
}

static int parse_integer_option(int argc, char **argv, const char *name,
		int *result)
{
	for (int i = 1; i + 1 < argc; i++) {
		if (!strcmp(argv[i], name)) {
			char *end = 0;
			long value = strtol(argv[i + 1], &end, 10);
			if (*end || value < 0 || value > INT32_MAX)
				return 0;
			*result = (int)value;
			return 1;
		}
	}
	return 0;
}

int main(int argc, char **argv)
{
	int input_fd = -1;
	int output_fd = -1;
	int task_id = -1;
	int worker_pid = -1;
	signal(SIGPIPE, SIG_IGN);
	if (!parse_integer_option(argc, argv, "--in-pipe-fd", &input_fd) ||
			!parse_integer_option(argc, argv, "--out-pipe-fd", &output_fd) ||
			!parse_integer_option(argc, argv, "--task-id", &task_id) ||
			!parse_integer_option(argc, argv, "--worker-pid", &worker_pid) ||
			task_id < 1 || worker_pid < 1 ||
			!send_configuration(output_fd, task_id, (pid_t)worker_pid))
		return 2;

	for (;;) {
		size_t frame_size = 0;
		if (!read_size(input_fd, &frame_size))
			return 0;
		unsigned char *frame = malloc(frame_size + 1);
		if (!frame || !read_all(input_fd, frame, frame_size)) {
			free(frame);
			return 3;
		}
		frame[frame_size] = 0;
		unsigned char *newline = memchr(frame, '\n', frame_size);
		unsigned long long parsed_id = 0;
		size_t input_size = 0;
		char function[64];
		int valid = newline && sscanf((char *)frame,
				"%llu %63s %*s %*s %zu",
				&parsed_id,
				function, &input_size) == 3;
		uint64_t invocation_id = (uint64_t)parsed_id;
		unsigned char *input = newline ? newline + 1 : 0;
		size_t available = newline ? frame_size - (size_t)(input - frame) : 0;
		valid = valid && input_size == available;
		const void *result = 0;
		size_t result_size = 0;
		if (valid && !strcmp(function, "execute_datavine_task_ticket") &&
				input_size == 32 && !memcmp(input, "DVT1", 4) &&
				vine_datavine_get_u64(input + 8) > 0 &&
				vine_datavine_get_u64(input + 16) > 0 &&
				vine_datavine_get_u64(input + 24) == 1) {
			/* fixed-width native noop ticket */
		} else if (valid && !strcmp(function, "datavine_builtin") &&
				input_size == 5 && !memcmp(input, "DVB1", 4) &&
				input[4] == 1) {
			/* noop */
		} else if (valid && !strcmp(function, "datavine_builtin") &&
				input_size >= 5 && !memcmp(input, "DVB1", 4) &&
				input[4] == 2) {
			/* echo: intentionally text/byte-safe except embedded NUL, which the
			 * TaskVine stdout completion contract does not preserve. */
			result = input + 5;
			result_size = input_size - 5;
			if (memchr(result, 0, result_size))
				valid = 0;
		} else {
			valid = 0;
		}
		int sent = send_result(output_fd, invocation_id, valid ? 0 : 2,
				result, valid ? result_size : 0, (pid_t)worker_pid);
		free(frame);
		if (!sent)
			return 4;
	}
}
