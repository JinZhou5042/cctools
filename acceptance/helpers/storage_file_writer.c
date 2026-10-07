#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static int write_all(int fd, const void *buffer, size_t length)
{
	const char *cursor = buffer;
	while (length) {
		ssize_t written = write(fd, cursor, length);
		if (written < 0 && errno == EINTR) continue;
		if (written <= 0) return 0;
		cursor += written;
		length -= (size_t)written;
	}
	return 1;
}

int main(int argc, char **argv)
{
	if (argc != 4) return 2;
	char *end = 0;
	errno = 0;
	uint64_t bytes = strtoull(argv[2], &end, 10);
	if (errno || !end || *end) return 2;
	end = 0;
	errno = 0;
	(void)strtoull(argv[3], &end, 10);
	if (errno || !end || *end) return 2;

	char temporary[4096];
	if (snprintf(temporary, sizeof(temporary), "%s/.part.%s.XXXXXX", argv[1], argv[3]) >=
			(int)sizeof(temporary)) return 2;
	int fd = mkstemp(temporary);
	if (fd < 0) return 1;

	char *buffer = calloc(1, 1U << 20);
	int valid = buffer != 0;
	for (uint64_t remaining = bytes; valid && remaining;) {
		size_t count = remaining < (1U << 20) ? (size_t)remaining : (1U << 20);
		valid = write_all(fd, buffer, count);
		remaining -= count;
	}
	free(buffer);
	if (valid) valid = fsync(fd) == 0;
	if (close(fd)) valid = 0;

	char final[4096];
	if (valid && snprintf(final, sizeof(final), "%s/data.%s", argv[1], argv[3]) >=
			(int)sizeof(final)) valid = 0;
	if (valid) valid = rename(temporary, final) == 0;
	if (!valid) unlink(temporary);
	return valid ? 0 : 1;
}
