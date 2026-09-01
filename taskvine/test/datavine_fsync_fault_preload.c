#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

/* Linux-only test shim.  It fails fsync only for DataVine private result files
 * while the test-owned gate exists.  Production code contains no fault hook. */
int fsync(int fd)
{
	const char *gate = getenv("DATAVINE_TEST_FSYNC_FAULT_GATE");
	const char *root = getenv("DATAVINE_TEST_FSYNC_TARGET_ROOT");
	const char *name = getenv("DATAVINE_TEST_FSYNC_FAULT");
	if (gate && root && name && access(gate, F_OK) == 0) {
		char descriptor[64];
		char path[PATH_MAX];
		int descriptor_size = snprintf(descriptor, sizeof(descriptor),
				"/proc/self/fd/%d", fd);
		ssize_t path_size = descriptor_size > 0 &&
				descriptor_size < (int)sizeof(descriptor)
				? readlink(descriptor, path, sizeof(path) - 1) : -1;
		if (path_size > 0) {
			path[path_size] = 0;
			size_t root_size = strlen(root);
			int fault = !strcmp(name, "ENOSPC") ? ENOSPC
					: !strcmp(name, "EIO") ? EIO : 0;
			if (fault && !strncmp(path, root, root_size) && path[root_size] == '/' &&
					strstr(path + root_size, ".part.")) {
				const char *log = getenv("DATAVINE_TEST_FSYNC_FAULT_LOG");
				if (log && log[0]) {
					int output = open(log, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC,
							0600);
					if (output >= 0) {
						char line[PATH_MAX + 64];
						int size = snprintf(line, sizeof(line), "%s %s\n", name, path);
						if (size > 0 && size < (int)sizeof(line))
								syscall(SYS_write, output, line, (size_t)size);
						close(output);
					}
				}
				errno = fault;
				return -1;
			}
		}
	}
	return (int)syscall(SYS_fsync, fd);
}
