/* Size-only datapool policy, shared by external and intermediate data. */

#include "vine_datapool.h"
#include "vine_cache_file.h"

#include "copy_stream.h"
#include "debug.h"
#include "path.h"
#include "priority_queue.h"
#include "stringtools.h"
#include "trash.h"
#include "unlink_recursive.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#if defined(CCTOOLS_OPSYS_LINUX)
#include <sys/vfs.h>
#include <linux/magic.h>
#endif
#include <unistd.h>

/* This manages disk and datapool placement for cached Worker files. */
struct vine_datapool {
	const char *disk_directory;   /* This borrows the disk cache directory from the cache owner. */
	char *memory_directory;	      /* This owns the private datapool directory path. */
	uint64_t memory_limit;	      /* This fixes the memory budget in bytes at creation. */
	uint64_t memory_used;	      /* This counts managed memory bytes, excluding evicted files still held open. */
	struct priority_queue *queue; /* This borrows file records and puts the largest memory file first. */
};

struct vine_datapool *vine_datapool_create(const char *disk_directory, uint64_t memory_limit)
{
	struct vine_datapool *s = calloc(1, sizeof(*s));
	if (!s) {
		return NULL;
	}
	s->disk_directory = disk_directory;
	s->memory_limit = memory_limit;
	s->queue = priority_queue_create(0);
	if (!s->queue) {
		free(s);
		return NULL;
	}
	if (!memory_limit) {
		return s;
	}
#if defined(CCTOOLS_OPSYS_LINUX)
	struct statfs filesystem;
	if (statfs("/dev/shm", &filesystem) || filesystem.f_type != TMPFS_MAGIC) {
		debug(D_NOTICE, "cache: datapool requires tmpfs at /dev/shm");
		goto failure;
	}
	char *directory = string_format("/dev/shm/vine-datapool-%d-%d-XXXXXX", (int)getuid(), (int)getpid());
	if (!mkdtemp(directory)) {
		debug(D_NOTICE, "cache: cannot create datapool: %s", strerror(errno));
		free(directory);
		goto failure;
	}
	s->memory_directory = directory;
	debug(D_VINE, "cache: configured memory limit %llu bytes", (unsigned long long)memory_limit);
	return s;
#else
	debug(D_NOTICE, "cache: datapool requires Linux tmpfs");
	goto failure;
#endif

failure:
	vine_datapool_delete(s);
	return NULL;
}

char *vine_datapool_disk_path(struct vine_datapool *s, const char *name)
{
	return string_format("%s/%s", s->disk_directory, name);
}

/* Try to discard a memory copy while keeping existing paths readable through disk.
 * Return 1 on success or 0 if the copy cannot be removed.
 * Open descriptors retain the memory data until their references are released. */
static int vine_datapool_memory_try_evict(struct vine_datapool *s, struct vine_cache_file *file)
{
	const char *name = path_basename(file->memory_path);
	char *disk_path = vine_datapool_disk_path(s, name);
	char *forward = string_format("%s.disk", file->memory_path);
	int removed = 0;
	int error = 0;
	if (symlink(disk_path, forward) < 0) {
		goto cleanup;
	}
	if (rename(forward, file->memory_path) < 0) {
		goto cleanup;
	}
	priority_queue_remove(s->queue, priority_queue_find_idx(s->queue, file));
	s->memory_used -= file->memory_bytes;
	file->memory_bytes = 0;
	removed = 1;

cleanup:
	error = errno;
	unlink(forward);
	free(forward);
	free(disk_path);
	if (!removed) {
		errno = error;
	}
	return removed;
}

/* Try to load a disk object into memory without changing its authoritative disk copy.
 * Return 1 on success or 0 so reads can continue using disk.
 * If space is insufficient, try to evict a larger memory copy before admission. */
static int vine_datapool_memory_try_admit(struct vine_datapool *s, struct vine_cache_file *file, const char *name, const char *disk_path, uint64_t size)
{
	if (!s->memory_directory || size > s->memory_limit) {
		return 0;
	}
	uint64_t page = (uint64_t)getpagesize();
	uint64_t bytes = size ? ((size + page - 1) / page) * page : page;
	if (bytes > s->memory_limit) {
		return 0;
	}
	struct stat info;
	if (lstat(disk_path, &info) || !S_ISREG(info.st_mode) || (uint64_t)info.st_size != size) {
		return 0;
	}
	if (bytes > s->memory_limit - s->memory_used) {
		struct vine_cache_file *largest = priority_queue_peek_top(s->queue);
		if (!largest || size >= largest->size || !vine_datapool_memory_try_evict(s, largest)) {
			return 0;
		}
	}
	if (priority_queue_push(s->queue, file, (double)size) < 0) {
		return 0;
	}
	char *memory_path = string_format("%s/%s", s->memory_directory, name);
	char *temporary = string_format("%s.memory", memory_path);
	int stored = 0;
	int error = 0;
	int64_t copied = copy_file_to_file(disk_path, temporary);
	if (copied < 0) {
		goto cleanup;
	}
	if ((uint64_t)copied != size) {
		errno = EIO;
		goto cleanup;
	}
	if (chmod(temporary, info.st_mode & ~0222) < 0) {
		goto cleanup;
	}
	if (rename(temporary, memory_path) < 0) {
		goto cleanup;
	}
	file->size = size;
	file->memory_path = memory_path;
	file->memory_bytes = bytes;
	s->memory_used += bytes;
	stored = 1;
	debug(D_VINE, "cache: memory stored %s size=%llu used=%llu limit=%llu", name, (unsigned long long)size, (unsigned long long)s->memory_used, (unsigned long long)s->memory_limit);

cleanup:
	error = errno;
	if (!stored) {
		priority_queue_remove(s->queue, priority_queue_find_idx(s->queue, file));
		free(memory_path);
	}
	unlink(temporary);
	free(temporary);
	if (!stored) {
		errno = error;
	}
	return stored;
}

void vine_datapool_delete(struct vine_datapool *s)
{
	/* Every object already has a disk copy, so shutdown only removes memory copies. */
	if (s->memory_directory) {
		unlink_recursive(s->memory_directory);
		free(s->memory_directory);
	}
	priority_queue_delete(s->queue);
	free(s);
}

uint64_t vine_datapool_memory_get_usage(struct vine_datapool *s)
{
	return s->memory_used;
}

uint64_t vine_datapool_memory_get_limit(struct vine_datapool *s)
{
	return s->memory_limit;
}

int vine_datapool_store(struct vine_datapool *s, struct vine_cache_file *file, const char *name, const char *source, uint64_t size)
{
	char *path = vine_datapool_disk_path(s, name);
	if (rename(source, path) < 0) {
		free(path);
		return 0;
	}
	/* A failed preload never removes or replaces the disk object. */
	vine_datapool_memory_try_admit(s, file, name, path, size);
	free(path);
	return 1;
}

static int remove_path(const char *path)
{
	if (!path || !unlink(path) || errno == ENOENT) {
		return 1;
	}
	debug(D_NOTICE, "cache: could not remove %s: %s", path, strerror(errno));
	return 0;
}

int vine_datapool_remove(struct vine_datapool *s, struct vine_cache_file *file, const char *name)
{
	if (!remove_path(file->memory_path)) {
		return 0;
	}
	if (file->memory_bytes) {
		priority_queue_remove(s->queue, priority_queue_find_idx(s->queue, file));
		s->memory_used -= file->memory_bytes;
		file->memory_bytes = 0;
	}
	char *path = vine_datapool_disk_path(s, name);
	trash_file(path);
	free(path);
	return 1;
}
