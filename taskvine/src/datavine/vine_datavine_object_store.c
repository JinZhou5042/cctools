/* Immutable content-addressed objects for the DataVine data plane. */

#include "vine_datavine_object_store.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <openssl/evp.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

struct vine_datavine_object_store {
	char root[PATH_MAX];
	atomic_uint_fast64_t put_requests;
	atomic_uint_fast64_t put_deduplicated;
	atomic_uint_fast64_t put_bytes;
	atomic_uint_fast64_t get_requests;
	atomic_uint_fast64_t get_bytes;
	atomic_uint_fast64_t failures;
	atomic_uint_fast64_t temporary_sequence;
};

static int valid_digest(const char digest[65])
{
	if (!digest || strlen(digest) != 64)
		return 0;
	for (size_t index = 0; index < 64; index++) {
		if (!((digest[index] >= '0' && digest[index] <= '9') ||
					(digest[index] >= 'a' && digest[index] <= 'f')))
			return 0;
	}
	return 1;
}

static int digest_matches(const void *data, size_t size, const char expected[65])
{
	unsigned char digest[EVP_MAX_MD_SIZE];
	unsigned int digest_size = 0;
	EVP_MD_CTX *context = EVP_MD_CTX_new();
	int valid = context && EVP_DigestInit_ex(context, EVP_sha256(), 0) == 1 &&
			EVP_DigestUpdate(context, data, size) == 1 &&
			EVP_DigestFinal_ex(context, digest, &digest_size) == 1 &&
			digest_size == 32;
	EVP_MD_CTX_free(context);
	if (!valid)
		return 0;
	static const char hexadecimal[] = "0123456789abcdef";
	char encoded[65];
	for (size_t index = 0; index < 32; index++) {
		encoded[index * 2] = hexadecimal[digest[index] >> 4];
		encoded[index * 2 + 1] = hexadecimal[digest[index] & 15];
	}
	encoded[64] = 0;
	return !strcmp(encoded, expected);
}

static int ensure_directory(const char *path)
{
	return mkdir(path, 0700) == 0 || errno == EEXIST;
}

static int object_path(struct vine_datavine_object_store *store,
		const char digest[65], char path[PATH_MAX], int create)
{
	if (!store || !valid_digest(digest))
		return 0;
	char first[PATH_MAX];
	char second[PATH_MAX];
	if (snprintf(first, sizeof(first), "%s/%.2s", store->root, digest) >=
					(int)sizeof(first) ||
			snprintf(second, sizeof(second), "%s/%.2s", first, digest + 2) >=
					(int)sizeof(second) ||
			snprintf(path, PATH_MAX, "%s/%s", second, digest) >= PATH_MAX)
		return 0;
	return !create || (ensure_directory(first) && ensure_directory(second));
}

static int write_all(int fd, const unsigned char *data, size_t size)
{
	while (size) {
		ssize_t written = write(fd, data, size);
		if (written > 0) {
			data += written;
			size -= (size_t)written;
		} else if (written < 0 && errno == EINTR) {
			continue;
		} else {
			return 0;
		}
	}
	return 1;
}

static int sync_parent_directory(const char *path)
{
	char parent[PATH_MAX];
	size_t length = strlen(path);
	if (length >= sizeof(parent))
		return 0;
	memcpy(parent, path, length + 1);
	char *separator = strrchr(parent, '/');
	if (!separator)
		return 0;
	*separator = 0;
	int fd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	int valid = fd >= 0 && fsync(fd) == 0;
	if (fd >= 0 && close(fd) != 0)
		valid = 0;
	return valid;
}

struct vine_datavine_object_store *vine_datavine_object_store_open(
		const char *workflow_journal_path)
{
	if (!workflow_journal_path || !workflow_journal_path[0])
		return 0;
	struct vine_datavine_object_store *store = calloc(1, sizeof(*store));
	if (!store)
		return 0;
	char configured[PATH_MAX];
	if (snprintf(configured, sizeof(configured), "%s.objects", workflow_journal_path) >= (int)sizeof(configured) ||
			!ensure_directory(configured) || !realpath(configured, store->root)) {
		free(store);
		return 0;
	}
	return store;
}

void vine_datavine_object_store_close(
		struct vine_datavine_object_store *store)
{
	free(store);
}

const char *vine_datavine_object_store_root(
		struct vine_datavine_object_store *store)
{
	return store ? store->root : 0;
}

int vine_datavine_object_store_put(
		struct vine_datavine_object_store *store, const char digest[65],
		const void *data, size_t size, int *deduplicated)
{
	if (deduplicated)
		*deduplicated = 0;
	if (!store || (!data && size) || !digest_matches(data, size, digest)) {
		if (store)
			atomic_fetch_add(&store->failures, 1);
		return 0;
	}
	atomic_fetch_add(&store->put_requests, 1);
	char path[PATH_MAX];
	if (!object_path(store, digest, path, 1)) {
		atomic_fetch_add(&store->failures, 1);
		return 0;
	}
	struct stat status;
	if (stat(path, &status) == 0) {
		if ((uint64_t)status.st_size != (uint64_t)size) {
			atomic_fetch_add(&store->failures, 1);
			return 0;
		}
		atomic_fetch_add(&store->put_deduplicated, 1);
		if (deduplicated)
			*deduplicated = 1;
		return 1;
	}
	char temporary[PATH_MAX];
	uint64_t sequence = atomic_fetch_add(&store->temporary_sequence, 1);
	if (snprintf(temporary, sizeof(temporary), "%s.part-%ld-%llu", path, (long)getpid(), (unsigned long long)sequence) >=
			(int)sizeof(temporary)) {
		atomic_fetch_add(&store->failures, 1);
		return 0;
	}
	int fd = open(temporary, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
	int valid = fd >= 0 && write_all(fd, data, size) && fsync(fd) == 0;
	if (fd >= 0 && close(fd) != 0)
		valid = 0;
	if (valid && link(temporary, path) != 0) {
		if (errno == EEXIST && stat(path, &status) == 0) {
			valid = (uint64_t)status.st_size == (uint64_t)size;
			if (valid) {
				atomic_fetch_add(&store->put_deduplicated, 1);
				if (deduplicated)
					*deduplicated = 1;
			}
		} else {
			valid = 0;
		}
	} else if (valid) {
		valid = sync_parent_directory(path);
	}
	int cleanup_error = unlink(temporary) != 0 && errno != ENOENT;
	if (!valid)
		atomic_fetch_add(&store->failures, 1);
	else
		atomic_fetch_add(&store->put_bytes, size);
	if (cleanup_error)
		atomic_fetch_add(&store->failures, 1);
	return valid;
}

int vine_datavine_object_store_get(
		struct vine_datavine_object_store *store, const char digest[65],
		unsigned char **data, size_t *size)
{
	if (!store || !data || !size)
		return 0;
	*data = 0;
	*size = 0;
	atomic_fetch_add(&store->get_requests, 1);
	char path[PATH_MAX];
	if (!object_path(store, digest, path, 0))
		goto failure;
	int fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0)
		goto failure;
	struct stat status;
	if (fstat(fd, &status) != 0 || status.st_size < 0 ||
			(uint64_t)status.st_size > SIZE_MAX) {
		close(fd);
		goto failure;
	}
	size_t length = (size_t)status.st_size;
	unsigned char *buffer = length ? malloc(length) : malloc(1);
	if (!buffer) {
		close(fd);
		goto failure;
	}
	size_t used = 0;
	while (used < length) {
		ssize_t count = read(fd, buffer + used, length - used);
		if (count > 0)
			used += (size_t)count;
		else if (count < 0 && errno == EINTR)
			continue;
		else
			break;
	}
	int valid = used == length && close(fd) == 0 &&
			digest_matches(buffer, length, digest);
	if (!valid) {
		free(buffer);
		goto failure;
	}
	*data = buffer;
	*size = length;
	atomic_fetch_add(&store->get_bytes, length);
	return 1;

failure:
	atomic_fetch_add(&store->failures, 1);
	return 0;
}

int vine_datavine_object_store_path(
		struct vine_datavine_object_store *store, const char digest[65],
		char *path, size_t path_size, int create_directories)
{
	if (!path || !path_size)
		return 0;
	char resolved[PATH_MAX];
	if (!object_path(store, digest, resolved, create_directories) ||
			strlen(resolved) + 1 > path_size)
		return 0;
	memcpy(path, resolved, strlen(resolved) + 1);
	return 1;
}

int vine_datavine_object_store_metrics(
		struct vine_datavine_object_store *store,
		struct vine_datavine_object_store_metrics *metrics)
{
	if (!store || !metrics)
		return 0;
	*metrics = (struct vine_datavine_object_store_metrics){
			.put_requests = atomic_load(&store->put_requests),
			.put_deduplicated = atomic_load(&store->put_deduplicated),
			.put_bytes = atomic_load(&store->put_bytes),
			.get_requests = atomic_load(&store->get_requests),
			.get_bytes = atomic_load(&store->get_bytes),
			.failures = atomic_load(&store->failures),
	};
	return 1;
}
