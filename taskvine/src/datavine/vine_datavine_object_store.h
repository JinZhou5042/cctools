/* Immutable content-addressed objects for the DataVine data plane. */
#ifndef VINE_DATAVINE_OBJECT_STORE_H
#define VINE_DATAVINE_OBJECT_STORE_H

#include <stddef.h>
#include <stdint.h>

struct vine_datavine_object_store;

struct vine_datavine_object_store_metrics {
	uint64_t put_requests;
	uint64_t put_deduplicated;
	uint64_t put_bytes;
	uint64_t get_requests;
	uint64_t get_bytes;
	uint64_t failures;
};

struct vine_datavine_object_store *vine_datavine_object_store_open(
		const char *workflow_journal_path);
void vine_datavine_object_store_close(
		struct vine_datavine_object_store *store);

/* Stable SharedFS root used to resolve immutable content keys. */
const char *vine_datavine_object_store_root(
		struct vine_datavine_object_store *store);

/* The caller supplies the lowercase SHA-256 of data.  A successful put is
 * durable and atomic; an existing identical object is a successful no-op. */
int vine_datavine_object_store_put(
		struct vine_datavine_object_store *store, const char digest[65],
		const void *data, size_t size, int *deduplicated);

/* Return a newly allocated byte buffer owned by the caller. */
int vine_datavine_object_store_get(
		struct vine_datavine_object_store *store, const char digest[65],
		unsigned char **data, size_t *size);

/* Resolve the canonical SharedFS path for an object. */
int vine_datavine_object_store_path(
		struct vine_datavine_object_store *store, const char digest[65],
		char *path, size_t path_size, int create_directories);

int vine_datavine_object_store_metrics(
		struct vine_datavine_object_store *store,
		struct vine_datavine_object_store_metrics *metrics);

#endif
