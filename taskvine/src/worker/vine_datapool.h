/*
The datapool keeps memory copies of files in the Worker cache. The cache owns object identity and lifetime through
vine_cache_file records. The datapool manages placement and uses a dttools priority queue to select memory copies for
eviction. Other Worker components access data through vine_cache.

Disk always holds the complete object. Loading a memory copy is optional, and failure to load it leaves the disk object
usable. Reads prefer the memory copy when one is available. Eviction discards that copy without writing data back to
disk. A forwarding link keeps existing paths readable. Open descriptors may retain the old memory pages after they leave
datapool usage accounting.

Cached content is assumed to remain unchanged for the lifetime of its cache name. Memory copies are loaded from disk
without content checksums. Repeated admission of a ready object keeps its existing copies. A Manager unlink removes
both copies through the existing cache lifecycle.

The Worker configures a memory budget for data that is fixed when the datapool is created. Tasks cannot borrow unused
capacity or evict data to make room for execution. The limit and current usage are measured separately in bytes. When
memory is full, an incoming file can replace a larger memory copy. Otherwise, the incoming file remains on disk.

Cached data, task sandboxes and Worker files share the available disk space. The datapool has no separate disk quota.
*/

#ifndef VINE_DATAPOOL_H
#define VINE_DATAPOOL_H

#include <stdint.h>

struct vine_datapool;
struct vine_cache_file;

/* Create a datapool with an immutable memory limit in bytes. Zero disables memory copies.
 * disk_directory must remain valid until deletion. Return NULL if initialization fails. */
struct vine_datapool *vine_datapool_create(const char *disk_directory, uint64_t memory_limit);
void vine_datapool_delete(struct vine_datapool *pool);
/* Return the current data memory usage in bytes. */
uint64_t vine_datapool_memory_get_usage(struct vine_datapool *pool);
/* Return the memory budget reserved for data, including unused capacity. */
uint64_t vine_datapool_memory_get_limit(struct vine_datapool *pool);

/* Return the authoritative disk path. The caller owns the returned string. */
char *vine_datapool_disk_path(struct vine_datapool *pool, const char *name);
int vine_datapool_store(struct vine_datapool *pool, struct vine_cache_file *file, const char *name, const char *source, uint64_t size);
/* Failed removal retains the entry and memory accounting for a later retry. */
int vine_datapool_remove(struct vine_datapool *pool, struct vine_cache_file *file, const char *name);

#endif
