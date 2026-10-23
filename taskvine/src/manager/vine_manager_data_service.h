/* Manage the Manager data port and asynchronous transfers in both directions.
 * The vault is a directory under the Manager staging directory. Each entry is named by a cache name and is the only
 * data a Worker can fetch from the data port: a symlink to a declared local file, or a retrieved checkpoint.
 * The vault table indexes these entries by cache name for queries on the Manager thread. A checkpoint that came inline
 * is held in memory, within a memory limit, and is written to its vault path only when that limit is exceeded or a
 * caller asks for its path.
 * Each direction has slots of its own: 128 for Worker requests served from the vault, and 128 for checkpoint receives
 * into it over connections, so neither direction waits for the other. Receives that come inline hold no connection and
 * no executor, and are bounded by their bytes in flight instead.
 * The Manager thread owns file lookup and completion callbacks. Executors only perform network and file I/O.
 * All entry points except executor functions run on the Manager thread. */
#ifndef VINE_MANAGER_DATA_SERVICE_H
#define VINE_MANAGER_DATA_SERVICE_H

#include <stdint.h>
#include <time.h>

struct vine_manager;
struct vine_file;
struct vine_worker_info;
struct vine_manager_data_service;
struct link_poll_set;

/* Result of a checkpoint request. Every refusal leaves nothing behind and can be retried later. */
typedef enum {
	VINE_CHECKPOINT_ADMITTED = 1,	/* Started, already in progress, or already in the vault. */
	VINE_CHECKPOINT_BUSY = 0,	/* Every receive slot is in use, or, for a file that comes inline, the bound of inline bytes in flight, or the
				 * host of every Worker that holds a large file sends files to others. */
	VINE_CHECKPOINT_NO_SOURCE = -1, /* The file is not a temporary file with a ready Worker replica. */
	VINE_CHECKPOINT_FULL = -2,	/* The vault limit leaves no room for the file. */
} vine_manager_checkpoint_result_t;

/* Create a data listener and a bounded executor pool. */
struct vine_manager_data_service *vine_manager_data_service_create(const char *runtime_directory);
/* Cancel outstanding transfers and release their resources without invoking callbacks. */
void vine_manager_data_service_delete(struct vine_manager_data_service *ds);
/* Return the data port advertised to Workers. */
int vine_manager_data_service_port(struct vine_manager_data_service *ds);
/* Watch the listener and the completion notification in the Manager's poll set before it waits. */
void vine_manager_data_service_poll(struct vine_manager_data_service *ds, struct link_poll_set *set);
/* Accept connections and process a bounded batch of executor results. */
void vine_manager_data_service_handle(struct vine_manager *manager);

/* Request a checkpoint of a temporary file into the vault through a receive slot. A file of at most
 * VINE_DATA_INLINE_BYTES comes over the control connection of its Worker, which answers "inline-get" with
 * "inline-data" and the bytes, instead of over a connection of its own. Larger files, and files that the Worker does
 * not send inline, come with the native get over a connection.
 * When admitted, the Manager thread later calls complete with one when the complete file is published, with zero
 * when the receive fails, or with -1 when it fails because the source answered that it cannot send the file or an
 * entry inside it, such as one it cannot read, which a retry from that source cannot change. An existing vault entry
 * completes immediately. A checkpoint published as a copy in the shared directory (see
 * vine_manager_data_service_set_shared_directory()) keeps its callback while its entry exists: when a Worker's request
 * finds the copy gone, deleted outside the vault, the entry is removed and complete is called again with -2, so the
 * caller must keep its context valid until it removes the file from the vault.
 * An in-progress request keeps its original callback. Removing the vault entry cancels the receive and drops
 * its callback. */
vine_manager_checkpoint_result_t vine_manager_data_service_checkpoint(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument);
/* Request a checkpoint of a temporary file that a submitted task reads. It behaves as a checkpoint, except that a file
 * that comes inline is admitted beyond the bound of inline receives in flight, because the task waits for it: a ready
 * task does not fetch a small input from a peer while the input comes inline into the vault, and the input goes with
 * the task at dispatch once it is published (see vine_manager_data_service_receiving_inline()). A larger file is
 * refused with VINE_CHECKPOINT_BUSY and keeps its place in the caller's checkpoint order. */
vine_manager_checkpoint_result_t vine_manager_data_service_checkpoint_input(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument);
/* Request a temporary file that the application needs now. It behaves as a checkpoint, except that it is admitted
 * beyond the vault limit and the bound of inline receives in flight, so only slots and a ready Worker replica can
 * refuse it. */
vine_manager_checkpoint_result_t vine_manager_data_service_fetch(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument);
/* Return the admission order of a receive of the file that comes inline and is in progress, which starts at one, or
 * zero when there is none. While a small file comes inline, a task that reads it waits for its publication instead of a
 * transfer of its own from a peer, which costs a process on the source Worker and a connection: the vault holds it in
 * a moment and sends it with the task over the control connection. The receive started last tends to end last, so a
 * task that waits for several waits for that one. Does not touch the filesystem. */
uint64_t vine_manager_data_service_receiving_inline(struct vine_manager *manager, struct vine_file *file);

/* Handle the header line of a Worker's reply to an inline request, "inline-data <name> <length> <mode>" or
 * "inline-error <name> <errno>", and read whatever of the data already arrived. Return zero for a reply that does not
 * match the Worker's oldest request, which breaks the protocol. */
int vine_manager_data_service_inline_reply(struct vine_manager *manager, struct vine_worker_info *w, const char *line);
/* Return non-zero while the data of an inline reply from this Worker is still arriving. Its next bytes are that data,
 * not a message. */
int vine_manager_data_service_inline_pending(struct vine_manager *manager, struct vine_worker_info *w);
/* Read the rest of the data of a Worker's inline reply until stoptime. LINK_NOWAIT reads only what arrived. Return 1
 * when no data remains, 0 when more is to come, and -1 when the link failed. */
int vine_manager_data_service_inline_read(struct vine_manager *manager, struct vine_worker_info *w, time_t stoptime);

/* Handle a Worker's answer to a copy to the shared directory, "cache-copied <token> <size>" or "cache-copy-error <token>
 * <errno>": publish the copy, fail the receive when the file left the Worker's cache, or receive the file over a
 * connection otherwise. Return zero for an answer that matches no copy of the Worker, which breaks the protocol. */
int vine_manager_data_service_copy_reply(struct vine_manager *manager, struct vine_worker_info *w, const char *line);

/* Declare a regular local file as a workflow-cached VINE_FILE served through the vault. Return NULL on failure.
 * The caller keeps the local source unchanged until undeclaration. */
struct vine_file *vine_manager_data_service_declare_file(struct vine_manager *manager, const char *source_path);
/* A declared file up to this many bytes travels with its task over the control link, like any local input, instead of
 * through the data port, and a checkpoint up to this many bytes comes back over the control link. A connection of its own costs more than such a file, and every connection occupies an entry
 * of the host's connection tracking table for minutes after it closes. */
#define VINE_DATA_INLINE_BYTES 65536
/* Cancel checkpoint retrieval and remove the file's vault entry without waiting for queued work.
 * Only a connected receive is joined. Open sends retain their descriptors. */
void vine_manager_data_service_vault_remove(struct vine_manager_data_service *ds, struct vine_file *file);

/* Cancel every receive from a Worker that is being removed. Each reports failure at once, so its file is tried again
 * from another source or recreated, instead of waiting on a connection that may never answer. */
void vine_manager_data_service_remove_worker(struct vine_manager_data_service *ds, struct vine_worker_info *w);

/* Free the removed entries of the vault index and the job indexes, which grow with files, and return how many were
 * freed. Call it on the Manager thread outside any walk of these tables, see compact_tables() in vine_manager.c. */
int vine_manager_data_service_compact(struct vine_manager_data_service *ds);
/* Return non-zero when the vault holds a complete entry for this file. Does not touch the filesystem. */
int vine_manager_data_service_vault_contains(struct vine_manager *manager, struct vine_file *file);
/* Return non-zero when the vault holds a checkpoint of the file that came inline from its Worker: a regular file of at
 * most VINE_DATA_INLINE_BYTES. The Manager sends such a file at dispatch over the control connection, like a small
 * local file, instead of a transfer from a peer or the data port, so such an entry is never served from the data port,
 * which serves only entries on disk. Does not touch the filesystem. */
int vine_manager_data_service_vault_small_file(struct vine_manager *manager, struct vine_file *file);
/* Return the bytes of a checkpoint that the vault holds in memory, and set length and, unless NULL, mode to its size and
 * permission bits. Return NULL when the vault holds no entry of the file in memory. The bytes stay valid until the
 * Manager thread next changes the vault, so a caller copies or sends them at once. Does not touch the filesystem. */
const char *vine_manager_data_service_vault_data(struct vine_manager *manager, struct vine_file *file, int64_t *length, int *mode);
/* Return the data of a small checkpoint (vine_manager_data_service_vault_small_file()) as Worker queues send it, and set
 * length and mode to its size and permission bits, or return NULL. The data is the entry's bytes held in memory, or its
 * vault path, which a queue reads when the data reaches its head. A queue holds its own reference, so the data stays
 * valid after its entry is written to disk or removed: removal reads data on disk into memory while queues hold it.
 * Does not touch the filesystem. */
struct link_queue_shared;
struct link_queue_shared *vine_manager_data_service_vault_shared(struct vine_manager *manager, struct vine_file *file, int64_t *length, int *mode);
/* Return a newly allocated path of the file's vault entry, or NULL when the vault holds none. The entry stays until
 * the file is pruned or undeclared. An entry held in memory is first written to that path on the calling thread, a
 * single file of at most VINE_DATA_INLINE_BYTES, because the caller reads the path next. */
char *vine_manager_data_service_vault_path(struct vine_manager *manager, struct vine_file *file);
/* Limit the bytes of checkpoint entries, in memory or on disk, including receives in progress. Zero means unlimited.
 * Declared local files are not counted because the vault only links to them. Bytes held in memory count because they
 * may be written to disk at any time. */
void vine_manager_data_service_vault_set_limit(struct vine_manager_data_service *ds, int64_t bytes);
/* Limit the bytes of checkpoints held in memory. Beyond it, executors write the oldest to disk. Zero writes every
 * checkpoint to disk before it is published. */
void vine_manager_data_service_vault_set_memory_limit(struct vine_manager_data_service *ds, int64_t bytes);
/* Keep checkpoints of at least the shared minimum size in a directory of their own, made inside path, on a file system
 * that the Manager and every Worker mount at the same path. The Worker that holds such a file copies it there in a
 * process of its own ("cache-copy"), up to VINE_DATA_COPY_SLOTS at once, and the vault links the copy under the file's
 * cache name when the Worker answers. While every copy slot is taken, up to VINE_DATA_SHARED_RECEIVES such files come
 * over connections instead, so the vault's own disk adds to the bandwidth without holding the tail of a run. Removing an entry deletes its copy off the Manager thread,
 * and deleting the service removes the directory. A copy deleted by others is noticed when a Worker asks for it: the
 * entry is removed, so the Manager recreates the file for its readers. May be set once. Return zero with errno set on
 * failure. */
int vine_manager_data_service_set_shared_directory(struct vine_manager_data_service *ds, const char *path);
/* Set the smallest checkpoint that goes to the shared directory, VINE_DATA_SHARED_MIN_BYTES by default. */
void vine_manager_data_service_set_shared_min_bytes(struct vine_manager_data_service *ds, int64_t bytes);
/* Return the bytes of published checkpoint entries plus reservations of receives in progress. */
int64_t vine_manager_data_service_vault_get_usage(struct vine_manager_data_service *ds);

#endif
