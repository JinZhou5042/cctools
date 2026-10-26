/* Serve vault entries to Workers and fetch Worker files into the vault through the native get protocol. */
#include "vine_manager_data_service.h"
#include "vine_manager.h"
#include "vine_file.h"
#include "vine_file_replica_table.h"
#include "vine_worker_info.h"
#include "debug.h"
#include "full_io.h"
#include "hash_table.h"
#include "itable.h"
#include "link.h"
#include "link_auth.h"
#include "link_writer.h"
#include "list.h"
#include "set.h"
#include "macros.h"
#include "random.h"
#include "stringtools.h"
#include "thread_pool.h"
#include "url_encode.h"
#include "vine_cached_name.h"
#include "vine_current_transfers.h"
#include "vine_schedule.h"
#include "vine_transfer_receive.h"
#include "vine_transfer_send.h"
#include "unlink_recursive.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

/* Maximum size of a native data request or response header, including the terminator. */
#define VINE_DATA_LINE_MAX 4096

/* Transfer slots of each direction: Worker requests served from the vault, and checkpoint receives into it over
 * connections. Neither direction uses or waits for the slots of the other. */
#define VINE_DATA_REQUEST_SLOTS 128U
#define VINE_DATA_RECEIVE_SLOTS 128U

/* Bytes of inline receives in flight. An inline receive holds no connection and no executor while it waits, only its
 * request in the Worker's control connection, and its reply waits in the Worker's memory until the Manager reads it,
 * so these receives are bounded by their bytes instead of by receive slots. Each counts at least
 * VINE_DATA_INLINE_MINIMUM bytes, which bounds the number of requests at once. */
#define VINE_DATA_INLINE_FLIGHT ((int64_t)16 << 20)
#define VINE_DATA_INLINE_MINIMUM ((int64_t)1024)

/* Writes of checkpoints held in memory to disk that run at once, when the memory limit is exceeded. */
#define VINE_DATA_SPILLS 8U

/* Executors for both directions. A cancelled receive releases its slot before its executor returns, so each receive
 * slot has a spare executor. One more deletes removed directory entries, and the rest write checkpoints held in memory
 * to disk. */
#define VINE_DATA_THREADS (VINE_DATA_REQUEST_SLOTS + 2 * VINE_DATA_RECEIVE_SLOTS + 1 + VINE_DATA_SPILLS)

/* Bytes of checkpoints that the vault holds in memory by default. */
#define VINE_DATA_MEMORY_LIMIT ((int64_t)1000000000)

/* Copies of checkpoints that their Workers write to the shared directory at once, each holding a copy slot. */
#define VINE_DATA_COPY_SLOTS 128U
/* Subdirectories of the vault's shared directory, among which copies are spread by token. A shared file system may
 * bound the writes into one directory: on the cluster's /groups, 128 copies into one directory moved 7.5 MB/s each,
 * about 1 GB/s in all, while a copy into another directory at the same time moved 80-430 MB/s. */
#define VINE_DATA_SHARED_SUBDIRECTORIES 256

/* Checkpoints large enough to copy to the shared directory that come over connections at once, when every copy slot is
 * taken. A copy goes first, and the vault's disk takes a few such files beside the copies: a rotational disk is
 * saturated by about 20 receives, so more only lengthen the tail of a run by the time the disk takes to write them.
 * On the cluster, with 128 such receives the copies of 429 GB ended at 220 s and the receives at 295 s. */
#define VINE_DATA_SHARED_RECEIVES 16U


/* Smallest checkpoint that goes to the shared directory by default: about 16 ms at 250 MB/s, against the few
 * milliseconds that a network file system takes to create and close a file. */
#define VINE_DATA_SHARED_MIN_BYTES ((int64_t)4 << 20)

/* How long a large checkpoint whose holders are all on slow hosts waits for a moment in which one of them sends nothing
 * to other Workers (see find_checkpoint_source()). */
#define VINE_DATA_SLOW_HOST_WAIT ((timestamp_t)10000000)

/* Own the data listener, bounded jobs, the index of vault entries, and the vault disk budget. */
struct vine_manager_data_service {
	char *vault_directory;		/* Own the fixed directory of published cache names. */
	struct hash_table *vault_table; /* Maps cache name -> vault_entry of a complete vault entry. */
	int64_t vault_limit;		/* Maximum bytes of checkpoint entries. Zero means unlimited. */
	int64_t vault_used;		/* Bytes of published checkpoint entries, in memory or on disk. */
	int64_t vault_reserved;		/* Bytes reserved by checkpoint receives in progress. */
	int64_t memory_limit;		/* Maximum bytes of checkpoints held in memory. Zero writes every checkpoint to disk. */
	int64_t memory_used;		/* Bytes of checkpoints held in memory, including those being written to disk. */
	int64_t memory_spilling;	/* Bytes of memory_used that executors are writing to disk. */
	unsigned spills;		/* Executors writing checkpoints held in memory to disk, at most VINE_DATA_SPILLS. */
	struct vault_entry *memory_oldest; /* The list of entries held in memory and not being written, oldest first. */
	struct vault_entry *memory_newest; /* The newest entry of that list. */
	struct link *listener;		/* Accept incoming Worker data connections. */
	struct link *notification;	/* Wake the Manager when an executor finishes a step. */
	int notify_fd;			/* Write completion notifications to this pipe. */
	struct thread_pool *threads;	/* Execute both transfer directions. */
	struct set *jobs;		/* Own all admitted jobs until the Manager releases them. */
	struct hash_table *file_jobs;	/* Maps cache name -> newest job of that file, chained through next_file_job. */
	struct itable *source_jobs;	/* Maps source Worker -> newest receive from it, chained through next_source_job. */
	unsigned requests_active;	/* Worker requests, each holding a request slot. */
	unsigned receives_active;	/* Receives over connections that are not cancelled, each holding a receive slot. */
	int64_t inline_flight;		/* Counted bytes of inline receives that are not cancelled. */
	uint64_t next_order;		/* Admission order of the next receive. */
	struct list *ready;		/* Queue jobs returned by executors. */
	pthread_mutex_t lock;		/* Protect the ready queue, cancellation, completion, active links, and removals. */
	int stopping;			/* Prevent new connections while executors are being stopped. */
	struct itable *waiting;		/* Maps token -> receive waiting for its Worker to connect back. */
	uint64_t next_token;		/* Token of the next receive from a Worker that connects back. Never zero. */
	time_t next_expiry;		/* Earliest time to look for receives whose Worker did not connect back. */
	struct itable *inline_requests; /* Maps source Worker -> list of inline_request, in the order they were sent. */
	struct list *removals;		/* Paths to delete: removed directory entries, renamed out of the vault's names, and copies in the shared directory. */
	int removing;			/* Set while an executor deletes the paths in removals. */
	uint64_t next_removal;		/* Number that makes the name of the next renamed directory entry unique. */
	char *shared_directory;		/* Own the vault's directory on a file system that every Worker mounts, or NULL. */
	int64_t shared_min_bytes;	/* Smallest checkpoint that its Worker copies to the shared directory. */
	unsigned copies_active;		/* Copies to the shared directory that are not cancelled, each holding a copy slot. */
	unsigned shareable_receives;	/* Receives over connections of files large enough to copy, at most VINE_DATA_SHARED_RECEIVES. */
	uint64_t next_copy;		/* Number of the next copy, which names its file and the Worker's answer. Never zero. */
	struct itable *copy_requests;	/* Maps source Worker -> list of copy_request, the copies it has not answered. */
};

/* Index one complete vault entry: a link to a declared local file, or a published checkpoint. A checkpoint that came
 * inline is held in memory, and is written to its vault path only when the memory limit is exceeded or a caller asks
 * for its path. Workers' queues send such a small checkpoint by reference, in memory or from its vault path, so a file
 * that many tasks read is held once however many sends wait in queues. */
struct vault_entry {
	struct vine_file *file;		    /* Hold a reference to the file whose data the entry holds. */
	int64_t size;			    /* Bytes counted in vault_used: the received bytes of a checkpoint, zero for a declared file. */
	int small;			    /* Set for a checkpoint that came inline: a regular file of at most VINE_DATA_INLINE_BYTES. */
	struct link_writer_shared *data;	    /* Hold the bytes of a checkpoint held in memory, which queues share, or NULL. */
	struct link_writer_shared *on_disk;  /* Hold a small checkpoint on disk as queues send it, from its first send, or NULL. */
	int mode;			    /* Permission bits of a small checkpoint. */
	struct transfer_job *spill;	    /* The job writing the bytes to disk, or NULL. */
	struct vault_entry *older;	    /* Neighbors in the list of entries held in memory and not being written. */
	struct vault_entry *newer;
	char *shared_path; /* Own the copy in the shared directory that the entry's vault name links to, or NULL. */
	void (*complete)(void *, struct vine_file *, int); /* For a copy: the callback of the receive that published it. */
	void *argument;					   /* For a copy: that callback's context. */
};

/* Hold one transfer until its completion is consumed by the Manager. A job with a file is a checkpoint receive
 * that fetches the file from a Worker into the vault. A job without one serves a Worker request from the vault. */
struct transfer_job {
	struct vine_manager_data_service *owner;	   /* Borrow the service that owns this job. */
	struct link *link;				   /* Own the active peer connection. */
	struct vine_file *file;				   /* Hold the checkpointed file. NULL for a Worker request. */
	char *password;					   /* Copy the Manager password for peer authentication. */
	char *name;					   /* Own the cache name of the requested or received object. */
	char *ip;					   /* Own the source Worker address for a receive. */
	struct vine_worker_info *source;		   /* Key of source_jobs, never dereferenced. Cleared on removal. */
	int port;					   /* Identify the source Worker data port. */
	char *path;					   /* Own the vault path of a receive. */
	int cancelled;					   /* Prevent a cancelled receive from renaming its file. */
	int removed;					   /* Mark a receive whose vault entry was removed. Never published. */
	int success;					   /* Report whether the transfer completed. */
	int source_error;				   /* Set when the source answered that it cannot send the file. */
	int64_t reserved;				   /* Own vault bytes reserved for a receive. */
	int64_t received;				   /* Record the bytes published by a successful receive. */
	void (*complete)(void *, struct vine_file *, int); /* Report a checkpoint result on the Manager thread. */
	void *argument;					   /* Borrow the caller's completion context. */
	char *transfer_id;				   /* Own the record that counts a receive against its source. */
	uint64_t token;					   /* Nonzero while the receive waits for its Worker to connect back. */
	time_t deadline;				   /* Fail a waiting receive after this time. */
	int timeout;					   /* Seconds the transfer may go without progress, from the Manager at admission. */
	struct inline_request *request;			   /* The request whose reply this receive waits for, while it waits. */
	char *data;					   /* Own the data of an inline reply to write, or borrow those of spilled. */
	struct link_writer_shared *spilled;		   /* Hold the bytes of an entry that a spill writes to disk. */
	int64_t data_length;				   /* Bytes of data. */
	int data_mode;					   /* Permission bits of the received file. */
	int spill;					   /* Set for a job that writes an entry held in memory to disk. No slot. */
	int held;					   /* Set for an inline receive whose data the vault holds in memory. */
	int64_t inline_counted;				   /* Bytes counted in inline_flight while the receive comes inline, else 0. */
	uint64_t order;					   /* Admission order of a receive, which starts at one. */
	struct transfer_job *next_file_job;		   /* The next older job of the same file in file_jobs. */
	struct transfer_job *next_source_job;		   /* Neighbors among the receives of the same source in source_jobs. */
	struct transfer_job *prev_source_job;
	int copying;		   /* Set for a receive that its Worker copies to the shared directory, which holds a copy slot. */
	int shareable;		   /* Set for a receive of a file large enough to copy, counted in shareable_receives unless copied. */
	struct copy_request *copy; /* The copy that this receive waits for, while it waits. */
	char *shared_path;	   /* Own the path of a complete copy in the shared directory until publication links it. */
	int missing;		   /* Set for a request whose vault entry did not exist when it was opened. */
};

/* A request sent to a Worker over its control connection for the data of a small file. The Worker answers each request
 * in order with "inline-data <name> <length> <mode>" and the bytes, or "inline-error <name> <errno>". The Manager reads
 * the bytes as they arrive, without waiting, so a partial reply never stalls other Workers. A request outlives a
 * receive that ends first by cancellation, so its reply is still read and then discarded. */
struct inline_request {
	struct transfer_job *job; /* The receive waiting for this reply, or NULL once it ended. */
	char *name;		  /* Own the requested cache name, which the reply repeats. */
	char *data;		  /* Own the reply's bytes as they arrive. */
	int64_t length;		  /* Bytes of the reply's data, or -1 until its header arrives. */
	int64_t received;	  /* Bytes of the data read so far. */
	int mode;		  /* Permission bits that the reply reports. */
};

/* A copy of a large checkpoint that its source Worker writes to the shared directory in a process of its own, and
 * answers with "cache-copied <token> <size>" or "cache-copy-error <token> <errno>". The Manager holds a copy slot for it,
 * but no connection, executor, or bandwidth of its own. A request outlives a receive that ends first by cancellation, so
 * the copy is removed once the Worker answered and stopped writing it. */
struct copy_request {
	struct transfer_job *job; /* The receive waiting for this copy, or NULL once it ended. */
	uint64_t token;		  /* Number of the copy, which the answer repeats. */
	char *path;		  /* Own the path in the shared directory that the Worker writes. */
};

static void vault_entry_delete(struct vault_entry *entry)
{
	if (entry->file->vault_entry == entry) {
		entry->file->vault_entry = NULL;
	}
	vine_file_delete(entry->file);
	link_writer_shared_release(entry->data);
	link_writer_shared_release(entry->on_disk);
	free(entry->shared_path);
	free(entry);
}

static void job_delete(struct transfer_job *job)
{
	if (job->link) {
		link_close(job->link);
	}
	if (job->file) {
		vine_file_delete(job->file);
	}
	free(job->password);
	free(job->name);
	free(job->ip);
	free(job->path);
	free(job->shared_path);
	free(job->transfer_id);
	if (job->spilled) {
		link_writer_shared_release(job->spilled);
	} else {
		free(job->data);
	}
	free(job);
}

/* Give a receive the vault path that its item is written to, unless it has one. Return zero if no memory was left. */
static int job_set_path(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	if (!job->path) {
		job->path = string_format("%s/%s", ds->vault_directory, job->name);
	}
	return job->path != NULL;
}

/* Give a receive over a connection the address of its source Worker and its vault path, unless it has them. Return
 * zero if no memory was left. */
static int job_set_connection(struct vine_manager_data_service *ds, struct transfer_job *job, struct vine_worker_info *worker)
{
	if (!job->ip) {
		job->ip = strdup(worker->transfer_host);
	}
	return job->ip && job_set_path(ds, job);
}

/* Return the reservation of a receive that ended, and log its outcome. Every admitted receive logs exactly one such
 * line after its "checkpoint started" line: when the Manager releases it, or when the service is deleted first. */
static void log_receive_end(struct vine_manager_data_service *ds, struct transfer_job *job, const char *outcome, int in_memory)
{
	ds->vault_reserved -= job->reserved;
	debug(D_VINE, "checkpoint %s: %s vault_used=%" PRId64 " vault_reserved=%" PRId64 " memory=%d memory_used=%" PRId64, outcome, job->name, ds->vault_used, ds->vault_reserved, in_memory, ds->memory_used);
}

static void inline_request_delete(struct inline_request *request)
{
	free(request->name);
	free(request->data);
	free(request);
}

static void copy_request_delete(struct copy_request *request)
{
	free(request->path);
	free(request);
}

/* Return non-zero for a checkpoint receive that has not been cancelled. Only the Manager thread writes the
 * cancellation flag, and the file is fixed at admission, so this needs no lock on the Manager thread. */
static int job_is_active_receive(struct transfer_job *job)
{
	return job->file && !job->spill && !job->cancelled;
}

/* Count the jobs of one direction that hold a slot: Worker requests, or receives over connections. A cancelled receive
 * releases its slot immediately. */
static unsigned slots_in_use(struct vine_manager_data_service *ds, int receives)
{
	return receives ? ds->receives_active : ds->requests_active;
}

/* Take or return what an active receive holds: a receive slot, a copy slot, or its bytes of inline receives in flight. */
static void receive_hold(struct vine_manager_data_service *ds, struct transfer_job *job, int delta)
{
	if (job->inline_counted) {
		ds->inline_flight += delta * job->inline_counted;
	} else if (job->copying) {
		ds->copies_active += delta;
	} else {
		ds->receives_active += delta;
		if (job->shareable) {
			ds->shareable_receives += delta;
		}
	}
}


/* Index a receive by its source Worker, so the receives of a removed Worker are found without a pass over all jobs. */
static void source_link(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	job->prev_source_job = NULL;
	job->next_source_job = itable_remove(ds->source_jobs, (uintptr_t)job->source);
	if (job->next_source_job) {
		job->next_source_job->prev_source_job = job;
	}
	itable_insert(ds->source_jobs, (uintptr_t)job->source, job);
}

/* Take a receive out of the index of its source Worker. */
static void source_unlink(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	if (job->next_source_job) {
		job->next_source_job->prev_source_job = job->prev_source_job;
	}
	if (job->prev_source_job) {
		job->prev_source_job->next_source_job = job->next_source_job;
	} else if (job->next_source_job) {
		itable_insert(ds->source_jobs, (uintptr_t)job->source, job->next_source_job);
	} else {
		itable_remove(ds->source_jobs, (uintptr_t)job->source);
	}
	job->next_source_job = job->prev_source_job = NULL;
}

/* Admit a job into the set of jobs, and index a job of a file by its cache name and a receive by its source Worker, so
 * the jobs of one file or one Worker are found without a pass over all jobs. A file has at most one active receive, and
 * also a cancelled one or a write to disk. */
static void job_track(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	set_insert(ds->jobs, job);
	if (!job->file) {
		ds->requests_active++;
		return;
	}
	job->file->vault_jobs++;
	job->next_file_job = hash_table_remove(ds->file_jobs, job->name);
	hash_table_insert(ds->file_jobs, job->name, job);
	if (job->source) {
		source_link(ds, job);
	}
	if (job_is_active_receive(job)) {
		receive_hold(ds, job, 1);
	}
}

/* Release a job from the set of jobs and from the indexes of its file and its source, and its slot if it still holds
 * one. */
static void job_untrack(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	set_remove(ds->jobs, job);
	if (!job->file) {
		ds->requests_active--;
		return;
	}
	if (job_is_active_receive(job)) {
		receive_hold(ds, job, -1);
	}
	if (job->source) {
		source_unlink(ds, job);
	}
	job->file->vault_jobs--;
	struct transfer_job *head = hash_table_lookup(ds->file_jobs, job->name);
	if (head == job) {
		hash_table_remove(ds->file_jobs, job->name);
		if (job->next_file_job) {
			hash_table_insert(ds->file_jobs, job->name, job->next_file_job);
		}
	} else {
		while (head && head->next_file_job != job) {
			head = head->next_file_job;
		}
		if (head) {
			head->next_file_job = job->next_file_job;
		}
	}
	job->next_file_job = NULL;
}

/* Receive a file or a directory with the Worker transfer protocol into a temporary directory beside the destination,
 * and rename only a complete item of the requested name into the vault, so a failed transfer never publishes. */
static int receive_file(struct transfer_job *job)
{
	char encoded[VINE_DATA_LINE_MAX];
	url_encode(job->name, encoded, sizeof(encoded));
	struct link_writer *lw = link_writer_create(job->link);
	if (!lw) {
		return 0;
	}
	char *request = string_format("get %s\n", encoded);
	int sent = link_writer_push(lw, request, strlen(request)) && link_writer_send_sync(lw, 0, job->timeout);
	free(request);
	link_writer_delete(lw);
	if (!sent) {
		return 0;
	}
	char *temporary = string_format("%s.XXXXXX", job->path);
	if (!mkdtemp(temporary)) {
		free(temporary);
		return 0;
	}
	int64_t size = 0;
	int mode, mtime;
	char *error = NULL;
	int got = vine_transfer_get_any(job->link, temporary, &size, &mode, &mtime, job->timeout, &error);
	/* The source cannot send an item of the file, such as one it cannot read, which a retry cannot change. */
	job->source_error = got < 0;
	int valid = got == 1;
	if (error) {
		debug(D_VINE, "checkpoint %s: %s", job->name, error);
		free(error);
	}
	char *received = string_format("%s/%s", temporary, job->name);
	pthread_mutex_lock(&job->owner->lock);
	valid = valid && !job->cancelled && rename(received, job->path) == 0;
	pthread_mutex_unlock(&job->owner->lock);
	unlink_recursive(temporary);
	free(received);
	free(temporary);
	if (valid) {
		job->received = size;
	}
	return valid;
}

/* Write data into a temporary file beside a vault path, and rename it to that path unless cancelled is set. The rename
 * happens under the lock, which the Manager thread holds when it sets the flag of a job. */
static int write_vault_file(struct vine_manager_data_service *ds, const char *path, const char *data, int64_t length, int mode, const int *cancelled)
{
	char *temporary = string_format("%s.XXXXXX", path);
	int fd = mkstemp(temporary);
	int valid = fd >= 0 && full_write(fd, data, length) == length;
	if (fd >= 0) {
		valid = fchmod(fd, mode) == 0 && valid;
		valid = close(fd) == 0 && valid;
	}
	pthread_mutex_lock(&ds->lock);
	valid = valid && !*cancelled && rename(temporary, path) == 0;
	pthread_mutex_unlock(&ds->lock);
	if (!valid && fd >= 0) {
		unlink(temporary);
	}
	free(temporary);
	return valid;
}

/* Write the data of an inline reply, or of an entry held in memory, into the vault unless the job was cancelled
 * meanwhile. An executor writes it, so a slow disk never stalls the Manager thread. */
static int write_data(struct transfer_job *job)
{
	int valid = write_vault_file(job->owner, job->path, job->data, job->data_length, job->data_mode, &job->cancelled);
	if (valid) {
		job->received = job->data_length;
	}
	return valid;
}

/* Send a vault entry with the Worker transfer protocol (vine_transfer_send.c), as a Worker sends a cached file, so a
 * declared file is sent as the file it links to. An entry that cannot be sent is reported as an error, which fails the
 * receiver. The writer fails when no data moves for the transfer timeout, however large the entry. */
static int send_entry(struct transfer_job *job, const char *path)
{
	struct link_writer *lw = link_writer_create(job->link);
	if (!lw) {
		return 0;
	}
	int result = vine_transfer_queue_item(lw, path, job->name, VINE_TRANSFER_MODE_ANY);
	if (result == ENOENT || result == ESTALE) {
		/* The entry itself is gone, such as a copy deleted from the shared directory outside the vault. */
		job->missing = 1;
	}
	if (result > 0) {
		vine_transfer_queue_error(lw, job->name, result);
	}
	int valid = link_writer_send_sync(lw, 0, job->timeout) && result == 0;
	link_writer_delete(lw);
	return valid;
}

/* Return a finished job to the Manager thread. Called with the lock held. */
static void job_finish_locked(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	if (job->link) {
		link_close(job->link);
		job->link = NULL;
	}
	list_push_tail(ds->ready, job);
	/* At most one notification per admitted job is outstanding. */
	ssize_t result;
	do {
		result = write(ds->notify_fd, "x", 1);
	} while (result < 0 && errno == EINTR);
}

/* Fail a receive that still waits for its Worker to connect back. Called with the lock held. */
static void stop_waiting_locked(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	itable_remove(ds->waiting, job->token);
	job->token = 0;
	job->success = 0;
	job_finish_locked(ds, job);
}

static void transfer_run(void *argument);
static void queue_removal(struct vine_manager_data_service *ds, char *path);
static void fall_back_to_connection(struct vine_manager *manager, struct transfer_job *job, struct vine_worker_info *worker);

/* Hand the connection of a Worker that connected back to the receive waiting for it. The receive then runs on an
 * executor of its own, so the connection holds a request slot only until it is identified. */
static int attach_connect_back(struct transfer_job *job, uint64_t token)
{
	struct vine_manager_data_service *ds = job->owner;
	pthread_mutex_lock(&ds->lock);
	struct transfer_job *receive = token ? itable_lookup(ds->waiting, token) : NULL;
	if (receive && !ds->stopping) {
		itable_remove(ds->waiting, token);
		receive->token = 0;
		receive->link = job->link;
		job->link = NULL;
	} else {
		receive = NULL;
	}
	pthread_mutex_unlock(&ds->lock);
	if (!receive) {
		debug(D_VINE, "data service: no receive waits for connect-back token %" PRIu64, token);
		return 0;
	}
	debug(D_VINE, "checkpoint connected back: %s", receive->name);
	if (!thread_pool_submit(ds->threads, transfer_run, receive)) {
		pthread_mutex_lock(&ds->lock);
		receive->success = 0;
		job_finish_locked(ds, receive);
		pthread_mutex_unlock(&ds->lock);
	}
	return 1;
}

/* Serve one Worker request from the vault without any Manager table lookup, or pass a connection that a Worker opened
 * to send a file to the receive waiting for it. */
static int serve_request(struct transfer_job *job)
{
	struct vine_manager_data_service *ds = job->owner;
	char line[VINE_DATA_LINE_MAX], encoded[VINE_DATA_LINE_MAX];
	int used = 0;
	uint64_t token = 0;
	int valid = (!job->password || link_auth_password(job->link, job->password, time(NULL) + job->timeout)) && link_readline(job->link, line, sizeof(line), time(NULL) + job->timeout);
	if (valid && sscanf(line, "connect-back %" SCNu64 "%n", &token, &used) == 1 && !line[used]) {
		return attach_connect_back(job, token);
	}
	valid = valid && sscanf(line, "get %4095s%n", encoded, &used) == 1 && !line[used];
	if (!valid) {
		return 0;
	}
	char name[VINE_DATA_LINE_MAX];
	url_decode(encoded, name, sizeof(name));
	job->name = strdup(name);
	if (!job->name) {
		return 0;
	}
	int valid_name = job->name[0] && !strchr(job->name, '/') && strcmp(job->name, ".") && strcmp(job->name, "..");
	char *path = valid_name ? string_format("%s/%s", ds->vault_directory, job->name) : NULL;
	valid = send_entry(job, path ? path : "");
	free(path);
	return valid;
}

static void transfer_run(void *argument)
{
	struct transfer_job *job = argument;
	struct vine_manager_data_service *ds = job->owner;
	pthread_mutex_lock(&ds->lock);
	int stopping = ds->stopping || job->cancelled;
	/* A Worker that connected back has already opened and authenticated the connection. */
	struct link *link = job->link;
	pthread_mutex_unlock(&ds->lock);
	if (stopping) {
		job->success = 0;
	} else if (job->data) {
		job->success = write_data(job);
	} else if (job->file && link) {
		job->success = receive_file(job);
	} else if (job->file) {
		link = link_connect(job->ip, job->port, time(NULL) + job->timeout);
		pthread_mutex_lock(&ds->lock);
		job->link = link;
		stopping = ds->stopping || job->cancelled;
		pthread_mutex_unlock(&ds->lock);
		job->success = link && !stopping && (!job->password || link_auth_password(link, job->password, time(NULL) + job->timeout)) && receive_file(job);
	} else {
		job->success = serve_request(job);
	}
	pthread_mutex_lock(&ds->lock);
	job_finish_locked(ds, job);
	pthread_mutex_unlock(&ds->lock);
}

/* Hand an admitted job to an executor. A receive from a Worker that connects back waits without an executor until its
 * connection arrives. Return zero if no executor took the job. */
static int job_start(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	if (job->token) {
		pthread_mutex_lock(&ds->lock);
		itable_insert(ds->waiting, job->token, job);
		pthread_mutex_unlock(&ds->lock);
		return 1;
	}
	return thread_pool_submit(ds->threads, transfer_run, job);
}

/* Admit a job and start it, except a receive over the control connection or a copy to the shared directory, which
 * waits for its Worker's answer without an executor. On failure the job is released and zero is returned. */
static int job_submit(struct vine_manager *manager, struct transfer_job *job)
{
	struct vine_manager_data_service *ds = manager->ds;
	job->owner = ds;
	/* A transfer fails after min-transfer-timeout without progress, or T of its worker with the timeout framework on.
	 * The source is still a valid worker here. */
	job->timeout = vine_manager_worker_timeout(manager, job->source, manager->minimum_transfer_timeout);
	job->password = manager->password ? strdup(manager->password) : NULL;
	job_track(ds, job);
	if (!job->request && !job->copy && !job_start(ds, job)) {
		job_untrack(ds, job);
		job_delete(job);
		return 0;
	}
	return 1;
}

/* Fail a receive that has no executor, on the Manager thread. */
static void fail_receive(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	pthread_mutex_lock(&ds->lock);
	job->success = 0;
	job_finish_locked(ds, job);
	pthread_mutex_unlock(&ds->lock);
}

/* Cancel a receive without waiting for it. Its socket is shut down so a connected executor returns promptly, and a
 * receive that waits for its Worker to connect back or for an inline reply finishes at once. The reply of an inline
 * request is then read and discarded. */
static void cancel_receive(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	pthread_mutex_lock(&ds->lock);
	if (job_is_active_receive(job)) {
		receive_hold(ds, job, -1);
	}
	job->cancelled = 1;
	if (job->token) {
		stop_waiting_locked(ds, job);
	} else if (job->request) {
		job->request->job = NULL;
		job->request = NULL;
		job->success = 0;
		job_finish_locked(ds, job);
	} else if (job->copy) {
		/* The Worker may still write the copy, so its request stays until the answer, which removes the copy. */
		job->copy->job = NULL;
		job->copy = NULL;
		job->success = 0;
		job_finish_locked(ds, job);
	} else if (job->link) {
		shutdown(link_fd(job->link), SHUT_RDWR);
	}
	pthread_mutex_unlock(&ds->lock);
}

/* Fail receives whose Worker did not connect back in time. They are few, at most one per receive slot, and they are
 * checked at most once a second. */
static void expire_connect_backs(struct vine_manager_data_service *ds)
{
	time_t now = time(NULL);
	if (now < ds->next_expiry) {
		return;
	}
	ds->next_expiry = now + 1;
	struct list *expired = list_create();
	pthread_mutex_lock(&ds->lock);
	int iteration;
	uint64_t token;
	struct transfer_job *job;
	ITABLE_ITERATE(ds->waiting, iteration, token, job)
	{
		if (job->deadline <= now) {
			list_push_tail(expired, job);
		}
	}
	while ((job = list_pop_head(expired))) {
		debug(D_VINE, "checkpoint %s: its Worker did not connect back", job->name);
		stop_waiting_locked(ds, job);
	}
	pthread_mutex_unlock(&ds->lock);
	list_delete(expired);
}

struct vine_manager_data_service *vine_manager_data_service_create(const char *runtime_directory)
{
	struct vine_manager_data_service *ds = calloc(1, sizeof(*ds));
	if (!ds) {
		return NULL;
	}
	ds->notify_fd = -1;
	pthread_mutex_init(&ds->lock, NULL);
	ds->jobs = set_create(0);
	ds->file_jobs = hash_table_create(0, 0);
	ds->source_jobs = itable_create(0);
	ds->removals = list_create();
	ds->ready = list_create();
	ds->vault_table = hash_table_create(0, 0);
	ds->waiting = itable_create(0);
	ds->inline_requests = itable_create(0);
	ds->copy_requests = itable_create(0);
	ds->next_token = (uint64_t)random_int64() | 1;
	ds->next_copy = (uint64_t)random_int64() | 1;
	ds->memory_limit = VINE_DATA_MEMORY_LIMIT;
	ds->shared_min_bytes = VINE_DATA_SHARED_MIN_BYTES;
	ds->vault_directory = string_format("%s/staging/vault-XXXXXX", runtime_directory);
	if (!mkdtemp(ds->vault_directory)) {
		free(ds->vault_directory);
		ds->vault_directory = NULL;
		vine_manager_data_service_delete(ds);
		return NULL;
	}
	int descriptors[2];
	if (pipe(descriptors) < 0) {
		vine_manager_data_service_delete(ds);
		return NULL;
	}
	ds->notification = link_attach_to_fd(descriptors[0]);
	ds->notify_fd = descriptors[1];
	fcntl(descriptors[0], F_SETFD, FD_CLOEXEC);
	fcntl(descriptors[1], F_SETFD, FD_CLOEXEC);
	fcntl(descriptors[1], F_SETFL, O_NONBLOCK);
	ds->listener = link_serve(0);
	if (!ds->listener) {
		const char *low = getenv("TCP_LOW_PORT");
		const char *high = getenv("TCP_HIGH_PORT");
		debug(D_NOTICE, "no free port for the manager's data listener in the port range %s-%s; give the manager a range with a free port besides its own", low ? low : "default", high ? high : "default");
	}
	/* Let many Workers wait for a slot in the kernel instead of retrying dropped connections. SOMAXCONN is only 128,
	 * so ask for more, and the kernel lowers it to net.core.somaxconn. */
	if (ds->listener) {
		listen(link_fd(ds->listener), 4096);
	}
	/* Cancelled receives release their slots before their executors return, so allow one spare executor per slot. */
	ds->threads = thread_pool_create(VINE_DATA_THREADS);
	if (!ds->listener || !ds->notification || !ds->threads || !ds->vault_table || !ds->waiting || !ds->inline_requests || !ds->copy_requests || !ds->source_jobs) {
		vine_manager_data_service_delete(ds);
		return NULL;
	}
	return ds;
}

void vine_manager_data_service_delete(struct vine_manager_data_service *ds)
{
	if (!ds) {
		return;
	}
	pthread_mutex_lock(&ds->lock);
	ds->stopping = 1;
	struct transfer_job *job;
	int iteration;
	SET_ITERATE(ds->jobs, iteration, job)
	{
		if (job->link) {
			shutdown(link_fd(job->link), SHUT_RDWR);
		}
	}
	pthread_mutex_unlock(&ds->lock);
	if (ds->threads) {
		thread_pool_delete(ds->threads);
	}
	/* Every executor has returned. A receive that the Manager has not released, because it was still running, waiting
	 * for its Worker to connect back or for an inline reply, or ready but not yet taken by a Manager step, ends here as
	 * cancelled. Nothing is published or reported, because the Manager and the callers' contexts are going away. */
	while ((job = set_pop(ds->jobs))) {
		if (job->file && !job->spill) {
			log_receive_end(ds, job, "cancelled", 0);
		}
		job_delete(job);
	}
	set_delete(ds->jobs);
	if (ds->file_jobs) {
		hash_table_delete(ds->file_jobs);
	}
	if (ds->source_jobs) {
		itable_delete(ds->source_jobs);
	}
	/* Paths that no executor took are inside the vault, which is deleted below. */
	list_clear(ds->removals, free);
	list_delete(ds->removals);
	list_delete(ds->ready);
	if (ds->waiting) {
		itable_delete(ds->waiting);
	}
	if (ds->inline_requests) {
		int iteration;
		uint64_t worker;
		struct list *requests;
		ITABLE_ITERATE(ds->inline_requests, iteration, worker, requests)
		{
			list_clear(requests, (void *)inline_request_delete);
			list_delete(requests);
		}
		itable_delete(ds->inline_requests);
	}
	if (ds->copy_requests) {
		int iteration;
		uint64_t worker;
		struct list *requests;
		ITABLE_ITERATE(ds->copy_requests, iteration, worker, requests)
		{
			list_clear(requests, (void *)copy_request_delete);
			list_delete(requests);
		}
		itable_delete(ds->copy_requests);
	}
	if (ds->listener) {
		link_close(ds->listener);
	}
	if (ds->notification) {
		link_close(ds->notification);
	}
	if (ds->notify_fd >= 0) {
		close(ds->notify_fd);
	}
	if (ds->vault_table) {
		hash_table_clear(ds->vault_table, (void *)vault_entry_delete);
		hash_table_delete(ds->vault_table);
	}
	if (ds->vault_directory) {
		unlink_recursive(ds->vault_directory);
		free(ds->vault_directory);
	}
	/* Copies that no executor deleted, and copies that Workers wrote after their requests ended, are removed with the
	 * directory. */
	if (ds->shared_directory) {
		if (unlink_recursive(ds->shared_directory) != 0 && errno != ENOENT) {
			debug(D_NOTICE | D_VINE, "could not remove the vault's shared directory %s: %s", ds->shared_directory, strerror(errno));
		}
		free(ds->shared_directory);
	}
	pthread_mutex_destroy(&ds->lock);
	free(ds);
}

int vine_manager_data_service_port(struct vine_manager_data_service *ds)
{
	char address[LINK_ADDRESS_MAX];
	int port = 0;
	link_address_local(ds->listener, address, &port);
	return port;
}

void vine_manager_data_service_poll(struct vine_manager_data_service *ds, struct link_poll_set *set)
{
	/* Watch the listener while a request slot is free. Otherwise connections wait in the kernel backlog without
	 * waking the Manager repeatedly. */
	int accepting = slots_in_use(ds, 0) < VINE_DATA_REQUEST_SLOTS;
	link_poll_set_add(set, ds->listener, accepting ? LINK_READ : 0, NULL);
	link_poll_set_add(set, ds->notification, LINK_READ, NULL);
}

/* Index a complete vault entry so the Manager thread can query it without touching the filesystem, and count its size
 * in vault_used. A small entry came inline and goes to Workers inline (vine_manager_data_service_vault_small_file()).
 * Return the new entry, or NULL when the name already had one. */
static struct vault_entry *vault_insert(struct vine_manager_data_service *ds, struct vine_file *file, int64_t size, int small)
{
	if (hash_table_lookup(ds->vault_table, file->cached_name)) {
		return NULL;
	}
	struct vault_entry *entry = calloc(1, sizeof(*entry));
	if (!entry) {
		return NULL;
	}
	entry->file = vine_file_addref(file);
	entry->size = size;
	entry->small = small;
	hash_table_insert(ds->vault_table, file->cached_name, entry);
	file->vault_entry = entry;
	ds->vault_used += size;
	return entry;
}

/* Append an entry held in memory to the list of entries that may be written to disk. */
static void memory_list_push(struct vine_manager_data_service *ds, struct vault_entry *entry)
{
	entry->older = ds->memory_newest;
	entry->newer = NULL;
	if (ds->memory_newest) {
		ds->memory_newest->newer = entry;
	} else {
		ds->memory_oldest = entry;
	}
	ds->memory_newest = entry;
}

/* Take an entry out of the list of entries that may be written to disk. */
static void memory_list_remove(struct vine_manager_data_service *ds, struct vault_entry *entry)
{
	if (entry->older) {
		entry->older->newer = entry->newer;
	} else if (ds->memory_oldest == entry) {
		ds->memory_oldest = entry->newer;
	}
	if (entry->newer) {
		entry->newer->older = entry->older;
	} else if (ds->memory_newest == entry) {
		ds->memory_newest = entry->older;
	}
	entry->older = entry->newer = NULL;
}

/* Return the bytes of an entry held in memory, or NULL. */
static const char *memory_bytes(struct vault_entry *entry)
{
	return entry->data ? link_writer_shared_bytes(entry->data) : NULL;
}

/* Forget the bytes of an entry held in memory, whose data is now on disk at the given path, or no longer needed when
 * path is NULL. A job writing them to disk is cancelled, and its completion is ignored because the entry no longer
 * refers to it. Worker queues that still hold the data of an entry on disk read it from there when it reaches their
 * head, so the vault's memory accounts for every byte of checkpoints in memory but those of removed entries. */
static void memory_release(struct vine_manager_data_service *ds, struct vault_entry *entry, const char *path)
{
	if (!entry->data) {
		return;
	}
	if (entry->spill) {
		pthread_mutex_lock(&ds->lock);
		entry->spill->cancelled = 1;
		pthread_mutex_unlock(&ds->lock);
		entry->spill = NULL;
		ds->memory_spilling -= entry->size;
		/* The job may still be reading the bytes, so queues that hold them keep them. */
		path = NULL;
	} else {
		memory_list_remove(ds, entry);
	}
	if (path && link_writer_shared_unload(entry->data, path)) {
		entry->on_disk = entry->data;
	} else {
		link_writer_shared_release(entry->data);
	}
	entry->data = NULL;
	ds->memory_used -= entry->size;
}

/* While checkpoints held in memory exceed the memory limit, write the oldest ones to disk on executors, at most
 * VINE_DATA_SPILLS at once. An entry stays readable from memory until its write completes. */
static void memory_spill(struct vine_manager_data_service *ds)
{
	while (ds->memory_used - ds->memory_spilling > ds->memory_limit && ds->spills < VINE_DATA_SPILLS && ds->memory_oldest) {
		struct vault_entry *entry = ds->memory_oldest;
		struct transfer_job *job = calloc(1, sizeof(*job));
		if (!job) {
			return;
		}
		job->owner = ds;
		job->spill = 1;
		job->file = vine_file_addref(entry->file);
		job->path = string_format("%s/%s", ds->vault_directory, entry->file->cached_name);
		job->name = strdup(entry->file->cached_name);
		if (!job->name) {
			job_delete(job);
			return;
		}
		/* The job holds the bytes with its own reference, so they outlive a removal of the entry during the write. */
		job->spilled = link_writer_shared_hold(entry->data);
		job->data = (char *)link_writer_shared_bytes(entry->data);
		job->data_length = entry->size;
		job->data_mode = entry->mode;
		job_track(ds, job);
		if (!thread_pool_submit(ds->threads, transfer_run, job)) {
			job_untrack(ds, job);
			job_delete(job);
			return;
		}
		memory_list_remove(ds, entry);
		entry->spill = job;
		ds->spills++;
		ds->memory_spilling += entry->size;
	}
}

/* Release a finished write of an entry held in memory. An entry that still refers to the job is on disk now, or keeps
 * its bytes in memory when the write failed, and is not written again. */
static void finish_spill(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	ds->spills--;
	struct vault_entry *entry = job->removed ? NULL : hash_table_lookup(ds->vault_table, job->name);
	if (!entry || entry->spill != job) {
		return;
	}
	entry->spill = NULL;
	ds->memory_spilling -= entry->size;
	if (job->success) {
		/* The job no longer reads the bytes, so they leave memory unless queues are writing them. The entry is in no
		 * list, so the release takes it out of none. */
		memory_release(ds, entry, job->path);
		debug(D_VINE, "vault entry %s: written to disk memory_used=%" PRId64, job->name, ds->memory_used);
	} else {
		debug(D_VINE, "vault entry %s: could not be written to disk, so it stays in memory", job->name);
	}
}

/* Link a copy that its Worker wrote to the shared directory into the vault under its cache name, as a declared file is
 * linked, so the vault serves it, gives its path, and removes it like any entry. Return zero when it cannot be linked. */
static int link_shared_copy(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	if (!job_set_path(ds, job)) {
		return 0;
	}
	if (symlink(job->shared_path, job->path) != 0) {
		debug(D_VINE, "checkpoint %s: could not link its copy %s: %s", job->name, job->shared_path, strerror(errno));
		return 0;
	}
	return 1;
}

/* Release a finished receive: return its reservation, publish its file, and report the result.
 * Success means the file was renamed into the vault before any cancellation, or that the data of an inline reply is
 * held in memory. A removed receive, whose name vault removal already unlinked, never publishes. */
static void finish_receive(struct vine_manager *manager, struct transfer_job *job)
{
	struct vine_manager_data_service *ds = manager->ds;
	int published = job->success && !job->removed;
	int in_memory = 0;
	if (published && job->shared_path && !link_shared_copy(ds, job)) {
		published = 0;
	}
	struct link_writer_shared *held = NULL;
	if (published && job->held) {
		/* The data of an inline reply was not written: the entry holds it. A receive that cannot keep it fails. */
		held = link_writer_shared_create(job->data, job->data_length);
		if (held) {
			job->data = NULL;
		} else {
			published = 0;
		}
	}
	if (published) {
		struct vault_entry *entry = vault_insert(ds, job->file, job->received, job->data != NULL || held);
		if (entry && entry->small) {
			entry->mode = job->data_mode;
		}
		if (entry && held) {
			entry->data = held;
			held = NULL;
			memory_list_push(ds, entry);
			ds->memory_used += entry->size;
			in_memory = 1;
		}
		if (entry && job->shared_path) {
			entry->shared_path = job->shared_path;
			job->shared_path = NULL;
			entry->complete = job->complete;
			entry->argument = job->argument;
		}
	}
	/* A copy that was not published is removed. */
	if (job->shared_path) {
		queue_removal(ds, job->shared_path);
		job->shared_path = NULL;
	}
	link_writer_shared_release(held);
	/* The vault is a source of a published file, so ready tasks that wait for one may run, and tasks that waited for
	 * the file to come inline use another source when it did not. */
	vine_schedule_wake_declared_file(manager, job->file);
	/* A receive withdrawn by vault removal, such as the pruning of its file, is cancelled, not failed. */
	log_receive_end(ds, job, published ? "complete" : (job->removed ? "cancelled" : "failed"), in_memory);
	if (job->transfer_id) {
		/* A record already removed because its source Worker disconnected is ignored. */
		vine_current_transfers_remove(manager, job->transfer_id);
	}
	if (job->complete) {
		job->complete(job->argument, job->file, published ? 1 : (job->source_error && !job->removed ? -1 : 0));
	}
	if (in_memory) {
		memory_spill(ds);
	}
}

/* A Worker asked for a vault entry that was gone. A copy in the shared directory, which others than the vault may
 * delete, is no longer a copy of the file: the entry is removed, so the Manager recreates the file for the tasks that
 * read it, and the receive's caller learns that the vault lost it. Other entries are the vault's own, and stay. */
static void lose_entry(struct vine_manager *manager, const char *name)
{
	struct vine_manager_data_service *ds = manager->ds;
	struct vault_entry *entry = hash_table_lookup(ds->vault_table, name);
	struct stat info;
	if (!entry || !entry->shared_path || stat(entry->shared_path, &info) == 0 || (errno != ENOENT && errno != ESTALE)) {
		return;
	}
	debug(D_NOTICE | D_VINE, "vault entry %s lost: its copy %s is gone", name, entry->shared_path);
	struct vine_file *file = vine_file_addref(entry->file);
	void (*complete)(void *, struct vine_file *, int) = entry->complete;
	void *argument = entry->argument;
	vine_manager_data_service_vault_remove(ds, file);
	/* The vault no longer serves the file, so tasks that wait for it look for another source, or for its recovery. */
	vine_schedule_wake_declared_file(manager, file);
	if (complete) {
		complete(argument, file, -2);
	}
	vine_file_delete(file);
}

void vine_manager_data_service_handle(struct vine_manager *manager)
{
	struct vine_manager_data_service *ds = manager->ds;
	expire_connect_backs(ds);
	for (unsigned i = 0; i < VINE_DATA_THREADS; i++) {
		pthread_mutex_lock(&ds->lock);
		struct transfer_job *job = list_pop_head(ds->ready);
		pthread_mutex_unlock(&ds->lock);
		if (!job) {
			break;
		}
		char notification;
		read(link_fd(ds->notification), &notification, 1);
		job_untrack(ds, job);
		if (job->spill) {
			finish_spill(ds, job);
		} else if (job->file) {
			finish_receive(manager, job);
		} else if (job->missing && job->name) {
			lose_entry(manager, job->name);
		}
		job_delete(job);
	}
	memory_spill(ds);
	/* Connections are accepted only when the last poll found the listener readable, which saves a failed accept in
	 * every step. The listener does not block, so accepting stops at the first connection that is not pending. */
	int pending = manager->poll_set && link_poll_set_take(manager->poll_set, ds->listener, LINK_READ);
	while (pending && slots_in_use(ds, 0) < VINE_DATA_REQUEST_SLOTS) {
		struct link *link = link_accept(ds->listener, LINK_NOWAIT);
		if (!link) {
			break;
		}
		struct transfer_job *job = calloc(1, sizeof(*job));
		if (!job) {
			link_close(link);
			break;
		}
		job->link = link;
		job_submit(manager, job);
	}
}

/* Prepare a receive over a connection of its own: a Worker that connects back gets a token to present. */
static void prepare_connection(struct vine_manager *manager, struct transfer_job *job, struct vine_worker_info *worker)
{
	struct vine_manager_data_service *ds = manager->ds;
	if (!worker->transfer_connect_back) {
		return;
	}
	job->token = ds->next_token++;
	if (!ds->next_token) {
		ds->next_token = 1;
	}
	/* The Worker answers the request and connects within the minimum transfer timeout, or T with the timeout framework
	 * on. Its connection may then wait in the listener backlog while every request slot is busy, so it is allowed
	 * another one. */
	job->deadline = time(NULL) + 2 * vine_manager_worker_timeout(manager, worker, manager->minimum_transfer_timeout);
}

/* Count a started receive over a connection among the checkpoints its Worker sends, and ask a Worker that connects back
 * to connect. */
static void start_connection(struct vine_manager *manager, struct transfer_job *job, struct vine_worker_info *worker)
{
	struct vine_manager_data_service *ds = manager->ds;
	/* The record has no destination Worker and never penalizes the source on failure. */
	char *source = string_format("%s/%s", worker->transfer_url, job->name);
	job->transfer_id = vine_current_transfers_add_checkpoint(manager, worker, source, job->name);
	free(source);
	/* The Worker connects to the data port and sends the token before it answers the native get. */
	if (job->token && vine_manager_send(manager, worker, "connect-back %" PRIu64 "\n", job->token) < 0) {
		pthread_mutex_lock(&ds->lock);
		if (job->token) {
			stop_waiting_locked(ds, job);
		}
		pthread_mutex_unlock(&ds->lock);
	}
}

/* Ask the source Worker for the data of a small file over its control connection. The request joins the Worker's list
 * of requests, whose replies arrive in order. A request that cannot be sent fails its receive at once. */
static void send_inline_request(struct vine_manager *manager, struct transfer_job *job, struct vine_worker_info *worker)
{
	struct vine_manager_data_service *ds = manager->ds;
	struct inline_request *request = job->request;
	char encoded[VINE_DATA_LINE_MAX];
	url_encode(job->name, encoded, sizeof(encoded));
	if (vine_manager_send(manager, worker, "inline-get %s %d\n", encoded, VINE_DATA_INLINE_BYTES) < 0) {
		job->request = NULL;
		inline_request_delete(request);
		fail_receive(ds, job);
		return;
	}
	struct list *requests = itable_lookup(ds->inline_requests, (uintptr_t)worker);
	if (!requests) {
		requests = list_create();
		itable_insert(ds->inline_requests, (uintptr_t)worker, requests);
	}
	list_push_tail(requests, request);
}

/* Ask the source Worker to copy a large file to the shared directory in the background. Its answer links the copy into
 * the vault (vine_manager_data_service_copy_reply()). The copy is counted among the checkpoints its Worker sends. A
 * request that cannot be sent fails its receive at once. */
static void send_copy_request(struct vine_manager *manager, struct transfer_job *job, struct vine_worker_info *worker)
{
	struct vine_manager_data_service *ds = manager->ds;
	struct copy_request *request = job->copy;
	char name[VINE_DATA_LINE_MAX], path[VINE_DATA_LINE_MAX];
	url_encode(job->name, name, sizeof(name));
	url_encode(request->path, path, sizeof(path));
	if (vine_manager_send(manager, worker, "cache-copy %" PRIu64 " %s %s\n", request->token, name, path) < 0) {
		job->copy = NULL;
		copy_request_delete(request);
		fail_receive(ds, job);
		return;
	}
	char *source = string_format("%s/%s", worker->transfer_url, job->name);
	job->transfer_id = vine_current_transfers_add_checkpoint(manager, worker, source, job->name);
	free(source);
	struct list *requests = itable_lookup(ds->copy_requests, (uintptr_t)worker);
	if (!requests) {
		requests = list_create();
		itable_insert(ds->copy_requests, (uintptr_t)worker, requests);
	}
	list_push_tail(requests, request);
}

static int copy_request_has_token(void *request, const void *token)
{
	return ((struct copy_request *)request)->token == *(const uint64_t *)token;
}

int vine_manager_data_service_copy_reply(struct vine_manager *manager, struct vine_worker_info *w, const char *line)
{
	struct vine_manager_data_service *ds = manager->ds;
	uint64_t token;
	int64_t size = 0;
	int error = 0, used = 0;
	if (sscanf(line, "cache-copied %" SCNu64 " %" SCNd64 "%n", &token, &size, &used) == 2 && !line[used] && size >= 0) {
		error = 0;
	} else if (sscanf(line, "cache-copy-error %" SCNu64 " %d%n", &token, &error, &used) == 2 && !line[used] && error > 0) {
		size = 0;
	} else {
		return 0;
	}
	/* A Worker has few copies in progress, so its list is short. */
	struct list *requests = ds ? itable_lookup(ds->copy_requests, (uintptr_t)w) : NULL;
	struct copy_request *request = requests ? list_find(requests, copy_request_has_token, &token) : NULL;
	if (!request) {
		debug(D_VINE, "copy answer from %s without a request: %s", w->addrport, line);
		return 0;
	}
	list_remove(requests, request);
	struct transfer_job *job = request->job;
	char *path = request->path;
	request->path = NULL;
	copy_request_delete(request);
	if (!job || error) {
		/* A copy whose receive ended is removed, and so is a failed one, which a copy process that was killed leaves. */
		queue_removal(ds, path);
		path = NULL;
	}
	if (!job) {
		return 1;
	}
	job->copy = NULL;
	if (!error) {
		job->shared_path = path;
		job->success = 1;
		job->received = size;
		job_untrack(ds, job);
		finish_receive(manager, job);
		job_delete(job);
	} else if (error == ENOENT) {
		/* Like a failed transfer: the file left the Worker's cache. */
		debug(D_VINE, "checkpoint %s: not copied to the shared directory: %s", job->name, strerror(error));
		fail_receive(ds, job);
	} else {
		/* A file that is not a regular file, or a shared directory that cannot take it, comes over a connection. */
		debug(D_VINE, "checkpoint %s: %s", job->name, strerror(error));
		fall_back_to_connection(manager, job, w);
	}
	return 1;
}

/* Receive a file over a connection that its Worker would not send inline, too large or not a regular file, or did not
 * copy to the shared directory. */
static void fall_back_to_connection(struct vine_manager *manager, struct transfer_job *job, struct vine_worker_info *worker)
{
	debug(D_VINE, "checkpoint %s: not %s, receiving it over a connection", job->name, job->copying ? "copied to the shared directory" : "sent inline");
	/* The receive takes a receive slot now, even beyond their number, because it was admitted already. Tasks that wait
	 * for it to come inline are woken, because it does not anymore. */
	receive_hold(manager->ds, job, -1);
	job->inline_counted = 0;
	job->copying = 0;
	receive_hold(manager->ds, job, 1);
	vine_schedule_wake_file(manager, job->name);
	if (job->transfer_id) {
		vine_current_transfers_remove(manager, job->transfer_id);
		free(job->transfer_id);
		job->transfer_id = NULL;
	}
	if (!job_set_connection(manager->ds, job, worker)) {
		fail_receive(manager->ds, job);
		return;
	}
	prepare_connection(manager, job, worker);
	if (!job_start(manager->ds, job)) {
		fail_receive(manager->ds, job);
		return;
	}
	start_connection(manager, job, worker);
}

/* Parse "inline-data <name> <length> <mode>" with the name of fewer than size bytes, the length in decimal, the mode in
 * octal, and nothing after it, as sscanf did with "inline-data %4095s %lld %o%n". Return one if the line matches. It
 * is parsed without sscanf because a worker sends one for every small checkpoint. */
static int inline_data_header(const char *line, char *encoded, size_t size, long long *length, int *mode)
{
	if (!string_prefix_is(line, "inline-data ")) {
		return 0;
	}
	const char *s = line + strlen("inline-data ");
	while (*s == ' ') {
		s++;
	}
	size_t n = strcspn(s, " ");
	if (n == 0 || n >= size) {
		return 0;
	}
	memcpy(encoded, s, n);
	encoded[n] = 0;
	s += n;

	char *end;
	errno = 0;
	*length = strtoll(s, &end, 10);
	if (end == s || errno || *end != ' ') {
		return 0;
	}
	s = end;
	long value = strtol(s, &end, 8);
	if (end == s || errno || *end || value < INT_MIN || value > INT_MAX) {
		return 0;
	}
	*mode = value;
	return 1;
}

int vine_manager_data_service_inline_reply(struct vine_manager *manager, struct vine_worker_info *w, const char *line)
{
	struct vine_manager_data_service *ds = manager->ds;
	struct list *requests = ds ? itable_lookup(ds->inline_requests, (uintptr_t)w) : NULL;
	struct inline_request *request = requests ? list_peek_head(requests) : NULL;
	char encoded[VINE_DATA_LINE_MAX], name[VINE_DATA_LINE_MAX];
	long long length;
	int mode, error, used = 0;
	if (!request || request->length >= 0) {
		debug(D_VINE, "inline reply from %s without a request: %s", w->addrport, line);
		return 0;
	}
	if (inline_data_header(line, encoded, sizeof(encoded), &length, &mode)) {
		url_decode(encoded, name, sizeof(name));
		if (strcmp(name, request->name) || length < 0 || length > VINE_DATA_INLINE_BYTES) {
			debug(D_VINE, "inline reply from %s does not match its request for %s: %s", w->addrport, request->name, line);
			return 0;
		}
		request->data = malloc(length + 1);
		if (!request->data) {
			return 0;
		}
		request->length = length;
		request->mode = mode & 0777;
		/* The data usually arrived with the header. Whatever remains is read when it arrives. */
		return vine_manager_data_service_inline_read(manager, w, LINK_NOWAIT) >= 0;
	}
	if (sscanf(line, "inline-error %4095s %d%n", encoded, &error, &used) == 2 && !line[used]) {
		url_decode(encoded, name, sizeof(name));
		if (strcmp(name, request->name)) {
			debug(D_VINE, "inline reply from %s does not match its request for %s: %s", w->addrport, request->name, line);
			return 0;
		}
		list_pop_head(requests);
		struct transfer_job *job = request->job;
		inline_request_delete(request);
		if (!job) {
			return 1;
		}
		job->request = NULL;
		if (error == EFBIG || error == EISDIR) {
			fall_back_to_connection(manager, job, w);
		} else {
			/* Like a failed transfer: the file may have left the Worker's cache. A file that the Worker cannot read
			 * cannot be sent by it however often it is asked. */
			debug(D_VINE, "checkpoint %s: %s", job->name, strerror(error));
			job->source_error = error == EACCES;
			fail_receive(ds, job);
		}
		return 1;
	}
	return 0;
}

int vine_manager_data_service_inline_pending(struct vine_manager *manager, struct vine_worker_info *w)
{
	struct vine_manager_data_service *ds = manager->ds;
	struct list *requests = ds ? itable_lookup(ds->inline_requests, (uintptr_t)w) : NULL;
	struct inline_request *request = requests ? list_peek_head(requests) : NULL;
	return request && request->length >= 0;
}

int vine_manager_data_service_inline_read(struct vine_manager *manager, struct vine_worker_info *w, time_t stoptime)
{
	if (!vine_manager_data_service_inline_pending(manager, w)) {
		return 1;
	}
	struct vine_manager_data_service *ds = manager->ds;
	struct list *requests = itable_lookup(ds->inline_requests, (uintptr_t)w);
	struct inline_request *request = list_peek_head(requests);
	while (request->received < request->length) {
		ssize_t actual = link_read_avail(w->link, request->data + request->received, request->length - request->received, stoptime);
		if (actual > 0) {
			request->received += actual;
		} else if (actual < 0 && errno == ETIMEDOUT && stoptime == LINK_NOWAIT) {
			return 0;
		} else {
			debug(D_VINE, "failed to read the inline data of %s from %s", request->name, w->addrport);
			return -1;
		}
	}
	list_pop_head(requests);
	struct transfer_job *job = request->job;
	if (job) {
		job->request = NULL;
		job->data = request->data;
		job->data_length = request->length;
		job->data_mode = request->mode;
		request->data = NULL;
	}
	inline_request_delete(request);
	if (job && ds->memory_limit > 0) {
		/* The vault holds the data in memory, so the receive is published now, without an executor or the disk. */
		job->held = 1;
		job->success = 1;
		job->received = job->data_length;
		job_untrack(ds, job);
		finish_receive(manager, job);
		job_delete(job);
	} else if (job && (!job_set_path(ds, job) || !job_start(ds, job))) {
		/* An executor writes the data into the vault. */
		fail_receive(ds, job);
	}
	return 1;
}

/* How a receive is admitted. A required receive is admitted beyond the vault limit and the bound of inline receives
 * in flight. An urgent one only beyond that bound. */
typedef enum {
	RECEIVE_CHECKPOINT = 0,
	RECEIVE_URGENT = 1,
	RECEIVE_REQUIRED = 2,
} receive_kind_t;

/* Return a Worker that holds a READY replica of the file and can send it to the vault, or NULL. Neither kind of
 * checkpoint counts against the limits on the transfers that a source serves to other Workers, so a Worker that anchors
 * its outputs still serves the tasks that read them, and the reverse. A Worker answers an inline request in its main
 * loop over its control connection, without limit, and the first eligible holder from a random start sends it: under
 * the transfer limits, a small result whose Worker was sending a 64 MB checkpoint waited for that checkpoint, up to a
 * minute into a busy vault disk. A large file, copied to the shared directory or received over a connection, needs a
 * Worker that sends fewer than worker-source-max-checkpoints checkpoints and had no transfer failure within the
 * transient error interval. While checkpoints counted among a source's transfers, one 128 MiB copy held every reader of
 * the source back for its 16 s, and readers held back anchoring.
 *
 * A large file also leaves through its source's network link, which the Workers of one host share with the readers they
 * serve, so it is sent from a holder whose host sends nothing to other Workers, the fewest bytes, files, and then
 * checkpoints first. Checkpoints of a host only rank it, because they slow each other but no reader. When the host of
 * every holder sends files to other Workers, a checkpoint waits (busy is set and NULL returned) for the transfers in
 * progress at its first such refusal (file->checkpoint_deferred), not for ones that start later, so it waits at most
 * for those transfers to end, which they do within the peer transfer timeout or with their Worker's removal. A fetch,
 * which the application needs now, takes the least loaded holder at once. Decided on 2026-10-23: on the cluster, hosts
 * with 1 Gb/s links ran up to 5 Workers, and readers of a file whose host was copying a checkpoint to the shared
 * directory moved 24 MB/s instead of 78.
 *
 * A host whose link is far slower than the others' (vine_current_transfers_host_rate()) takes several times as long for
 * a copy, during which the sends to the readers of its other files share its link, so a holder on such a host comes
 * after every holder on another host, it sends one checkpoint at a time, and it sends only while its host sends
 * nothing to other Workers, for up to VINE_DATA_SLOW_HOST_WAIT after the first refusal, and then as any host. Decided
 * on 2026-10-24: on the cluster, two hosts with 1 Gb/s links copied 32 MB checkpoints in 0.65 s (0.16 s elsewhere),
 * 40% of them begun while their hosts sent files to readers, and a send that overlapped a copy took 0.96 s instead of
 * 0.45 s. Without the limit of one, the checkpoints that waited the full time left together, up to 10 at once, and
 * each copy took 1.7 s. Costs O(holders). */
static struct vine_worker_info *find_checkpoint_source(struct vine_manager *manager, struct vine_file *file, int inline_receive, receive_kind_t kind, int *busy)
{
	*busy = 0;
	struct set *workers = hash_table_lookup(manager->file_worker_table, file->cached_name);
	if (!workers || set_size(workers) < 1) {
		return NULL;
	}
	timestamp_t now = timestamp_get();
	struct vine_worker_info *best = NULL;
	int best_idle = 0;
	int best_slow = 0;
	int best_sends = 0;
	int64_t best_bytes = 0;
	int best_checkpoints = 0;
	struct vine_worker_info *worker;
	int offset_bookkeep;
	int iteration;
	SET_ITERATE_RANDOM_START(workers, offset_bookkeep, iteration, worker)
	{
		if (worker->pending_removal || vine_worker_source_is_suspect(worker)) {
			continue;
		}
		if (!inline_receive && (!worker->transfer_port_active || worker->outgoing_checkpoints >= manager->worker_source_max_checkpoints || now - worker->last_transfer_failure < manager->transient_error_interval)) {
			continue;
		}
		struct vine_file_replica *replica = vine_file_replica_table_lookup(worker, file->cached_name);
		if (!replica || replica->state != VINE_FILE_REPLICA_STATE_READY) {
			continue;
		}
		if (inline_receive) {
			return worker;
		}
		int sends;
		int64_t bytes;
		int checkpoints;
		timestamp_t oldest = vine_current_transfers_host_load(worker, &sends, &bytes, &checkpoints);
		int slow;
		vine_current_transfers_host_rate(manager, worker, &slow);
		if (!oldest && !checkpoints && !slow) {
			/* Its host sends nothing. */
			return worker;
		}
		/* A host idle for this file serves no reader, or only readers that came after the file's first refusal. A slow
		 * host is idle only when it sends no checkpoint, and serves no reader until the file waited
		 * VINE_DATA_SLOW_HOST_WAIT. */
		int patient = slow && (!file->checkpoint_deferred || now - file->checkpoint_deferred < VINE_DATA_SLOW_HOST_WAIT);
		int idle = (!oldest || (!patient && file->checkpoint_deferred && oldest >= file->checkpoint_deferred)) && !(slow && checkpoints);
		if (!best || slow < best_slow || (slow == best_slow && (idle > best_idle || (idle == best_idle && (bytes < best_bytes || (bytes == best_bytes && (sends < best_sends || (sends == best_sends && checkpoints < best_checkpoints)))))))) {
			best = worker;
			best_idle = idle;
			best_slow = slow;
			best_sends = sends;
			best_bytes = bytes;
			best_checkpoints = checkpoints;
		}
	}
	if (best && !best_idle && kind == RECEIVE_CHECKPOINT) {
		if (!file->checkpoint_deferred) {
			file->checkpoint_deferred = now;
			debug(D_VINE, "checkpoint of %s waits for the transfers to workers of the hosts that hold it%s", file->cached_name, best_slow ? ", whose links are slow" : "");
		}
		*busy = 1;
		return NULL;
	}
	return best;
}

/* Admit a receive of a temporary file into the vault. */
static vine_manager_checkpoint_result_t admit_receive(struct vine_manager *manager, struct vine_file *file, receive_kind_t kind, void (*complete)(void *, struct vine_file *, int), void *argument)
{
	struct vine_manager_data_service *ds = manager->ds;
	if (!file || file->type != VINE_TEMP) {
		return VINE_CHECKPOINT_NO_SOURCE;
	}
	if (vine_manager_data_service_vault_contains(manager, file)) {
		if (complete) {
			complete(argument, file, 1);
		}
		return VINE_CHECKPOINT_ADMITTED;
	}
	/* A cancelled receive will never publish, so a new request starts another one. A receive that the Manager started
	 * without a callback reports to the first caller that gives one. */
	struct transfer_job *job;
	for (job = hash_table_lookup(ds->file_jobs, file->cached_name); job; job = job->next_file_job) {
		if (job->file == file && job_is_active_receive(job)) {
			if (!job->complete) {
				job->complete = complete;
				job->argument = argument;
			}
			return VINE_CHECKPOINT_ADMITTED;
		}
	}
	int64_t size = (int64_t)file->size;
	/* A small file comes over the control connection of its Worker, as a small input goes the other way. */
	int inline_receive = size <= VINE_DATA_INLINE_BYTES;
	int64_t counted = MAX(size, VINE_DATA_INLINE_MINIMUM);
	/* Only a ready task's wait for a file that comes inline makes it urgent. Other files keep the checkpoint order. */
	if (kind == RECEIVE_URGENT && !inline_receive) {
		return VINE_CHECKPOINT_BUSY;
	}
	/* A large file that its Worker may copy to the shared directory is copied while a copy slot is free, and otherwise
	 * comes over a connection while fewer than VINE_DATA_SHARED_RECEIVES such files do. Others come over a connection. */
	int shareable = !inline_receive && ds->shared_directory && size >= ds->shared_min_bytes;
	int copy = shareable && ds->copies_active < VINE_DATA_COPY_SLOTS;
	if (shareable && !copy && ds->shareable_receives >= VINE_DATA_SHARED_RECEIVES) {
		return VINE_CHECKPOINT_BUSY;
	}
	if (inline_receive ? kind == RECEIVE_CHECKPOINT && ds->inline_flight + counted > VINE_DATA_INLINE_FLIGHT : !copy && slots_in_use(ds, 1) >= VINE_DATA_RECEIVE_SLOTS) {
		return VINE_CHECKPOINT_BUSY;
	}
	if (kind != RECEIVE_REQUIRED && ds->vault_limit > 0 && ds->vault_used + ds->vault_reserved + size > ds->vault_limit) {
		return VINE_CHECKPOINT_FULL;
	}
	int busy;
	struct vine_worker_info *worker = find_checkpoint_source(manager, file, inline_receive, kind, &busy);
	if (busy) {
		return VINE_CHECKPOINT_BUSY;
	}
	if (!worker || strlen(file->cached_name) >= VINE_DATA_LINE_MAX / 3) {
		return VINE_CHECKPOINT_NO_SOURCE;
	}
	job = calloc(1, sizeof(*job));
	if (!job) {
		return VINE_CHECKPOINT_BUSY;
	}
	job->source = worker;
	job->port = worker->transfer_port;
	job->name = strdup(file->cached_name);
	job->file = vine_file_addref(file);
	/* A receive that comes inline and stays in memory, or that its Worker copies to the shared directory, needs neither the
	 * source's address nor a vault path, so they are made only for a receive over a connection, a write to disk, or the
	 * link to a copy (job_set_connection(), job_set_path()). */
	if (!job->name || (!inline_receive && !copy && !job_set_connection(ds, job, worker))) {
		job_delete(job);
		return VINE_CHECKPOINT_BUSY;
	}
	job->reserved = size;
	job->shareable = shareable;
	job->complete = complete;
	job->argument = argument;
	job->order = ++ds->next_order;
	struct inline_request *request = NULL;
	if (inline_receive) {
		request = calloc(1, sizeof(*request));
		if (!request) {
			job_delete(job);
			return VINE_CHECKPOINT_BUSY;
		}
		request->name = strdup(file->cached_name);
		request->length = -1;
		request->job = job;
		job->request = request;
		job->inline_counted = counted;
	} else if (copy) {
		struct copy_request *copy_request = calloc(1, sizeof(*copy_request));
		if (copy_request) {
			copy_request->token = ds->next_copy++;
			copy_request->path = string_format("%s/%02x/%016" PRIx64, ds->shared_directory, (unsigned)(copy_request->token % VINE_DATA_SHARED_SUBDIRECTORIES), copy_request->token);
		}
		if (!copy_request || !copy_request->path) {
			free(copy_request);
			job_delete(job);
			return VINE_CHECKPOINT_BUSY;
		}
		if (!ds->next_copy) {
			ds->next_copy = 1;
		}
		copy_request->job = job;
		job->copy = copy_request;
		job->copying = 1;
	} else {
		prepare_connection(manager, job, worker);
	}
	if (!job_submit(manager, job)) {
		return VINE_CHECKPOINT_BUSY;
	}
	ds->vault_reserved += size;
	if (file->checkpoint_deferred) {
		debug(D_VINE, "checkpoint of %s waited %.3f s for the transfers to workers of the hosts that hold it", file->cached_name, (timestamp_get() - file->checkpoint_deferred) / 1000000.0);
		file->checkpoint_deferred = 0;
	}
	debug(D_VINE, "checkpoint started: %s from %s:%d size=%" PRId64 " required=%d urgent=%d connect_back=%d inline=%d shared=%d vault_used=%" PRId64 " vault_reserved=%" PRId64 " vault_limit=%" PRId64, file->cached_name, worker->transfer_host, worker->transfer_port, size, kind == RECEIVE_REQUIRED, kind == RECEIVE_URGENT, worker->transfer_connect_back, inline_receive, copy, ds->vault_used, ds->vault_reserved, ds->vault_limit);
	if (inline_receive) {
		send_inline_request(manager, job, worker);
	} else if (copy) {
		send_copy_request(manager, job, worker);
	} else {
		start_connection(manager, job, worker);
	}
	return VINE_CHECKPOINT_ADMITTED;
}

vine_manager_checkpoint_result_t vine_manager_data_service_checkpoint(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument)
{
	return admit_receive(manager, file, RECEIVE_CHECKPOINT, complete, argument);
}

vine_manager_checkpoint_result_t vine_manager_data_service_checkpoint_input(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument)
{
	return admit_receive(manager, file, RECEIVE_URGENT, complete, argument);
}

vine_manager_checkpoint_result_t vine_manager_data_service_fetch(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument)
{
	return admit_receive(manager, file, RECEIVE_REQUIRED, complete, argument);
}

uint64_t vine_manager_data_service_receiving_inline(struct vine_manager *manager, struct vine_file *file)
{
	if (!manager || !manager->ds || !file || file->type != VINE_TEMP || file->vault_entry || file->size > VINE_DATA_INLINE_BYTES) {
		return 0;
	}
	struct transfer_job *job;
	for (job = hash_table_lookup(manager->ds->file_jobs, file->cached_name); job; job = job->next_file_job) {
		if (job->file == file && job_is_active_receive(job)) {
			return job->inline_counted ? job->order : 0;
		}
	}
	return 0;
}

/* Declare Manager-local data served through the vault, such as application inputs.
 * It is a VINE_FILE because its bytes already exist at the Manager and never need to be recomputed. Task outputs
 * remain VINE_TEMP even after a checkpoint places a copy in the vault.
 * The cache name is random so declaration never reads the file to compute a checksum. */
struct vine_file *vine_manager_data_service_declare_file(struct vine_manager *manager, const char *source_path)
{
	char *source = realpath(source_path, NULL);
	if (!source) {
		return NULL;
	}
	struct stat info;
	if (stat(source, &info) != 0 || !S_ISREG(info.st_mode)) {
		free(source);
		return NULL;
	}

	struct vine_file probe = {.type = VINE_FILE};
	char *cached_name = vine_random_name(&probe);
	struct vine_file *file = vine_file_create(source, cached_name, 0, info.st_size, VINE_FILE, 0, VINE_CACHE_LEVEL_WORKFLOW, 0);
	free(cached_name);
	if (!file) {
		free(source);
		return NULL;
	}
	file->mtime = info.st_mtime;
	file->mode = info.st_mode & 0777;
	char *path = string_format("%s/%s", manager->ds->vault_directory, file->cached_name);
	int result = symlink(source, path);
	free(path);
	free(source);
	if (result != 0) {
		vine_file_delete(file);
		return NULL;
	}
	file = vine_manager_declare_file(manager, file);
	vault_insert(manager->ds, file, 0, 0);
	return file;
}

/* Delete the directory entries that vault removal renamed, one after another, on one executor. */
static void delete_removed_entries(void *argument)
{
	struct vine_manager_data_service *ds = argument;
	pthread_mutex_lock(&ds->lock);
	char *path;
	while ((path = list_pop_head(ds->removals))) {
		pthread_mutex_unlock(&ds->lock);
		unlink_recursive(path);
		free(path);
		pthread_mutex_lock(&ds->lock);
	}
	ds->removing = 0;
	pthread_mutex_unlock(&ds->lock);
}

/* Delete a path, which the queue takes, on one executor after the paths queued before it, off the Manager thread. A path
 * that no executor takes is inside the vault or its shared directory, which are deleted when the service ends. */
static void queue_removal(struct vine_manager_data_service *ds, char *path)
{
	if (!path) {
		return;
	}
	pthread_mutex_lock(&ds->lock);
	list_push_tail(ds->removals, path);
	int start = !ds->removing;
	ds->removing = 1;
	pthread_mutex_unlock(&ds->lock);
	if (start && !thread_pool_submit(ds->threads, delete_removed_entries, ds)) {
		pthread_mutex_lock(&ds->lock);
		ds->removing = 0;
		pthread_mutex_unlock(&ds->lock);
	}
}

/* Remove the item at a vault path without waiting for the deletion of a directory. A file or a link is unlinked at
 * once. A directory, which may hold many entries, is renamed to a name that no cache name takes, so the name is free
 * for a new entry at once, and an executor deletes it. */
static void remove_entry_path(struct vine_manager_data_service *ds, const char *path)
{
	if (unlink(path) == 0 || errno == ENOENT) {
		return;
	}
	if (errno != EISDIR && errno != EPERM) {
		debug(D_VINE, "could not remove vault entry %s: %s", path, strerror(errno));
		return;
	}
	char *removed = string_format("%s.removed.%" PRIu64, path, ds->next_removal++);
	if (rename(path, removed) != 0) {
		debug(D_VINE, "could not remove vault entry %s: %s", path, strerror(errno));
		free(removed);
		return;
	}
	queue_removal(ds, removed);
}

void vine_manager_data_service_vault_remove(struct vine_manager_data_service *ds, struct vine_file *file)
{
	if (!ds || !file || !file->cached_name) {
		return;
	}
	/* Every entry of the vault table is the vault_entry of its file, and the file counts its jobs, so a file with
	 * neither has nothing to remove and needs no lookup by name. Most removals are such: an output pruned again when
	 * it is undeclared after its release, and a task's buffer when it is undeclared. */
	if (!file->vault_entry && !file->vault_jobs) {
		return;
	}
	/* A cancelled receive can never publish: the executor checks the flag under the lock before connecting, before
	 * receiving, and before renaming its item into the vault. So no receive is joined: its executor deletes its
	 * partial item on its own, and an item renamed into the vault before the cancellation is removed below. The
	 * callback is dropped because the caller may release its context next. */
	int retrieving = 0;
	struct transfer_job *job;
	for (job = file->vault_jobs ? hash_table_lookup(ds->file_jobs, file->cached_name) : NULL; job; job = job->next_file_job) {
		if (job->file != file) {
			continue;
		}
		retrieving = 1;
		job->complete = NULL;
		job->removed = 1;
		cancel_receive(ds, job);
	}
	/* A receive may have published its file before the Manager thread indexed it, so remove the name either way. An
	 * entry held only in memory was never written, so it has no name to remove. A write of it to disk was found as a
	 * job above. Worker queues that still hold the data of a small entry on disk get it in memory first, one file of
	 * at most VINE_DATA_INLINE_BYTES, because they read the vault path only when the data reaches their head. */
	struct vault_entry *held = file->vault_entry ? hash_table_remove(ds->vault_table, file->cached_name) : NULL;
	if (held && held->on_disk && !link_writer_shared_load(held->on_disk)) {
		debug(D_NOTICE, "could not read vault entry %s for the sends that wait for it", file->cached_name);
	}
	if ((held && (!held->data || held->spill)) || retrieving) {
		char *path = string_format("%s/%s", ds->vault_directory, file->cached_name);
		remove_entry_path(ds, path);
		free(path);
	}
	if (held) {
		/* The vault name of a copy in the shared directory is a link, unlinked above, and the copy is deleted off the
		 * Manager thread, because a network file system may be slow to answer. */
		queue_removal(ds, held->shared_path);
		held->shared_path = NULL;
		memory_release(ds, held, NULL);
		ds->vault_used -= MIN(ds->vault_used, held->size);
		vault_entry_delete(held);
	}
}

int vine_manager_data_service_compact(struct vine_manager_data_service *ds)
{
	if (!ds) {
		return 0;
	}
	/* Only the Manager thread uses these tables. The table of receives waiting for a connect-back, which executors
	 * share, holds at most the receive slots and is compacted by its own inserts. */
	int freed = hash_table_compact(ds->vault_table);
	freed += hash_table_compact(ds->file_jobs);
	freed += set_compact(ds->jobs);
	freed += itable_compact(ds->source_jobs);
	freed += itable_compact(ds->inline_requests);
	freed += itable_compact(ds->copy_requests);
	return freed;
}

void vine_manager_data_service_remove_worker(struct vine_manager_data_service *ds, struct vine_worker_info *w)
{
	if (!ds || !w) {
		return;
	}
	/* Only the receives from this Worker are visited. Each leaves the index and forgets its source, and an active one
	 * is cancelled, which reports it failed in the next step. */
	struct transfer_job *job = itable_remove(ds->source_jobs, (uintptr_t)w);
	while (job) {
		struct transfer_job *next = job->next_source_job;
		job->next_source_job = job->prev_source_job = NULL;
		job->source = NULL;
		if (job_is_active_receive(job)) {
			debug(D_VINE, "checkpoint %s: cancelled because its source %s was removed", job->name, w->addrport);
			cancel_receive(ds, job);
		}
		job = next;
	}
	/* Receives that waited for replies of this Worker were cancelled above, so its requests hold only data. */
	struct list *requests = itable_remove(ds->inline_requests, (uintptr_t)w);
	if (requests) {
		list_clear(requests, (void *)inline_request_delete);
		list_delete(requests);
	}
	/* The Worker kills its copies when it leaves the Manager, so the copies it has not answered are removed. */
	requests = itable_remove(ds->copy_requests, (uintptr_t)w);
	if (requests) {
		struct copy_request *request;
		while ((request = list_pop_head(requests))) {
			queue_removal(ds, request->path);
			request->path = NULL;
			copy_request_delete(request);
		}
		list_delete(requests);
	}
}

/* Every declared file is one object per cache name, which the entry of its name refers to and points back from
 * vault_entry, so the queries below read the pointer instead of looking up the name. */
int vine_manager_data_service_vault_contains(struct vine_manager *manager, struct vine_file *file)
{
	return manager && manager->ds && file && file->vault_entry;
}

int vine_manager_data_service_vault_small_file(struct vine_manager *manager, struct vine_file *file)
{
	if (!manager || !manager->ds || !file) {
		return 0;
	}
	struct vault_entry *entry = file->vault_entry;
	return entry && entry->small;
}

struct link_writer_shared *vine_manager_data_service_vault_shared(struct vine_manager *manager, struct vine_file *file, int64_t *length, int *mode)
{
	if (!vine_manager_data_service_vault_small_file(manager, file)) {
		return NULL;
	}
	struct vault_entry *entry = file->vault_entry;
	if (!entry->data && !entry->on_disk) {
		char *path = string_format("%s/%s", manager->ds->vault_directory, file->cached_name);
		entry->on_disk = link_writer_shared_create_file(path, entry->size);
		free(path);
		if (!entry->on_disk) {
			return NULL;
		}
	}
	*length = entry->size;
	*mode = entry->mode;
	return entry->data ? entry->data : entry->on_disk;
}

const char *vine_manager_data_service_vault_data(struct vine_manager *manager, struct vine_file *file, int64_t *length, int *mode)
{
	if (!manager || !manager->ds || !file) {
		return NULL;
	}
	struct vault_entry *entry = file->vault_entry;
	const char *data = entry ? memory_bytes(entry) : NULL;
	if (data) {
		*length = entry->size;
		if (mode) {
			*mode = entry->mode;
		}
	}
	return data;
}

char *vine_manager_data_service_vault_path(struct vine_manager *manager, struct vine_file *file)
{
	if (!vine_manager_data_service_vault_contains(manager, file)) {
		return NULL;
	}
	struct vine_manager_data_service *ds = manager->ds;
	struct vault_entry *entry = file->vault_entry;
	char *path = string_format("%s/%s", ds->vault_directory, file->cached_name);
	const char *data = memory_bytes(entry);
	if (data) {
		/* The caller reads the path now, so the Manager thread writes this one small file itself, instead of waiting
		 * for an executor. A job writing the same bytes is cancelled, and renames them first or not at all. */
		int cancelled = 0;
		if (!write_vault_file(ds, path, data, entry->size, entry->mode, &cancelled)) {
			debug(D_NOTICE, "could not write vault entry %s: %s", path, strerror(errno));
			free(path);
			return NULL;
		}
		memory_release(ds, entry, path);
		debug(D_VINE, "vault entry %s: written to disk for a local path memory_used=%" PRId64, file->cached_name, ds->memory_used);
	}
	return path;
}

void vine_manager_data_service_vault_set_limit(struct vine_manager_data_service *ds, int64_t bytes)
{
	if (ds) {
		ds->vault_limit = MAX(0, bytes);
	}
}

void vine_manager_data_service_vault_set_memory_limit(struct vine_manager_data_service *ds, int64_t bytes)
{
	if (ds) {
		ds->memory_limit = MAX(0, bytes);
		memory_spill(ds);
	}
}

int vine_manager_data_service_set_shared_directory(struct vine_manager_data_service *ds, const char *path)
{
	if (!ds || !path) {
		errno = EINVAL;
		return 0;
	}
	if (ds->shared_directory) {
		errno = EEXIST;
		return 0;
	}
	/* Workers write at this path, so it is absolute and the same on every host. */
	char *root = realpath(path, NULL);
	if (!root) {
		return 0;
	}
	char *directory = string_format("%s/vine-vault-XXXXXX", root);
	free(root);
	if (strlen(directory) >= VINE_DATA_LINE_MAX / 4) {
		free(directory);
		errno = ENAMETOOLONG;
		return 0;
	}
	if (!mkdtemp(directory)) {
		int error = errno;
		free(directory);
		errno = error;
		return 0;
	}
	for (int i = 0; i < VINE_DATA_SHARED_SUBDIRECTORIES; i++) {
		char *subdirectory = string_format("%s/%02x", directory, i);
		int made = subdirectory && mkdir(subdirectory, 0700) == 0;
		int error = errno;
		free(subdirectory);
		if (!made) {
			unlink_recursive(directory);
			free(directory);
			errno = error;
			return 0;
		}
	}
	ds->shared_directory = directory;
	debug(D_VINE, "vault shared directory %s for checkpoints of at least %" PRId64 " bytes", directory, ds->shared_min_bytes);
	return 1;
}

void vine_manager_data_service_set_shared_min_bytes(struct vine_manager_data_service *ds, int64_t bytes)
{
	if (ds) {
		ds->shared_min_bytes = MAX(0, bytes);
	}
}

int64_t vine_manager_data_service_vault_get_usage(struct vine_manager_data_service *ds)
{
	return ds ? ds->vault_used + ds->vault_reserved : 0;
}
