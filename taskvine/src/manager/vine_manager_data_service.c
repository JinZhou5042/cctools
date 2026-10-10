/* Serve vault entries to Workers and fetch Worker files into the vault through the native get protocol. */
#include "vine_manager_data_service.h"
#include "vine_manager.h"
#include "vine_file.h"
#include "vine_file_replica_table.h"
#include "vine_worker_info.h"
#include "debug.h"
#include "hash_table.h"
#include "link.h"
#include "link_auth.h"
#include "list.h"
#include "macros.h"
#include "stringtools.h"
#include "thread_pool.h"
#include "url_encode.h"
#include "vine_cached_name.h"
#include "vine_current_transfers.h"
#include "unlink_recursive.h"

#include <errno.h>
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

/* Default data connection budget and idle timeout in seconds. */
#define VINE_DATA_CONNECTIONS_DEFAULT 16U
#define VINE_DATA_IDLE_SECONDS 60

/* Slots that checkpoint receives never use, so arriving Worker requests find a free slot without preemption. */
#define VINE_DATA_WORKER_RESERVED_SLOTS 4U

/* Seconds a checkpoint receive may spend connecting to its source Worker. */
#define VINE_DATA_CHECKPOINT_CONNECT_SECONDS 5

/* Own the data listener, bounded jobs, the index of vault entries, and the vault disk budget. */
struct vine_manager_data_service {
	char *vault_directory;		/* Own the fixed directory of published cache names. */
	struct hash_table *vault_table; /* Maps cache name -> vine_file with a complete vault entry. Holds a reference. */
	int64_t vault_limit;		/* Maximum bytes of checkpoint entries. Zero means unlimited. */
	int64_t vault_used;		/* Bytes of published checkpoint entries. */
	int64_t vault_reserved;		/* Bytes reserved by checkpoint receives in progress. */
	struct link *listener;		/* Accept incoming Worker data connections. */
	struct link *notification;	/* Wake the Manager when an executor finishes a step. */
	int notify_fd;			/* Write completion notifications to this pipe. */
	struct thread_pool *threads;	/* Execute both transfer directions. */
	struct list *jobs;		/* Own all admitted jobs until the Manager releases them. */
	struct list *ready;		/* Queue jobs returned by executors. */
	pthread_mutex_t lock;		/* Protect the ready queue, cancellation, completion, and active links. */
	pthread_cond_t finished;	/* Wake vault removal after a cancelled receive finishes. */
	int stopping;			/* Prevent new connections while executors are being stopped. */
};

/* Hold one transfer until its completion is consumed by the Manager. A job with a file is a checkpoint receive
 * that fetches the file from a Worker into the vault. A job without one serves a Worker request from the vault. */
struct transfer_job {
	struct vine_manager_data_service *owner;	   /* Borrow the service that owns this job. */
	int done;					   /* Set under the lock when the executor finishes. */
	struct link *link;				   /* Own the active peer connection. */
	struct vine_file *file;				   /* Hold the checkpointed file. NULL for a Worker request. */
	char *password;					   /* Copy the Manager password for peer authentication. */
	char name[VINE_DATA_LINE_MAX];			   /* Identify the requested cache object. */
	char *ip;					   /* Own the source Worker address for a receive. */
	int port;					   /* Identify the source Worker data port. */
	char *path;					   /* Own the vault path of a receive. */
	int cancelled;					   /* Prevent a cancelled receive from renaming its file. */
	int removed;					   /* Mark a receive whose vault entry was removed. Never published. */
	int success;					   /* Report whether the transfer completed. */
	int64_t reserved;				   /* Own vault bytes reserved for a receive. */
	int64_t received;				   /* Record the bytes published by a successful receive. */
	void (*complete)(void *, struct vine_file *, int); /* Report a checkpoint result on the Manager thread. */
	void *argument;					   /* Borrow the caller's completion context. */
	char *transfer_id;				   /* Own the record that counts a receive against its source. */
};

static void job_delete(struct transfer_job *job)
{
	if (job->link) {
		link_close(job->link);
	}
	if (job->file) {
		vine_file_delete(job->file);
	}
	free(job->password);
	free(job->ip);
	free(job->path);
	free(job->transfer_id);
	free(job);
}

/* Return non-zero for a checkpoint receive that has not been cancelled. Only the Manager thread writes the
 * cancellation flag, and the file is fixed at admission, so this needs no lock on the Manager thread. */
static int job_is_active_receive(struct transfer_job *job)
{
	return job->file && !job->cancelled;
}

/* Count jobs that still hold a transfer slot, and optionally the receives among them.
 * A cancelled receive releases its slot immediately. */
static unsigned active_job_count(struct vine_manager_data_service *ds, unsigned *receives)
{
	unsigned count = 0;
	unsigned receiving = 0;
	struct transfer_job *job;
	LIST_ITERATE(ds->jobs, job)
	{
		if (!job->cancelled) {
			count++;
			receiving += job->file != NULL;
		}
	}
	if (receives) {
		*receives = receiving;
	}
	return count;
}

/* Stage incoming bytes beside the destination so failed transfers never replace it. */
static int receive_file(struct transfer_job *job)
{
	char line[VINE_DATA_LINE_MAX], encoded[VINE_DATA_LINE_MAX], name[VINE_DATA_LINE_MAX];
	int64_t size;
	int mode, mtime;
	time_t stoptime = time(NULL) + 900;
	url_encode(job->name, encoded, sizeof(encoded));
	if (link_printf(job->link, time(NULL) + 3600, "get %s\n", encoded) < 0 ||
			!link_readline(job->link, line, sizeof(line), stoptime) ||
			sscanf(line, "file %4095s %" SCNd64 " %o %d", encoded, &size, &mode, &mtime) != 4 || size < 0) {
		return 0;
	}
	url_decode(encoded, name, sizeof(name));
	if (strcmp(name, job->name)) {
		return 0;
	}
	char *temporary = string_format("%s.XXXXXX", job->path);
	int fd = mkstemp(temporary);
	int valid = fd >= 0 && link_stream_to_fd(job->link, fd, size, stoptime) == size;
	if (fd >= 0) {
		valid = valid && fchmod(fd, mode & 0777) == 0;
		if (close(fd) != 0) {
			valid = 0;
		}
	}
	pthread_mutex_lock(&job->owner->lock);
	valid = valid && !job->cancelled && rename(temporary, job->path) == 0;
	pthread_mutex_unlock(&job->owner->lock);
	unlink(temporary);
	free(temporary);
	if (valid) {
		job->received = size;
	}
	return valid;
}

/* Send an open vault entry, or report that the requested name has no vault entry. */
static int send_file(struct transfer_job *job, int fd)
{
	struct stat info;
	char encoded[VINE_DATA_LINE_MAX];
	url_encode(job->name, encoded, sizeof(encoded));
	time_t stoptime = time(NULL) + 900;
	int valid = fd >= 0 && fstat(fd, &info) == 0 && S_ISREG(info.st_mode);
	if (valid) {
		valid = link_printf(job->link, stoptime, "file %s %" PRId64 " %o %ld\n", encoded, (int64_t)info.st_size, info.st_mode & 0777, (long)info.st_mtime) >= 0 &&
			link_stream_from_fd(job->link, fd, info.st_size, stoptime) == info.st_size;
	} else {
		link_printf(job->link, stoptime, "error %s %d\n", encoded, ENOENT);
	}
	if (fd >= 0) {
		close(fd);
	}
	return valid;
}

/* Serve one Worker request from the vault without any Manager table lookup. */
static int serve_request(struct transfer_job *job)
{
	struct vine_manager_data_service *ds = job->owner;
	char line[VINE_DATA_LINE_MAX], encoded[VINE_DATA_LINE_MAX];
	int used = 0;
	int valid = (!job->password || link_auth_password(job->link, job->password, time(NULL) + 5)) &&
		    link_readline(job->link, line, sizeof(line), time(NULL) + VINE_DATA_IDLE_SECONDS) &&
		    sscanf(line, "get %4095s%n", encoded, &used) == 1 && !line[used];
	if (!valid) {
		return 0;
	}
	url_decode(encoded, job->name, sizeof(job->name));
	int fd = -1;
	if (job->name[0] && !strchr(job->name, '/') && strcmp(job->name, ".") && strcmp(job->name, "..")) {
		char *path = string_format("%s/%s", ds->vault_directory, job->name);
		fd = open(path, O_RDONLY | O_CLOEXEC);
		free(path);
	}
	return send_file(job, fd);
}

static void transfer_run(void *argument)
{
	struct transfer_job *job = argument;
	struct vine_manager_data_service *ds = job->owner;
	pthread_mutex_lock(&ds->lock);
	int stopping = ds->stopping || job->cancelled;
	pthread_mutex_unlock(&ds->lock);
	if (stopping) {
		job->success = 0;
	} else if (job->file) {
		struct link *link = link_connect(job->ip, job->port, time(NULL) + VINE_DATA_CHECKPOINT_CONNECT_SECONDS);
		pthread_mutex_lock(&ds->lock);
		job->link = link;
		stopping = ds->stopping || job->cancelled;
		pthread_mutex_unlock(&ds->lock);
		job->success = link && !stopping && (!job->password || link_auth_password(link, job->password, time(NULL) + 5)) && receive_file(job);
	} else {
		job->success = serve_request(job);
	}
	pthread_mutex_lock(&ds->lock);
	if (job->link) {
		link_close(job->link);
		job->link = NULL;
	}
	job->done = 1;
	list_push_tail(ds->ready, job);
	pthread_cond_broadcast(&ds->finished);
	/* At most one notification per admitted job is outstanding. */
	ssize_t result;
	do {
		result = write(ds->notify_fd, "x", 1);
	} while (result < 0 && errno == EINTR);
	pthread_mutex_unlock(&ds->lock);
}

/* Admit a job and hand it to an executor. On failure the job is released and zero is returned. */
static int job_submit(struct vine_manager *manager, struct transfer_job *job)
{
	struct vine_manager_data_service *ds = manager->ds;
	job->owner = ds;
	job->password = manager->password ? strdup(manager->password) : NULL;
	list_push_tail(ds->jobs, job);
	if (!thread_pool_submit(ds->threads, transfer_run, job)) {
		list_remove(ds->jobs, job);
		job_delete(job);
		return 0;
	}
	return 1;
}

/* Cancel a receive without waiting for it. Its socket is shut down so a connected executor returns promptly. */
static void cancel_receive(struct vine_manager_data_service *ds, struct transfer_job *job)
{
	pthread_mutex_lock(&ds->lock);
	job->cancelled = 1;
	if (job->link) {
		shutdown(link_fd(job->link), SHUT_RDWR);
	}
	pthread_mutex_unlock(&ds->lock);
}

/* Give a Worker request priority by cancelling the most recently admitted receive, which has the least progress.
 * A receive that already finished only waits for the Manager thread and frees no executor, so it is skipped.
 * The receive reports failure through its callback so the caller can request it again later. */
static int preempt_receive(struct vine_manager_data_service *ds)
{
	struct transfer_job *youngest = NULL;
	struct transfer_job *job;
	pthread_mutex_lock(&ds->lock);
	LIST_ITERATE(ds->jobs, job)
	{
		if (job_is_active_receive(job) && !job->done) {
			youngest = job;
		}
	}
	pthread_mutex_unlock(&ds->lock);
	if (!youngest) {
		return 0;
	}
	debug(D_VINE, "checkpoint preempted: %s", youngest->name);
	cancel_receive(ds, youngest);
	return 1;
}

struct vine_manager_data_service *vine_manager_data_service_create(const char *runtime_directory)
{
	struct vine_manager_data_service *ds = calloc(1, sizeof(*ds));
	if (!ds) {
		return NULL;
	}
	ds->notify_fd = -1;
	pthread_mutex_init(&ds->lock, NULL);
	pthread_cond_init(&ds->finished, NULL);
	ds->jobs = list_create();
	ds->ready = list_create();
	ds->vault_table = hash_table_create(0, 0);
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
	/* Let many Workers wait for a slot in the kernel instead of retrying dropped connections. */
	if (ds->listener) {
		listen(link_fd(ds->listener), SOMAXCONN);
	}
	/* Cancelled receives release their slots before their executors return, so allow one spare executor per slot. */
	ds->threads = thread_pool_create(2 * VINE_DATA_CONNECTIONS_DEFAULT);
	if (!ds->listener || !ds->notification || !ds->threads || !ds->vault_table) {
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
	LIST_ITERATE(ds->jobs, job)
	{
		if (job->link) {
			shutdown(link_fd(job->link), SHUT_RDWR);
		}
	}
	pthread_mutex_unlock(&ds->lock);
	if (ds->threads) {
		thread_pool_delete(ds->threads);
	}
	while ((job = list_pop_head(ds->jobs))) {
		job_delete(job);
	}
	list_delete(ds->jobs);
	list_delete(ds->ready);
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
		hash_table_clear(ds->vault_table, (void *)vine_file_delete);
		hash_table_delete(ds->vault_table);
	}
	if (ds->vault_directory) {
		unlink_recursive(ds->vault_directory);
		free(ds->vault_directory);
	}
	pthread_cond_destroy(&ds->finished);
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

void vine_manager_data_service_poll(struct vine_manager_data_service *ds, struct link_info *entries)
{
	/* Watch the listener while a slot is free or a receive can be preempted. Otherwise connections wait in the
	 * kernel backlog without waking the Manager repeatedly. */
	int accepting = active_job_count(ds, NULL) < VINE_DATA_CONNECTIONS_DEFAULT;
	struct transfer_job *job;
	LIST_ITERATE(ds->jobs, job)
	{
		if (accepting) {
			break;
		}
		accepting = job_is_active_receive(job);
	}
	entries[0] = (struct link_info){ds->listener, accepting ? LINK_READ : 0, 0};
	entries[1] = (struct link_info){ds->notification, LINK_READ, 0};
}

/* Index a complete vault entry so the Manager thread can query it without touching the filesystem. */
static void vault_insert(struct vine_manager_data_service *ds, struct vine_file *file)
{
	if (!hash_table_lookup(ds->vault_table, file->cached_name)) {
		hash_table_insert(ds->vault_table, file->cached_name, vine_file_addref(file));
	}
}

/* Release a finished receive: return its reservation, publish its file, and report the result.
 * Success means the file was renamed into the vault before any cancellation, so a preempted receive that already
 * finished still publishes. Only a removed receive, whose name vault removal already unlinked, never publishes. */
static void finish_receive(struct vine_manager *manager, struct transfer_job *job)
{
	struct vine_manager_data_service *ds = manager->ds;
	int published = job->success && !job->removed;
	ds->vault_reserved -= job->reserved;
	if (published) {
		vault_insert(ds, job->file);
		ds->vault_used += job->received;
	}
	debug(D_VINE, "checkpoint %s: %s vault_used=%" PRId64 " vault_reserved=%" PRId64, published ? "complete" : "failed", job->name, ds->vault_used, ds->vault_reserved);
	if (job->transfer_id) {
		/* A record already removed because its source Worker disconnected is ignored. */
		vine_current_transfers_remove(manager, job->transfer_id);
	}
	if (job->complete) {
		job->complete(job->argument, job->file, published);
	}
}

void vine_manager_data_service_handle(struct vine_manager *manager)
{
	struct vine_manager_data_service *ds = manager->ds;
	for (unsigned i = 0; i < 2 * VINE_DATA_CONNECTIONS_DEFAULT; i++) {
		pthread_mutex_lock(&ds->lock);
		struct transfer_job *job = list_pop_head(ds->ready);
		pthread_mutex_unlock(&ds->lock);
		if (!job) {
			break;
		}
		char notification;
		read(link_fd(ds->notification), &notification, 1);
		list_remove(ds->jobs, job);
		if (job->file) {
			finish_receive(manager, job);
		}
		job_delete(job);
	}
	/* Worker requests always take priority over checkpoint receives. */
	while (link_usleep(ds->listener, 0, 1, 0)) {
		if (active_job_count(ds, NULL) >= VINE_DATA_CONNECTIONS_DEFAULT && !preempt_receive(ds)) {
			break;
		}
		struct link *link = link_accept(ds->listener, time(NULL));
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

/* Admit a receive of a temporary file into the vault. A required receive is admitted beyond the vault limit. */
static vine_manager_checkpoint_result_t admit_receive(struct vine_manager *manager, struct vine_file *file, int required, void (*complete)(void *, struct vine_file *, int), void *argument)
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
	/* A cancelled receive will never publish, so a new request starts another one. */
	struct transfer_job *job;
	LIST_ITERATE(ds->jobs, job)
	{
		if (job->file == file && job_is_active_receive(job)) {
			return VINE_CHECKPOINT_ADMITTED;
		}
	}
	/* Receives never take the slots reserved for Worker requests. */
	unsigned receives;
	unsigned active = active_job_count(ds, &receives);
	if (active >= VINE_DATA_CONNECTIONS_DEFAULT || receives >= VINE_DATA_CONNECTIONS_DEFAULT - VINE_DATA_WORKER_RESERVED_SLOTS) {
		return VINE_CHECKPOINT_BUSY;
	}
	int64_t size = (int64_t)file->size;
	if (!required && ds->vault_limit > 0 && ds->vault_used + ds->vault_reserved + size > ds->vault_limit) {
		return VINE_CHECKPOINT_BUSY;
	}
	struct vine_worker_info *worker = vine_file_replica_table_find_worker(manager, file->cached_name);
	if (!worker || strlen(file->cached_name) >= VINE_DATA_LINE_MAX / 3) {
		return VINE_CHECKPOINT_NO_SOURCE;
	}
	job = calloc(1, sizeof(*job));
	if (!job) {
		return VINE_CHECKPOINT_BUSY;
	}
	job->ip = strdup(worker->transfer_host);
	job->port = worker->transfer_port;
	job->path = string_format("%s/%s", ds->vault_directory, file->cached_name);
	strcpy(job->name, file->cached_name);
	job->file = vine_file_addref(file);
	job->reserved = size;
	job->complete = complete;
	job->argument = argument;
	if (!job_submit(manager, job)) {
		return VINE_CHECKPOINT_BUSY;
	}
	ds->vault_reserved += size;
	/* Count the receive like a peer transfer so source selection sees the Worker's outgoing load.
	 * It has no destination Worker and never penalizes the source on failure. */
	char *source = string_format("%s/%s", worker->transfer_url, file->cached_name);
	job->transfer_id = vine_current_transfers_add(manager, NULL, worker, source);
	free(source);
	debug(D_VINE, "checkpoint started: %s from %s:%d size=%" PRId64 " required=%d vault_used=%" PRId64 " vault_reserved=%" PRId64 " vault_limit=%" PRId64, file->cached_name, worker->transfer_host, worker->transfer_port, size, required, ds->vault_used, ds->vault_reserved, ds->vault_limit);
	return VINE_CHECKPOINT_ADMITTED;
}

vine_manager_checkpoint_result_t vine_manager_data_service_checkpoint(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument)
{
	return admit_receive(manager, file, 0, complete, argument);
}

vine_manager_checkpoint_result_t vine_manager_data_service_fetch(struct vine_manager *manager, struct vine_file *file, void (*complete)(void *, struct vine_file *, int), void *argument)
{
	return admit_receive(manager, file, 1, complete, argument);
}

/* Declare Manager-local data served through the vault, such as graph edata (serialized callables and arguments).
 * It is a VINE_FILE because its bytes already exist at the Manager and never need to be recomputed. Task outputs,
 * such as graph idata, remain VINE_TEMP even after a checkpoint places a copy in the vault.
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
	vault_insert(manager->ds, file);
	return file;
}

void vine_manager_data_service_vault_remove(struct vine_manager_data_service *ds, struct vine_file *file)
{
	if (!ds || !file || !file->cached_name) {
		return;
	}
	/* A cancelled receive can never publish: the executor checks the flag under the lock before connecting,
	 * before receiving, and before renaming. A queued or connecting receive has no local file yet, so only a
	 * connected receive is joined. Shutting down its socket makes it remove its partial file promptly.
	 * The callback is dropped because the caller may release its context next. */
	int retrieving = 0;
	struct transfer_job *job;
	LIST_ITERATE(ds->jobs, job)
	{
		if (job->file != file) {
			continue;
		}
		retrieving = 1;
		job->complete = NULL;
		job->removed = 1;
		cancel_receive(ds, job);
		pthread_mutex_lock(&ds->lock);
		while (job->link && !job->done) {
			pthread_cond_wait(&ds->finished, &ds->lock);
		}
		pthread_mutex_unlock(&ds->lock);
	}
	/* A receive may have published its file before the Manager thread indexed it, so remove the name either way. */
	struct vine_file *held = hash_table_remove(ds->vault_table, file->cached_name);
	if (held || retrieving) {
		char *path = string_format("%s/%s", ds->vault_directory, file->cached_name);
		struct stat info;
		if (held && held->type == VINE_TEMP && lstat(path, &info) == 0) {
			ds->vault_used -= MIN(ds->vault_used, (int64_t)info.st_size);
		}
		unlink(path);
		free(path);
	}
	if (held) {
		vine_file_delete(held);
	}
}

int vine_manager_data_service_vault_contains(struct vine_manager *manager, struct vine_file *file)
{
	return manager && manager->ds && file && file->cached_name && hash_table_lookup(manager->ds->vault_table, file->cached_name) != NULL;
}

char *vine_manager_data_service_vault_path(struct vine_manager *manager, struct vine_file *file)
{
	if (!vine_manager_data_service_vault_contains(manager, file)) {
		return NULL;
	}
	return string_format("%s/%s", manager->ds->vault_directory, file->cached_name);
}

void vine_manager_data_service_vault_set_limit(struct vine_manager_data_service *ds, int64_t bytes)
{
	if (ds) {
		ds->vault_limit = MAX(0, bytes);
	}
}

int64_t vine_manager_data_service_vault_get_usage(struct vine_manager_data_service *ds)
{
	return ds ? ds->vault_used + ds->vault_reserved : 0;
}

int vine_manager_data_service_has_local_file(struct vine_manager *manager, struct vine_file *file)
{
	if (!file) {
		return 0;
	}
	if (vine_manager_data_service_vault_contains(manager, file)) {
		return 1;
	}
	return file->type == VINE_FILE && access(file->source, R_OK) == 0;
}
