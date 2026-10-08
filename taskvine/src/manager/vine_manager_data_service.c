/* Transfer declared Manager data to Workers and fetch Worker files through the native get protocol. */
#include "vine_manager_data_service.h"
#include "vine_manager.h"
#include "vine_file.h"
#include "hash_table.h"
#include "link.h"
#include "link_auth.h"
#include "list.h"
#include "stringtools.h"
#include "thread_pool.h"
#include "url_encode.h"
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

/* Each job moves from a request to a transfer and then back to the Manager for cleanup. */
enum transfer_state {
	TRANSFER_REQUEST, /* Read a cache name and serve its export, or ask the Manager to look it up. */
	TRANSFER_SEND,	  /* Send the declared local file or buffer to the Worker. */
	TRANSFER_RECEIVE, /* Fetch a Worker file into a temporary local path. */
	TRANSFER_DONE,	  /* Deliver the result and release the job on the Manager thread. */
};

/* Own the data listener and bounded jobs without maintaining another file table. */
struct vine_manager_data_service {
	char *export_directory;       /* Own the fixed directory of published cache names. */
	struct link *listener;	     /* Accept incoming Worker data connections. */
	struct link *notification;   /* Wake the Manager when an executor finishes a step. */
	int notify_fd;		     /* Write completion notifications to this pipe. */
	struct thread_pool *threads; /* Execute both transfer directions. */
	struct list *jobs;	     /* Own all admitted jobs until the Manager releases them. */
	struct list *ready;	     /* Queue jobs returned by executors. */
	pthread_mutex_t lock;	     /* Protect the ready queue and active links during shutdown. */
	int stopping;		     /* Prevent new connections while executors are being stopped. */
};

/* Hold one transfer until its completion is consumed by the Manager. */
struct transfer_job {
	struct vine_manager_data_service *owner; /* Borrow the service that owns this job. */
	enum transfer_state state;		 /* Select the next executor operation. */
	struct link *link;			 /* Own the active peer connection. */
	struct vine_file *file;			 /* Hold a reference to declared data until sending finishes. */
	char *password;				 /* Copy the Manager password for peer authentication. */
	char name[VINE_DATA_LINE_MAX];		 /* Identify the requested cache object. */
	char *ip;				 /* Own the Worker address for an outgoing request. */
	int port;				 /* Identify the Worker data port. */
	char *path;				 /* Own the destination path for an incoming file. */
	int success;				 /* Report whether the transfer completed. */
	void (*complete)(void *, int);		 /* Notify the caller from the Manager thread. */
	void *argument;				 /* Borrow the caller's completion context. */
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
	free(job);
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
	valid = valid && rename(temporary, job->path) == 0;
	unlink(temporary);
	free(temporary);
	return valid;
}

static int send_file(struct transfer_job *job, int fd)
{
	struct vine_file *file = job->file;
	struct stat info;
	char encoded[VINE_DATA_LINE_MAX];
	url_encode(job->name, encoded, sizeof(encoded));
	time_t stoptime = time(NULL) + 900;
	if (file && file->type == VINE_BUFFER) {
		return link_printf(job->link, stoptime, "file %s %zu %o 0\n", encoded, file->size, file->mode ? file->mode : 0600) >= 0 &&
		       link_write(job->link, file->data, file->size, stoptime) == (int64_t)file->size;
	}
	if (fd < 0 && file && file->type == VINE_FILE) {
		fd = open(file->source, O_RDONLY | O_CLOEXEC);
	}
	int valid = fd >= 0 && fstat(fd, &info) == 0 && S_ISREG(info.st_mode);
	if (valid) {
		valid = link_printf(job->link, stoptime, "file %s %" PRId64 " %o %ld\n", encoded, (int64_t)info.st_size, file && file->mode ? file->mode : (info.st_mode & 0777), (long)info.st_mtime) >= 0 &&
			link_stream_from_fd(job->link, fd, info.st_size, stoptime) == info.st_size;
	} else {
		link_printf(job->link, stoptime, "error %s %d\n", encoded, ENOENT);
	}
	if (fd >= 0) {
		close(fd);
	}
	return valid;
}

static void transfer_run(void *argument)
{
	struct transfer_job *job = argument;
	struct vine_manager_data_service *ds = job->owner;
	pthread_mutex_lock(&ds->lock);
	int stopping = ds->stopping;
	pthread_mutex_unlock(&ds->lock);
	if (stopping) {
		job->state = TRANSFER_DONE;
	} else if (job->state == TRANSFER_RECEIVE) {
		struct link *link = link_connect(job->ip, job->port, time(NULL) + 300);
		pthread_mutex_lock(&ds->lock);
		job->link = link;
		stopping = ds->stopping;
		pthread_mutex_unlock(&ds->lock);
		job->success = link && !stopping && (!job->password || link_auth_password(link, job->password, time(NULL) + 5)) && receive_file(job);
		job->state = TRANSFER_DONE;
	} else if (job->state == TRANSFER_REQUEST) {
		char line[VINE_DATA_LINE_MAX], encoded[VINE_DATA_LINE_MAX];
		int used = 0;
		int valid = (!job->password || link_auth_password(job->link, job->password, time(NULL) + 5)) &&
			    link_readline(job->link, line, sizeof(line), time(NULL) + VINE_DATA_IDLE_SECONDS) &&
			    sscanf(line, "get %4095s%n", encoded, &used) == 1 && !line[used];
		if (valid) {
			url_decode(encoded, job->name, sizeof(job->name));
		}
		job->state = valid ? TRANSFER_SEND : TRANSFER_DONE;
		/* Exported objects need no Manager table lookup or borrowed vine_file. */
		if (valid && job->name[0] && !strchr(job->name, '/') && strcmp(job->name, ".") && strcmp(job->name, "..")) {
			char *path = string_format("%s/%s", ds->export_directory, job->name);
			int fd = open(path, O_RDONLY | O_CLOEXEC);
			free(path);
			if (fd >= 0) {
				job->success = send_file(job, fd);
				job->state = TRANSFER_DONE;
			}
		}
	} else {
		job->success = send_file(job, -1);
		job->state = TRANSFER_DONE;
	}
	pthread_mutex_lock(&ds->lock);
	if (job->state == TRANSFER_DONE && job->link) {
		link_close(job->link);
		job->link = NULL;
	}
	list_push_tail(ds->ready, job);
	/* At most one notification per admitted job is outstanding. */
	ssize_t result;
	do {
		result = write(ds->notify_fd, "x", 1);
	} while (result < 0 && errno == EINTR);
	pthread_mutex_unlock(&ds->lock);
}

struct vine_manager_data_service *vine_manager_data_service_create(const char *runtime_directory)
{
	struct vine_manager_data_service *ds = calloc(1, sizeof(*ds));
	if (!ds) {
		return NULL;
	}
	ds->notify_fd = -1;
	pthread_mutex_init(&ds->lock, NULL);
	ds->jobs = list_create();
	ds->ready = list_create();
	ds->export_directory = string_format("%s/staging/exports-XXXXXX", runtime_directory);
	if (!mkdtemp(ds->export_directory)) {
		free(ds->export_directory);
		ds->export_directory = NULL;
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
	ds->threads = thread_pool_create(VINE_DATA_CONNECTIONS_DEFAULT);
	if (!ds->listener || !ds->notification || !ds->threads) {
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
	thread_pool_delete(ds->threads);
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
	if (ds->export_directory) {
		unlink_recursive(ds->export_directory);
		free(ds->export_directory);
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

void vine_manager_data_service_poll(struct vine_manager_data_service *ds, struct link_info *entries)
{
	entries[0] = (struct link_info){ds->listener, (unsigned)list_size(ds->jobs) < VINE_DATA_CONNECTIONS_DEFAULT ? LINK_READ : 0, 0};
	entries[1] = (struct link_info){ds->notification, LINK_READ, 0};
}

void vine_manager_data_service_handle(struct vine_manager *manager)
{
	struct vine_manager_data_service *ds = manager->ds;
	for (unsigned i = 0; i < VINE_DATA_CONNECTIONS_DEFAULT; i++) {
		pthread_mutex_lock(&ds->lock);
		struct transfer_job *job = list_pop_head(ds->ready);
		pthread_mutex_unlock(&ds->lock);
		if (!job) {
			break;
		}
		char notification;
		read(link_fd(ds->notification), &notification, 1);
		if (job->state == TRANSFER_SEND) {
			struct vine_file *file = hash_table_lookup(manager->file_table, job->name);
			job->file = file ? vine_file_addref(file) : NULL;
			if (thread_pool_submit(ds->threads, transfer_run, job)) {
				continue;
			}
		}
		list_remove(ds->jobs, job);
		if (job->complete) {
			job->complete(job->argument, job->success);
		}
		job_delete(job);
	}
	while ((unsigned)list_size(ds->jobs) < VINE_DATA_CONNECTIONS_DEFAULT && link_usleep(ds->listener, 0, 1, 0)) {
		struct link *link = link_accept(ds->listener, time(NULL));
		if (!link) {
			break;
		}
		struct transfer_job *job = calloc(1, sizeof(*job));
		if (!job) {
			link_close(link);
			break;
		}
		job->owner = ds;
		job->link = link;
		job->password = manager->password ? strdup(manager->password) : NULL;
		job->state = TRANSFER_REQUEST;
		list_push_tail(ds->jobs, job);
		if (!thread_pool_submit(ds->threads, transfer_run, job)) {
			list_remove(ds->jobs, job);
			job_delete(job);
		}
	}
}

int vine_manager_data_service_get(struct vine_manager *manager, const char *ip, int port, const char *name, const char *path, void (*complete)(void *, int), void *argument)
{
	struct vine_manager_data_service *ds = manager->ds;
	if (!ip || !name || !path || port < 1 || port > 65535 || strlen(name) >= VINE_DATA_LINE_MAX / 3 ||
			(unsigned)list_size(ds->jobs) >= VINE_DATA_CONNECTIONS_DEFAULT) {
		return 0;
	}
	struct transfer_job *job = calloc(1, sizeof(*job));
	if (!job) {
		return 0;
	}
	job->owner = ds;
	job->state = TRANSFER_RECEIVE;
	job->ip = strdup(ip);
	job->port = port;
	job->path = strdup(path);
	strcpy(job->name, name);
	job->password = manager->password ? strdup(manager->password) : NULL;
	job->complete = complete;
	job->argument = argument;
	list_push_tail(ds->jobs, job);
	if (!thread_pool_submit(ds->threads, transfer_run, job)) {
		list_remove(ds->jobs, job);
		job_delete(job);
		return 0;
	}
	return 1;
}

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

	/* Use native URL identity and dispatch, with the local export as its backing store. */
	struct vine_file *file = vine_file_url("manager://", VINE_CACHE_LEVEL_WORKFLOW, 0);
	if (!file) {
		free(source);
		return NULL;
	}
	free(file->source);
	file->source = string_format("manager://%s", file->cached_name);
	file->size = info.st_size;
	file->mtime = info.st_mtime;
	file->mode = info.st_mode & 0777;
	char *path = string_format("%s/%s", manager->ds->export_directory, file->cached_name);
	int result = symlink(source, path);
	free(path);
	free(source);
	if (result != 0) {
		vine_file_delete(file);
		return NULL;
	}
	return vine_manager_declare_file(manager, file);
}

void vine_manager_data_service_unexport(struct vine_manager_data_service *ds, struct vine_file *file)
{
	if (ds && file->type == VINE_URL && string_prefix_is(file->source, "manager://")) {
		char *path = string_format("%s/%s", ds->export_directory, file->cached_name);
		unlink(path);
		free(path);
	}
}
