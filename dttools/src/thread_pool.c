#include "thread_pool.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

#define THREAD_POOL_IDLE_SECONDS 60

/* This stores one queued function call until an executor takes it. */
struct thread_pool_task {
	thread_pool_run_fn run;	       /* This is the function the executor will call. */
	void *argument;		       /* This borrows the argument passed to that function. */
	struct thread_pool_task *next; /* This links to the next queued function call. */
};

/* This owns a work queue and creates executor threads as demand grows. */
struct thread_pool {
	pthread_mutex_t lock;	       /* This protects the task queue and executor counts. */
	pthread_cond_t ready;	       /* This wakes executors for work, limit changes, or shutdown. */
	struct thread_pool_task *head; /* This points to the next queued task to execute. */
	struct thread_pool_task *tail; /* This points to the last queued task. */
	size_t count;		       /* This counts executor threads that have started or are starting. */
	size_t active;		       /* This counts executors currently running a task. */
	size_t queued;		       /* This counts tasks waiting for an executor. */
	size_t limit;		       /* This limits executor threads and is zero when no limit is configured. */
	int stopping;		       /* This stops new submissions while accepted work drains. */
};

static void *run_thread(void *argument)
{
	struct thread_pool *pool = argument;
	pthread_mutex_lock(&pool->lock);
	for (;;) {
		struct timespec deadline;
		clock_gettime(CLOCK_REALTIME, &deadline);
		deadline.tv_sec += THREAD_POOL_IDLE_SECONDS;
		int expired = 0;
		while (!pool->head && !pool->stopping &&
				(!pool->limit || pool->count <= pool->limit) && !expired) {
			expired = pthread_cond_timedwait(&pool->ready, &pool->lock, &deadline) == ETIMEDOUT;
		}
		if ((!pool->head && (pool->stopping || expired)) ||
				(pool->limit && pool->count > pool->limit)) {
			break;
		}
		struct thread_pool_task *task = pool->head;
		pool->head = task->next;
		if (!pool->head) {
			pool->tail = NULL;
		}
		pool->queued--;
		pool->active++;
		pthread_mutex_unlock(&pool->lock);
		task->run(task->argument);
		free(task);
		pthread_mutex_lock(&pool->lock);
		pool->active--;
	}
	pool->count--;
	pthread_cond_broadcast(&pool->ready);
	pthread_mutex_unlock(&pool->lock);
	return NULL;
}

/* Starting threads count as available, avoiding duplicate burst executors. */
static int grow_locked(struct thread_pool *pool)
{
	while (pool->queued > pool->count - pool->active &&
			(!pool->limit || pool->count < pool->limit)) {
		pthread_attr_t attributes;
		int error = pthread_attr_init(&attributes);
		if (!error) {
			error = pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_DETACHED);
			pthread_t id;
			if (!error) {
				error = pthread_create(&id, &attributes, run_thread, pool);
			}
			pthread_attr_destroy(&attributes);
		}
		if (error) {
			errno = error;
			return 0;
		}
		pool->count++;
	}
	return 1;
}

struct thread_pool *thread_pool_create(size_t limit)
{
	struct thread_pool *pool = calloc(1, sizeof(*pool));
	if (!pool) {
		return NULL;
	}
	int error = pthread_mutex_init(&pool->lock, NULL);
	if (!error) {
		error = pthread_cond_init(&pool->ready, NULL);
		if (error) {
			pthread_mutex_destroy(&pool->lock);
		}
	}
	if (error) {
		free(pool);
		errno = error;
		return NULL;
	}
	pool->limit = limit;
	return pool;
}

int thread_pool_submit(struct thread_pool *pool, thread_pool_run_fn run, void *argument)
{
	if (!pool || !run) {
		errno = EINVAL;
		return 0;
	}
	struct thread_pool_task *entry = malloc(sizeof(*entry));
	if (!entry) {
		return 0;
	}
	entry->run = run;
	entry->argument = argument;
	entry->next = NULL;
	pthread_mutex_lock(&pool->lock);
	if (pool->stopping) {
		pthread_mutex_unlock(&pool->lock);
		free(entry);
		errno = ECANCELED;
		return 0;
	}
	if (pool->tail) {
		pool->tail->next = entry;
	} else {
		pool->head = entry;
	}
	pool->tail = entry;
	pool->queued++;
	int grown = grow_locked(pool);
	/* Existing executors can drain the queue if growth hits an OS resource
	 * limit. With none, reject explicitly instead of stranding the request. */
	if (!grown && !pool->count) {
		pool->queued--;
		pool->head = pool->tail = NULL;
		pthread_mutex_unlock(&pool->lock);
		free(entry);
		return 0;
	}
	pthread_cond_signal(&pool->ready);
	pthread_mutex_unlock(&pool->lock);
	return 1;
}

void thread_pool_set_limit(struct thread_pool *pool, size_t limit)
{
	if (!pool) {
		return;
	}
	pthread_mutex_lock(&pool->lock);
	if (pool->limit != limit) {
		pool->limit = limit;
		if (!pool->stopping) {
			grow_locked(pool);
		}
		pthread_cond_broadcast(&pool->ready);
	}
	pthread_mutex_unlock(&pool->lock);
}

void thread_pool_delete(struct thread_pool *pool)
{
	if (!pool) {
		return;
	}
	pthread_mutex_lock(&pool->lock);
	pool->stopping = 1;
	pthread_cond_broadcast(&pool->ready);
	while (pool->count) {
		pthread_cond_wait(&pool->ready, &pool->lock);
	}
	pthread_mutex_unlock(&pool->lock);
	pthread_cond_destroy(&pool->ready);
	pthread_mutex_destroy(&pool->lock);
	free(pool);
}
