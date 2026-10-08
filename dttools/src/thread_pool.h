/* This interface queues background work and manages the threads that execute it. */
#ifndef CCTOOLS_THREAD_POOL_H
#define CCTOOLS_THREAD_POOL_H

#include <stddef.h>

struct thread_pool;
typedef void (*thread_pool_run_fn)(void *argument);

/* Zero means no configured executor limit. Idle executors retire after 60s. */
struct thread_pool *thread_pool_create(size_t limit);
/* Queue a request. Return zero when stopping or when resources are unavailable.
 * Tasks remain caller-owned if submission fails. */
int thread_pool_submit(struct thread_pool *pool, thread_pool_run_fn run, void *argument);
/* A lower limit retires excess executors after their active tasks finish. */
void thread_pool_set_limit(struct thread_pool *pool, size_t limit);
/* Stop admission, drain accepted tasks, wait for all executors, then free. */
void thread_pool_delete(struct thread_pool *pool);

#endif
