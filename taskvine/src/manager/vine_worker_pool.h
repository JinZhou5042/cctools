/* Optional dense Worker availability index. */

#ifndef VINE_WORKER_POOL_H
#define VINE_WORKER_POOL_H

#include <stddef.h>

struct vine_worker_pool;
struct vine_worker_info;

struct vine_worker_pool *vine_worker_pool_create(void);
void vine_worker_pool_delete(struct vine_worker_pool *pool);

int vine_worker_pool_add(struct vine_worker_pool *pool,
		struct vine_worker_info *worker);
void vine_worker_pool_remove(struct vine_worker_pool *pool,
		struct vine_worker_info *worker);
int vine_worker_pool_offer(struct vine_worker_pool *pool,
		struct vine_worker_info *worker);
struct vine_worker_info *vine_worker_pool_take(struct vine_worker_pool *pool);
size_t vine_worker_pool_ready(const struct vine_worker_pool *pool);

#endif
