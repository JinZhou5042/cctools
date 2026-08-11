#ifndef VINE_FUNCTION_CALL_H
#define VINE_FUNCTION_CALL_H

struct vine_task;
struct vine_manager;
struct vine_worker_info;

/* Generic inline function-call state kept out of the task lifecycle core. */
void vine_function_call_task_init(struct vine_task *task);
void vine_function_call_task_reset(struct vine_task *task);
void vine_function_call_task_copy(struct vine_task *target,
		const struct vine_task *source);
void vine_function_call_task_delete(struct vine_task *task);

int vine_function_call_handle_info(struct vine_manager *manager,
		struct vine_worker_info *worker, const char *field, const char *value,
		struct vine_task **requeue);
void vine_function_call_release_credit(struct vine_worker_info *worker,
		struct vine_task *task);
int vine_function_call_expire_grants(struct vine_manager *manager);
void vine_function_call_prepare_grant(struct vine_manager *manager,
		struct vine_task *task);
void vine_function_call_commit_grant(struct vine_manager *manager,
		struct vine_task *task);

#endif
