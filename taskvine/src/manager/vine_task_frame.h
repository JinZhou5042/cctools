#ifndef VINE_TASK_FRAME_H
#define VINE_TASK_FRAME_H

#include "buffer.h"

struct rmsummary;
struct vine_file;
struct vine_manager;
struct vine_task;

int vine_task_frame_build(buffer_t *frame, struct vine_manager *manager,
		struct vine_task *task, const char *command_line,
		struct rmsummary *limits, struct vine_file *target);

#endif
