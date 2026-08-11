#include "vine_task_frame.h"

#include "vine_file.h"
#include "vine_manager.h"
#include "vine_mount.h"
#include "vine_task.h"

#include "list.h"
#include "rmsummary.h"
#include "url_encode.h"

#include <inttypes.h>
#include <limits.h>
#include <string.h>

#define VINE_TASK_FRAME_MAX (16 * 1024 * 1024)

int vine_task_frame_build(buffer_t *frame, struct vine_manager *manager,
		struct vine_task *task, const char *command_line,
		struct rmsummary *limits, struct vine_file *target)
{
	buffer_init(frame);
	buffer_max(frame, VINE_TASK_FRAME_MAX);

#define FRAME_PRINTF(...) \
	do { \
		if (buffer_putfstring(frame, __VA_ARGS__) < 0) \
			goto failure; \
	} while (0)
#define FRAME_PUT(data, length) \
	do { \
		if (buffer_putlstring(frame, (data), (length)) < 0) \
			goto failure; \
	} while (0)

	if (target) {
		int mode = target->mode ? target->mode : 0755;
		FRAME_PRINTF("mini_task %s %s %d %lld 0%o\n", target->source, target->cached_name, target->cache_level, (long long)target->size, mode);
	} else {
		FRAME_PRINTF("task %lld\n", (long long)task->task_id);
	}

	size_t command_length = strlen(command_line);
	FRAME_PRINTF("cmd %zu\n", command_length);
	FRAME_PUT(command_line, command_length);

	if (task->needs_library) {
		FRAME_PRINTF("needs_library %s\n", task->needs_library);
		if (task->library_task) {
			FRAME_PRINTF("library_task_id %d\n", task->library_task_id);
			FRAME_PRINTF("function_credit_generation %" PRId64 "\n",
					task->function_credit_generation);
		}
		if (task->function_input_length) {
			FRAME_PRINTF("function_input %zu\n", task->function_input_length);
			FRAME_PUT(task->function_input, task->function_input_length);
		}
	}

	if (task->provides_library) {
		FRAME_PRINTF("provides_library %s\n", task->provides_library);
		FRAME_PRINTF("function_slots %d\n", task->function_slots_total);
		FRAME_PRINTF("func_exec_mode %d\n", task->func_exec_mode);
	}

	FRAME_PRINTF("category %s\n", task->category);
	if (limits) {
		FRAME_PRINTF("cores %s\n", rmsummary_resource_to_str("cores", limits->cores, 0));
		FRAME_PRINTF("gpus %s\n", rmsummary_resource_to_str("gpus", limits->gpus, 0));
		FRAME_PRINTF("memory %s\n", rmsummary_resource_to_str("memory", limits->memory, 0));
		FRAME_PRINTF("disk %s\n", rmsummary_resource_to_str("disk", limits->disk, 0));
		if (manager->monitor_mode != VINE_MON_WATCHDOG) {
			if (limits->end > 0)
				FRAME_PRINTF("end_time %s\n", rmsummary_resource_to_str("end", limits->end, 0));
			if (limits->wall_time > 0)
				FRAME_PRINTF("wall_time %s\n", rmsummary_resource_to_str("wall_time", limits->wall_time, 0));
		}
	}

	char *variable;
	LIST_ITERATE(task->env_list, variable)
	{
		FRAME_PRINTF("env %zu\n", strlen(variable));
		FRAME_PUT(variable, strlen(variable));
		FRAME_PUT("\n", 1);
	}

	struct vine_mount *mount;
	LIST_ITERATE(task->input_mounts, mount)
	{
		char remote_name[PATH_MAX];
		url_encode(mount->remote_name, remote_name, sizeof(remote_name));
		FRAME_PRINTF("infile %s %s %d\n", mount->file->cached_name, remote_name, mount->flags);
	}
	LIST_ITERATE(task->output_mounts, mount)
	{
		char remote_name[PATH_MAX];
		url_encode(mount->remote_name, remote_name, sizeof(remote_name));
		FRAME_PRINTF("outfile %s %s %d\n", mount->file->cached_name, remote_name, mount->flags);
	}
	if (task->group_id)
		FRAME_PRINTF("groupid %d\n", task->group_id);
	FRAME_PUT("end\n", 4);

#undef FRAME_PRINTF
#undef FRAME_PUT
	return 1;

failure:
#undef FRAME_PRINTF
#undef FRAME_PUT
	buffer_free(frame);
	return 0;
}
