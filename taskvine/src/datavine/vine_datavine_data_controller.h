/* Concurrent DataVine data-plane owner. */
#ifndef VINE_DATAVINE_DATA_CONTROLLER_H
#define VINE_DATAVINE_DATA_CONTROLLER_H

#include <stddef.h>
#include <stdint.h>

struct itable;
struct jx;
struct vine_datavine_data_controller;
struct vine_datavine_data_publication;
struct vine_datavine_workflow_result_info;
struct vine_file;
struct vine_manager;
struct vine_task;

struct vine_datavine_data_controller *vine_datavine_data_controller_open(
		const char *workflow_journal_path, size_t threads);
void vine_datavine_data_controller_close(
		struct vine_datavine_data_controller *controller);

/* Bind retained outputs directly to controller-owned immutable files. */
int vine_datavine_data_controller_bind_outputs(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id,
		struct vine_task *physical, struct jx *task, struct itable *files,
		struct itable *consumers,
		struct itable *requested, uint32_t attempt, int retain_all);

/* Queue validation and durable metadata publication. Payload bytes never
 * enter the workflow runtime or workflow journal. */
struct vine_datavine_data_publication *
vine_datavine_data_controller_publish_async(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, struct jx *task, struct itable *data,
		struct itable *files, struct itable *consumers, struct itable *requested,
		uint32_t attempt, int retain_all, struct vine_task *completed);
int vine_datavine_data_publication_ready(
		struct vine_datavine_data_publication *publication);
int vine_datavine_data_publication_wait(
		struct vine_datavine_data_publication *publication);
void vine_datavine_data_publication_delete(
		struct vine_datavine_data_publication *publication);

int vine_datavine_data_controller_fetch_result(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		char **data, size_t *size);
int vine_datavine_data_controller_result_info(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id,
		struct vine_datavine_workflow_result_info *result);
int vine_datavine_data_controller_result_active(
		struct vine_datavine_data_controller *controller,
		const char *workflow_id, uint64_t data_id);
struct vine_file *vine_datavine_data_controller_restore_file(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id,
		uint64_t data_id);
int vine_datavine_data_controller_finish_workflow(
		struct vine_datavine_data_controller *controller,
		struct vine_manager *manager, const char *workflow_id);

#endif
