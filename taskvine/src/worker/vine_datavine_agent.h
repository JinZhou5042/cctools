/* Worker-local DataVine data plane. */
#ifndef VINE_DATAVINE_AGENT_H
#define VINE_DATAVINE_AGENT_H

#include <stdint.h>

struct vine_cache;
struct vine_process;

enum vine_datavine_agent_prepare_status {
	VINE_DATAVINE_AGENT_NOT_TASK = 0,
	VINE_DATAVINE_AGENT_WAIT = 1,
	VINE_DATAVINE_AGENT_READY = 2,
	VINE_DATAVINE_AGENT_FAILED = 3,
};

int vine_datavine_agent_initialize(struct vine_cache *cache,
		const char *transfer_host, uint16_t transfer_port);
void vine_datavine_agent_shutdown(void);
enum vine_datavine_agent_prepare_status vine_datavine_agent_prepare(
		struct vine_process *process);
int vine_datavine_agent_commit(struct vine_process *process);
void vine_datavine_agent_progress(void);

#endif
