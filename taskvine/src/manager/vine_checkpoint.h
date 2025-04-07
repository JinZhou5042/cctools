#ifndef VINE_CHECKPOINT_H
#define VINE_CHECKPOINT_H

#include "vine_file.h"
#include "vine_manager.h"
#include "priority_queue.h"
#include "vine_worker_info.h"


int vine_checkpoint_persist(struct vine_manager *q, struct vine_worker_info *source, struct vine_file *f);

int vine_checkpoint_evict(struct vine_manager *q, struct vine_file *f);

int vine_checkpoint_checkpointed(struct vine_manager *q, struct vine_file *f);

int vine_checkpoint_pbb_available(struct vine_manager *q, struct vine_file *f);

struct vine_worker_info *vine_checkpoint_choose_source(struct vine_manager *q, struct vine_file *f);

int vine_checkpoint_ensure_pbb_space(struct vine_manager *q, struct vine_file *f);

void vine_checkpoint_update_file_penalty(struct vine_manager *q, struct vine_file *f);

void vine_checkpoint_check_consistency(struct vine_manager *q);

#endif /* VINE_CHECKPOINT_H */
