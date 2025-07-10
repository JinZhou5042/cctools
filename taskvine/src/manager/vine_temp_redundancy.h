#ifndef VINE_TEMP_REDUNDANCY_H
#define VINE_TEMP_REDUNDANCY_H

#include "vine_manager.h"

int is_checkpoint_worker(struct vine_manager *q, struct vine_worker_info *w);
int enqueue_file_for_replication(struct vine_manager *q, struct vine_file *f);
int enqueue_file_for_checkpointing(struct vine_manager *q, struct vine_file *f);
int vine_temp_redundancy_handle_file_lost(struct vine_manager *q, char *cachename);
int vine_temp_redundancy_process(struct vine_manager *q);
int vine_temp_redundancy_replicate_file(struct vine_manager *q, struct vine_file *f);
int vine_temp_redundancy_checkpoint_file(struct vine_manager *q, struct vine_file *f);
int file_has_been_checkpointed(struct vine_manager *q, struct vine_file *f);

#endif