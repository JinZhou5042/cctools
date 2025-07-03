#ifndef VINE_TEMP_REDUNDANCY_H
#define VINE_TEMP_REDUNDANCY_H

#include "vine_manager.h"

int vine_temp_redundancy_enqueue_file_for_replication(struct vine_manager *q, char *cachename);
int vine_temp_redundancy_consider_replication(struct vine_manager *q);

#endif