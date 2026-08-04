#ifndef VINE_DATAVINE_JOURNAL_H
#define VINE_DATAVINE_JOURNAL_H

#include <stddef.h>
#include <stdint.h>

struct vine_datavine_journal;

struct vine_datavine_journal_metrics {
	uint64_t commits;
	uint64_t bytes;
	uint64_t syncs;
	uint64_t sync_nanoseconds;
	uint64_t maximum_group;
	uint64_t waits;
	uint64_t replayed;
	uint64_t truncated_tails;
	uint64_t durable_sequence;
};

typedef int (*vine_datavine_journal_replay_fn)(
		void *context, uint16_t opcode, const unsigned char *payload,
		size_t payload_size);

struct vine_datavine_journal *vine_datavine_journal_open(const char *path);
int vine_datavine_journal_replay(struct vine_datavine_journal *journal,
		vine_datavine_journal_replay_fn replay, void *context);
int vine_datavine_journal_append(struct vine_datavine_journal *journal,
		uint16_t opcode, const unsigned char *payload, size_t payload_size);
int vine_datavine_journal_commit(struct vine_datavine_journal *journal,
		uint16_t opcode, const unsigned char *payload, size_t payload_size);
void vine_datavine_journal_get_metrics(
		struct vine_datavine_journal *journal,
		struct vine_datavine_journal_metrics *result);
void vine_datavine_journal_close(struct vine_datavine_journal *journal);

#endif
