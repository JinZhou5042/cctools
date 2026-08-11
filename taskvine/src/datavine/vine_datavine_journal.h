/* DataVine journal API. */
#ifndef VINE_DATAVINE_JOURNAL_H
#define VINE_DATAVINE_JOURNAL_H

#include <stddef.h>
#include <stdint.h>

struct vine_datavine_journal;

typedef int (*vine_datavine_journal_replay_fn)(
		void *context, uint16_t opcode, const unsigned char *payload,
		size_t payload_size);

struct vine_datavine_journal *vine_datavine_journal_open(const char *path);
int vine_datavine_journal_replay(struct vine_datavine_journal *journal,
		vine_datavine_journal_replay_fn replay, void *context);
/** Queue a record without waiting for the writer. A later commit is the
 * durability/error barrier for this record and all earlier records. */
int vine_datavine_journal_enqueue(struct vine_datavine_journal *journal,
		uint16_t opcode, const unsigned char *payload, size_t payload_size);
int vine_datavine_journal_commit(struct vine_datavine_journal *journal,
		uint16_t opcode, const unsigned char *payload, size_t payload_size);
void vine_datavine_journal_close(struct vine_datavine_journal *journal);

#endif
