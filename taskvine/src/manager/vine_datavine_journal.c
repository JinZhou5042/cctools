/*
Copyright (C) 2026- The University of Notre Dame
See the file COPYING for details.
*/

#include "vine_datavine_journal.h"

#include "full_io.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define JOURNAL_MAGIC UINT32_C(0x44564a31)
#define JOURNAL_VERSION 1
#define JOURNAL_HEADER_SIZE 24
#define JOURNAL_MAX_PAYLOAD (16U * 1024U * 1024U)
#define JOURNAL_GROUP_COMMIT_NS 1000000L

struct vine_datavine_journal {
	int fd;
	pthread_mutex_t lock;
	pthread_cond_t committed;
	pthread_cond_t changed;
	pthread_t thread;
	uint64_t sequence;
	uint64_t durable_sequence;
	int syncing;
	int stopping;
	int failed;
	struct vine_datavine_journal_metrics metrics;
};

static void *synchronize(void *argument);

static uint16_t get_u16(const unsigned char *buffer)
{
	return (uint16_t)((buffer[0] << 8) | buffer[1]);
}

static uint32_t get_u32(const unsigned char *buffer)
{
	return ((uint32_t)buffer[0] << 24) | ((uint32_t)buffer[1] << 16) | ((uint32_t)buffer[2] << 8) | buffer[3];
}

static uint64_t get_u64(const unsigned char *buffer)
{
	return ((uint64_t)get_u32(buffer) << 32) | get_u32(buffer + 4);
}

static void put_u16(unsigned char *buffer, uint16_t value)
{
	buffer[0] = (unsigned char)(value >> 8);
	buffer[1] = (unsigned char)value;
}

static void put_u32(unsigned char *buffer, uint32_t value)
{
	buffer[0] = (unsigned char)(value >> 24);
	buffer[1] = (unsigned char)(value >> 16);
	buffer[2] = (unsigned char)(value >> 8);
	buffer[3] = (unsigned char)value;
}

static void put_u64(unsigned char *buffer, uint64_t value)
{
	put_u32(buffer, (uint32_t)(value >> 32));
	put_u32(buffer + 4, (uint32_t)value);
}

static uint32_t checksum(uint16_t opcode, const unsigned char *payload,
		size_t payload_size)
{
	uint32_t value = UINT32_C(2166136261);
	value = (value ^ (opcode >> 8)) * UINT32_C(16777619);
	value = (value ^ (opcode & 0xff)) * UINT32_C(16777619);
	for (size_t i = 0; i < payload_size; i++) {
		value = (value ^ payload[i]) * UINT32_C(16777619);
	}
	return value;
}

struct vine_datavine_journal *vine_datavine_journal_open(const char *path)
{
	if (!path || !path[0]) {
		return 0;
	}
	struct vine_datavine_journal *journal = calloc(1, sizeof(*journal));
	if (!journal) {
		return 0;
	}
	journal->fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
	if (journal->fd < 0 || flock(journal->fd, LOCK_EX | LOCK_NB)) {
		if (journal->fd >= 0) {
			close(journal->fd);
		}
		free(journal);
		return 0;
	}
	if (pthread_mutex_init(&journal->lock, 0)) {
		close(journal->fd);
		free(journal);
		return 0;
	}
	if (pthread_cond_init(&journal->committed, 0)) {
		pthread_mutex_destroy(&journal->lock);
		close(journal->fd);
		free(journal);
		return 0;
	}
	if (pthread_cond_init(&journal->changed, 0)) {
		pthread_cond_destroy(&journal->committed);
		pthread_mutex_destroy(&journal->lock);
		close(journal->fd);
		free(journal);
		return 0;
	}
	if (pthread_create(&journal->thread, 0, synchronize, journal)) {
		pthread_cond_destroy(&journal->changed);
		pthread_cond_destroy(&journal->committed);
		pthread_mutex_destroy(&journal->lock);
		close(journal->fd);
		free(journal);
		return 0;
	}
	return journal;
}

int vine_datavine_journal_replay(struct vine_datavine_journal *journal,
		vine_datavine_journal_replay_fn replay, void *context)
{
	if (!journal || !replay) {
		return 0;
	}
	off_t offset = 0;
	for (;;) {
		unsigned char header[JOURNAL_HEADER_SIZE];
		ssize_t count = full_pread(journal->fd, header, sizeof(header), offset);
		if (count == 0) {
			break;
		}
		if (count < 0) {
			return 0;
		}
		if ((size_t)count != sizeof(header)) {
			if (ftruncate(journal->fd, offset)) {
				return 0;
			}
			journal->metrics.truncated_tails++;
			break;
		}
		uint32_t payload_size = get_u32(header + 8);
		uint64_t sequence = get_u64(header + 16);
		if (get_u32(header) != JOURNAL_MAGIC || get_u16(header + 4) != JOURNAL_VERSION || payload_size > JOURNAL_MAX_PAYLOAD || sequence != journal->sequence + 1) {
			return 0;
		}
		unsigned char *payload = payload_size ? malloc(payload_size) : 0;
		if (payload_size && !payload) {
			return 0;
		}
		count = payload_size
					? full_pread(journal->fd, payload, payload_size, offset + JOURNAL_HEADER_SIZE)
					: 0;
		if (count < 0) {
			free(payload);
			return 0;
		}
		if ((uint32_t)count != payload_size) {
			free(payload);
			if (ftruncate(journal->fd, offset)) {
				return 0;
			}
			journal->metrics.truncated_tails++;
			break;
		}
		uint16_t opcode = get_u16(header + 6);
		if (get_u32(header + 12) != checksum(opcode, payload, payload_size) || !replay(context, opcode, payload, payload_size)) {
			free(payload);
			return 0;
		}
		free(payload);
		journal->sequence = sequence;
		journal->durable_sequence = sequence;
		journal->metrics.replayed++;
		offset += JOURNAL_HEADER_SIZE + payload_size;
	}
	return lseek(journal->fd, 0, SEEK_END) >= 0;
}

static void *synchronize(void *argument)
{
	struct vine_datavine_journal *journal = argument;
	pthread_mutex_lock(&journal->lock);
	for (;;) {
		while (!journal->failed && !journal->stopping && journal->sequence == journal->durable_sequence) {
			pthread_cond_wait(&journal->changed, &journal->lock);
		}
		if (journal->failed || (journal->stopping && journal->sequence == journal->durable_sequence)) {
			break;
		}
		journal->syncing = 1;
		pthread_mutex_unlock(&journal->lock);
		struct timespec pause = {.tv_nsec = JOURNAL_GROUP_COMMIT_NS};
		nanosleep(&pause, 0);
		pthread_mutex_lock(&journal->lock);
		uint64_t target = journal->sequence;
		uint64_t group = target - journal->durable_sequence;
		pthread_mutex_unlock(&journal->lock);
		struct timespec started;
		struct timespec stopped;
		clock_gettime(CLOCK_MONOTONIC, &started);
		int failed = fdatasync(journal->fd);
		clock_gettime(CLOCK_MONOTONIC, &stopped);
		uint64_t elapsed = (uint64_t)((stopped.tv_sec - started.tv_sec) * INT64_C(1000000000) + stopped.tv_nsec - started.tv_nsec);
		pthread_mutex_lock(&journal->lock);
		journal->metrics.syncs++;
		journal->metrics.sync_nanoseconds += elapsed;
		if (group > journal->metrics.maximum_group) {
			journal->metrics.maximum_group = group;
		}
		if (failed) {
			journal->failed = 1;
		} else {
			journal->durable_sequence = target;
		}
		journal->syncing = 0;
		pthread_cond_broadcast(&journal->committed);
	}
	journal->syncing = 0;
	pthread_cond_broadcast(&journal->committed);
	pthread_mutex_unlock(&journal->lock);
	return 0;
}

static int append(struct vine_datavine_journal *journal, uint16_t opcode,
		const unsigned char *payload, size_t payload_size, int durable)
{
	if (!journal || payload_size > JOURNAL_MAX_PAYLOAD || (payload_size && !payload)) {
		return 0;
	}
	pthread_mutex_lock(&journal->lock);
	if (journal->failed) {
		pthread_mutex_unlock(&journal->lock);
		return 0;
	}
	uint64_t sequence = ++journal->sequence;
	unsigned char header[JOURNAL_HEADER_SIZE] = {0};
	put_u32(header, JOURNAL_MAGIC);
	put_u16(header + 4, JOURNAL_VERSION);
	put_u16(header + 6, opcode);
	put_u32(header + 8, (uint32_t)payload_size);
	put_u32(header + 12, checksum(opcode, payload, payload_size));
	put_u64(header + 16, sequence);
	if (full_write(journal->fd, header, sizeof(header)) != sizeof(header) || (payload_size && full_write(journal->fd, payload, payload_size) != (ssize_t)payload_size)) {
		journal->failed = 1;
		pthread_cond_broadcast(&journal->committed);
		pthread_mutex_unlock(&journal->lock);
		return 0;
	}
	journal->metrics.commits++;
	journal->metrics.bytes += sizeof(header) + payload_size;
	pthread_cond_signal(&journal->changed);
	while (durable && !journal->failed && journal->durable_sequence < sequence) {
		journal->metrics.waits++;
		pthread_cond_wait(&journal->committed, &journal->lock);
	}
	int result = !journal->failed;
	pthread_mutex_unlock(&journal->lock);
	return result;
}

int vine_datavine_journal_append(struct vine_datavine_journal *journal,
		uint16_t opcode, const unsigned char *payload, size_t payload_size)
{
	return append(journal, opcode, payload, payload_size, 0);
}

int vine_datavine_journal_commit(struct vine_datavine_journal *journal,
		uint16_t opcode, const unsigned char *payload, size_t payload_size)
{
	return append(journal, opcode, payload, payload_size, 1);
}

void vine_datavine_journal_get_metrics(
		struct vine_datavine_journal *journal,
		struct vine_datavine_journal_metrics *result)
{
	if (!result) {
		return;
	}
	memset(result, 0, sizeof(*result));
	if (!journal) {
		return;
	}
	pthread_mutex_lock(&journal->lock);
	*result = journal->metrics;
	result->durable_sequence = journal->durable_sequence;
	pthread_mutex_unlock(&journal->lock);
}

void vine_datavine_journal_close(struct vine_datavine_journal *journal)
{
	if (!journal) {
		return;
	}
	pthread_mutex_lock(&journal->lock);
	journal->stopping = 1;
	pthread_cond_signal(&journal->changed);
	pthread_mutex_unlock(&journal->lock);
	pthread_join(journal->thread, 0);
	close(journal->fd);
	pthread_cond_destroy(&journal->changed);
	pthread_cond_destroy(&journal->committed);
	pthread_mutex_destroy(&journal->lock);
	free(journal);
}
