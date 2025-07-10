#ifndef PROGRESS_BAR_H
#define PROGRESS_BAR_H

#include "list.h"
#include "timestamp.h"
#include <time.h>
#include <stdint.h>

struct ProgressBarPart {
	const char *label;
	uint64_t total;
	uint64_t current;
};

struct ProgressBar {
	const char *label;
	struct list *parts;
	timestamp_t start_time;
    timestamp_t last_draw_time;
	int has_drawn_once;
};

struct ProgressBarPart *progress_bar_part_create(const char *label, uint64_t total);
struct ProgressBar *progress_bar_init(const char *label);
void progress_bar_add_part(struct ProgressBar *bar, struct ProgressBarPart *part);
void progress_bar_advance_part_current(struct ProgressBar *bar, struct ProgressBarPart *part, uint64_t increment);
void progress_bar_update_part_total(struct ProgressBar *bar, struct ProgressBarPart *part, uint64_t new_total);
void progress_bar_reset_start_time(struct ProgressBar *bar, timestamp_t start_time);
void progress_bar_finish(struct ProgressBar *bar);
void progress_bar_delete(struct ProgressBar *bar);

#endif
