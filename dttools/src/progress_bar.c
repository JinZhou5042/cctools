#include "progress_bar.h"
#include "xxmalloc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <assert.h>
#include <stdint.h>

#define MAX_BAR_WIDTH 30
#define ONE_SECOND 1000000
#define PROGRESS_BAR_UPDATE_INTERVAL ONE_SECOND * 0.1

#define COLOR_RESET "\033[0m"
#define COLOR_GREEN "\033[32m"
#define COLOR_CYAN "\033[38;2;0;255;255m"
#define COLOR_ORANGE "\033[38;2;255;165;0m"
#define COLOR_PURPLE "\033[38;2;128;0;128m"
#define COLOR_PINK "\033[38;2;255;192;203m"
#define COLOR_YELLOW "\033[38;2;255;255;0m"

static void print_progress_bar(struct ProgressBar *bar);

struct ProgressBarPart *progress_bar_part_create(const char *label, uint64_t total)
{
	assert(label != NULL);
	struct ProgressBarPart *part = xxmalloc(sizeof(struct ProgressBarPart));
	part->label = xxstrdup(label);
	part->total = total;
	part->current = 0;
	return part;
}

struct ProgressBar *progress_bar_init(const char *label)
{
	struct ProgressBar *bar = xxmalloc(sizeof(struct ProgressBar));
	bar->label = xxstrdup(label);
	bar->parts = list_create();
	bar->start_time = timestamp_get();
	bar->last_draw_time = timestamp_get();
	bar->has_drawn_once = 0;
	return bar;
}

void progress_bar_add_part(struct ProgressBar *bar, struct ProgressBarPart *part)
{
	list_push_tail(bar->parts, part);
	print_progress_bar(bar);
}

static int get_terminal_width()
{
	struct winsize w;
	if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == -1) {
		return 80;
	}
	return w.ws_col;
}

static int compute_bar_width(const char *label, int part_text_len)
{
	int term_width = get_terminal_width();
	int label_len = label ? (int)strlen(label) : 0;
	int bar_width = term_width - label_len - part_text_len - 28;
	if (bar_width > MAX_BAR_WIDTH)
		bar_width = MAX_BAR_WIDTH;
	if (bar_width < 10)
		bar_width = 10;

	return bar_width * 0.8;
}

void progress_bar_update_part_total(struct ProgressBar *bar, struct ProgressBarPart *part, uint64_t new_total)
{
	if (!bar || !part) {
		return;
	}
	part->total = new_total;

	print_progress_bar(bar);
}

static void print_progress_bar(struct ProgressBar *bar)
{
	bar->last_draw_time = timestamp_get();

	char part_text[256];
	char *ptr = part_text;
	int remain = sizeof(part_text);
	int written = snprintf(ptr, remain, "[");
	ptr += written;
	remain -= written;

	uint64_t total_sum = 0;
	uint64_t current_sum = 0;

	bool first = true;
	struct ProgressBarPart *p;
	LIST_ITERATE(bar->parts, p)
	{
		total_sum += p->total;
		current_sum += p->current;

		if (!first) {
			written = snprintf(ptr, remain, ", ");
			ptr += written;
			remain -= written;
		}

		written = snprintf(ptr, remain, "%s: %" PRIu64 "/%" PRIu64, p->label, p->current, p->total);
		ptr += written;
		remain -= written;

		first = false;
	}
	snprintf(ptr, remain, "]");

	float progress = (total_sum > 0) ? ((float)current_sum / total_sum) : 0.0f;
	if (progress > 1.0f) {
		progress = 1.0f;
	}

	timestamp_t elapsed = timestamp_get() - bar->start_time;
	int h = elapsed / 3600000000;
	int m = (elapsed % 3600000000) / 60000000;
	int s = (elapsed % 60000000) / 1000000;

	if (bar->has_drawn_once) {
		printf("\r\033[2K");
	} else {
		bar->has_drawn_once = 1;
	}

	int part_text_len = (int)(ptr - part_text) + 1;
	int bar_width = compute_bar_width(bar->label, part_text_len);
	int filled = (int)(progress * bar_width);

	char bar_line[MAX_BAR_WIDTH * 3 + 1];
	int offset = 0;
	const char *block = "━";

	for (int i = 0; i < filled; ++i) {
		memcpy(bar_line + offset, block, 3);
		offset += 3;
	}

	memset(bar_line + offset, ' ', (bar_width - filled));
	offset += (bar_width - filled);
	bar_line[offset] = '\0';

	printf("%s " COLOR_GREEN "%s %" PRIu64 "/%" PRIu64 COLOR_YELLOW " %s" COLOR_CYAN " %.1f%%" COLOR_ORANGE " %02d:%02d:%02d" COLOR_RESET,
			bar->label ? bar->label : "",
			bar_line,
			current_sum,
			total_sum,
			part_text,
			progress * 100,
			h,
			m,
			s);

	fflush(stdout);
}

void progress_bar_advance_part_current(struct ProgressBar *bar, struct ProgressBarPart *part, uint64_t increment)
{
	if (!bar || !part) {
		return;
	}

	part->current += increment;
	if (part->current > part->total) {
		part->current = part->total;
	}

	if (timestamp_get() - bar->last_draw_time < PROGRESS_BAR_UPDATE_INTERVAL) {
		return;
	}

	print_progress_bar(bar);
}

void progress_bar_reset_start_time(struct ProgressBar *bar, timestamp_t start_time)
{
	if (!bar) {
		return;
	}
	bar->start_time = start_time;
}

void progress_bar_finish(struct ProgressBar *bar)
{
	if (!bar) {
		return;
	}
	print_progress_bar(bar);
	printf("\n");
}

void progress_bar_delete(struct ProgressBar *bar)
{
	if (!bar) {
		return;
	}
	free((void *)bar->label);
	struct ProgressBarPart *p;
	LIST_ITERATE(bar->parts, p)
	{
		free((void *)p->label);
		free(p);
	}
	list_delete(bar->parts);
	free(bar);
}
