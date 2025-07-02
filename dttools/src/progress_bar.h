#ifndef PROGRESS_BAR_H
#define PROGRESS_BAR_H

#include <time.h>

struct ProgressBar {
    const char *label;
    int total;
    int step;
    int current;
    int last_displayed;
    time_t start_time;
};

struct ProgressBar *progress_bar_init(const char *label, int total, int step);
void progress_bar_update(struct ProgressBar *bar, int increment);
void progress_bar_finish(struct ProgressBar *bar);
int progress_bar_completed(struct ProgressBar *bar);

#endif
