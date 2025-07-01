#include "progress_bar.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/ioctl.h>

#define MAX_BAR_WIDTH 30

#define COLOR_RESET  "\033[0m"
#define COLOR_GREEN  "\033[32m"
#define COLOR_BLUE   "\033[34m"
#define COLOR_PURPLE "\033[35m"
#define COLOR_CYAN   "\033[38;2;0;255;255m"

static int get_terminal_width() {
    struct winsize w;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == -1) {
        return 80;
    }
    return w.ws_col;
}

static int compute_bar_width(const char *label) {
    int term_width = get_terminal_width();
    int bar_width = term_width - (int)strlen(label) - 28;
    if (bar_width > MAX_BAR_WIDTH) bar_width = MAX_BAR_WIDTH;
    if (bar_width < 10) bar_width = 10;
    return bar_width;
}

struct ProgressBar *progress_bar_init(const char *label, int total, int step) {
    struct ProgressBar *bar = malloc(sizeof(struct ProgressBar));
    bar->label = label;
    bar->total = total;
    bar->step = step;
    bar->current = 0;
    bar->last_displayed = -1;
    bar->start_time = time(NULL);
    return bar;
}

void progress_bar_update(struct ProgressBar *bar, int increment) {
    bar->current += increment;

    if (bar->current < bar->total &&
        bar->current / bar->step == bar->last_displayed / bar->step) {
        return;
    }

    bar->last_displayed = bar->current;

    float progress = (float)bar->current / bar->total;
    if (progress > 1.0f) progress = 1.0f;

    int bar_width = compute_bar_width(bar->label);
    int filled = (int)(progress * bar_width);

    time_t now = time(NULL);
    int elapsed = now - bar->start_time;
    int h = elapsed / 3600, m = (elapsed % 3600) / 60, s = elapsed % 60;

    printf("\r%s " COLOR_GREEN, bar->label);
    for (int i = 0; i < bar_width; ++i) {
        printf(i < filled ? "━" : " ");
    }
    printf(COLOR_GREEN " %d/%d" COLOR_CYAN " %.1f%%" COLOR_PURPLE " %02d:%02d:%02d" COLOR_RESET,
           bar->current, bar->total,
           ((float)bar->current / bar->total) * 100, h, m, s);
    fflush(stdout);
}

void progress_bar_finish(struct ProgressBar *bar) {
    float progress = (float)bar->current / bar->total;
    if (progress > 1.0f) progress = 1.0f;

    int bar_width = compute_bar_width(bar->label);
    int filled = (int)(progress * bar_width);

    time_t now = time(NULL);
    int elapsed = now - bar->start_time;
    int h = elapsed / 3600, m = (elapsed % 3600) / 60, s = elapsed % 60;

    printf("\r%s " COLOR_GREEN, bar->label);
    for (int i = 0; i < bar_width; ++i) {
        printf(i < filled ? "━" : " ");
    }
    printf(COLOR_GREEN " %d/%d" COLOR_CYAN " %.1f%%" COLOR_PURPLE " %02d:%02d:%02d" COLOR_RESET,
           bar->current, bar->total,
           ((float)bar->current / bar->total) * 100, h, m, s);
    printf("\n");
    fflush(stdout);

    free(bar);
}