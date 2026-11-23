/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "round_timer.h"

#include <string.h>

void round_timer_start(struct round_timer *t)
{
	t->start = t->step_start = timestamp_monotonic();
	t->slowest = "none";
	t->slowest_time = 0;
}

static void end_step(struct round_timer *t, const char *name, timestamp_t now)
{
	timestamp_t length = now > t->step_start ? now - t->step_start : 0;
	if (length > t->slowest_time) {
		t->slowest_time = length;
		t->slowest = name;
	}
	t->step_start = now;
}

void round_timer_step(struct round_timer *t, const char *name)
{
	end_step(t, name, timestamp_monotonic());
}

static void window_add(struct round_window *w, const struct round_timer *t, timestamp_t length)
{
	if (!w) {
		return;
	}
	w->rounds++;
	if (length >= w->longest) {
		w->longest = length;
		w->step = t->slowest;
		w->step_time = t->slowest_time;
	}
}

timestamp_t round_timer_finish(struct round_timer *t, const char *name, struct round_window *a, struct round_window *b)
{
	timestamp_t now = timestamp_monotonic();
	end_step(t, name, now);
	timestamp_t length = now > t->start ? now - t->start : 0;
	window_add(a, t, length);
	window_add(b, t, length);
	return length;
}

void round_window_reset(struct round_window *w)
{
	memset(w, 0, sizeof(*w));
	w->start = timestamp_monotonic();
	w->step = "none";
}

timestamp_t round_window_length(const struct round_window *w)
{
	timestamp_t now = timestamp_monotonic();
	return now > w->start ? now - w->start : 0;
}
