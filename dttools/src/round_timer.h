/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef ROUND_TIMER_H
#define ROUND_TIMER_H

/** @file round_timer.h
Measure the rounds of a budgeted main loop (see @ref budget.h). A round is the work between two waits of the loop. A
loop starts its timer after each wait, ends each named step with @ref round_timer_step, and ends the round with @ref
round_timer_finish, which adds the round to one or more windows. A window keeps the number of rounds, the longest
round, and the slowest step of that round, which tell how long the loop was unable to answer because of its own work:
the response time X of the timeout framework is the longest round of a window. Each call costs one reading of the
monotonic clock and a few comparisons, so a timer adds O(1) work per step and per round.
*/

#include "timestamp.h"

#include <stdint.h>

/** The steps of the current round. Step names are static strings without spaces, so they can appear in messages. */
struct round_timer {
	timestamp_t start;	  /**< When the round began, after the loop's wait, in @ref timestamp_monotonic time. */
	timestamp_t step_start;	  /**< When the current step began. */
	const char *slowest;	  /**< Name of the slowest step of the round so far, or "none". */
	timestamp_t slowest_time; /**< Duration of that step, in microseconds. */
};

/** The rounds of a span of time, such as the time since the last report. */
struct round_window {
	timestamp_t start;	  /**< When the window began, in @ref timestamp_monotonic time. */
	int64_t rounds;		  /**< Rounds that ended in the window. */
	timestamp_t longest;	  /**< Duration of the longest round, in microseconds. */
	const char *step;	  /**< Slowest step of the longest round, or "none". */
	timestamp_t step_time;	  /**< Duration of that step, in microseconds. */
};

/** Begin a round, right after the loop's wait.
@param t The timer.
*/
void round_timer_start(struct round_timer *t);

/** End the current step of a round.
@param t The timer.
@param name The name of the step that just ended, a static string.
*/
void round_timer_step(struct round_timer *t, const char *name);

/** End a round, its last step named, and add it to the given windows.
@param t The timer.
@param name The name of the last step of the round, a static string.
@param a A window to add the round to, or NULL.
@param b Another window to add the round to, or NULL.
@return The duration of the round, in microseconds.
*/
timestamp_t round_timer_finish(struct round_timer *t, const char *name, struct round_window *a, struct round_window *b);

/** Begin a new window that holds no rounds.
@param w The window.
*/
void round_window_reset(struct round_window *w);

/** Report the length of a window so far.
@param w The window.
@return The microseconds since the window began.
*/
timestamp_t round_window_length(const struct round_window *w);

#endif
