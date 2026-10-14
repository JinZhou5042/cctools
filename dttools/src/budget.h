/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BUDGET_H
#define BUDGET_H

/** @file budget.h
A time budget for local work. A loop that must stay responsive to other parties starts a budget, does work while the
budget is not expired, and yields when it is, resuming the remaining work in its next round. A budget limits how long
we ourselves work before yielding. It is not a timeout on another party, which is judged by progress instead.
*/

#include "timestamp.h"

/** A time budget. */
struct budget {
	timestamp_t end; /**< Time of @ref timestamp_monotonic at which the budget expires. */
};

/** Start a budget that expires after the given length.
@param b The budget to start.
@param length The length of the budget in microseconds.
*/
void budget_start(struct budget *b, timestamp_t length);

/** Check whether a budget has expired.
@param b The budget to check.
@return Non-zero once the budget has expired, zero while time is left.
*/
int budget_expired(const struct budget *b);

/** Report the time left in a budget.
@param b The budget to examine.
@return The microseconds left before the budget expires, or zero once it has expired.
*/
timestamp_t budget_left(const struct budget *b);

#endif
