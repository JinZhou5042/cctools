/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "budget.h"

void budget_start(struct budget *b, timestamp_t length)
{
	b->end = timestamp_monotonic() + length;
}

int budget_expired(const struct budget *b)
{
	return timestamp_monotonic() >= b->end;
}

timestamp_t budget_left(const struct budget *b)
{
	timestamp_t now = timestamp_monotonic();
	if (now >= b->end) {
		return 0;
	}
	return b->end - now;
}
