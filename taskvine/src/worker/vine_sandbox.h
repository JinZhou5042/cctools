/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_SANDBOX_H
#define VINE_SANDBOX_H

#include "vine_process.h"
#include "vine_cache.h"
#include "link.h"

char *vine_sandbox_full_path(struct vine_process *p, const char *sandbox_name);

vine_cache_status_t vine_sandbox_ensure( struct vine_process *p, struct vine_cache *c, struct link *manager, struct itable  *procs_table );

int vine_sandbox_stagein( struct vine_process *p, struct vine_cache *c);

/* Move the outputs of a completed process into the cache until the budget expires. Return 1 once every output was
 * handled, or 0 if a later call continues. Stage-out never fails: the manager figures out missing outputs. Call only
 * for a process that reap_process ended. */
int vine_sandbox_stageout( struct vine_process *p, struct vine_cache *c, struct link *manager, const struct budget *b );

#endif
