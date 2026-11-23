/*
Copyright (C) 2024- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MANAGER_FACTORY_H
#define VINE_MANAGER_FACTORY_H

#include "vine_manager.h"
#include "vine_worker_info.h"
#include "vine_factory_info.h"

#include "list.h"

#include <time.h>

/*
This module handles workers that report themselves
as coming from a particular factory, then allowing
the manager to query the catalog for the factory
status and remove idle workers that overflow the user's
desired limit.
*/

int  vine_manager_factory_worker_arrive( struct vine_manager *q, struct vine_worker_info *w, const char *factory_name );
void vine_manager_factory_worker_leave( struct vine_manager *q, struct vine_worker_info *w );
int  vine_manager_factory_worker_prune( struct vine_manager *q, struct vine_worker_info *w );

/* Build the query of the factories that feed the manager, for the catalog updater to run: the text of its filter, and
 * in names a list of the factory names that it asks for. Both are the caller's. */
char *vine_manager_factory_query(struct vine_manager *q, struct list **names);

/* Apply the answer to a query of vine_manager_factory_query(): the records of the factories found, or NULL if the
 * catalog could not be queried. A queried factory that the catalog no longer lists and that no worker came from is
 * forgotten. Takes the names and the records. */
void vine_manager_factory_apply(struct vine_manager *q, struct list *names, struct list *records);

#endif
