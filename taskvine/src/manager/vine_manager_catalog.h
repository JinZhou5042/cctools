/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_MANAGER_CATALOG_H
#define VINE_MANAGER_CATALOG_H

/*
The catalog updater of a manager: a thread that sends the manager's status to the catalog servers and queries them for
the factories that feed the manager, so that the manager's thread never waits for a name lookup, a connection, or a
catalog server, and never forks the manager process to send an update. The manager's thread builds each update and
posts it, and later takes the factory records that the thread received. This module is internal to the manager.
*/

#include "list.h"

struct vine_manager_catalog;

/** Create a catalog updater and start its thread.
@return The updater, or NULL if it could not be created.
*/
struct vine_manager_catalog *vine_manager_catalog_create(void);

/** Post an update for the thread to send, replacing one that it has not started yet.
@param c The updater.
@param hosts The catalog servers, which the updater copies.
@param status The manager's status, which the updater owns from now on.
@param lean A shorter status sent when the first is too large for the catalog, which the updater owns from now on.
@param factory_filter A query of the factories that feed the manager, or NULL for none. The updater owns it.
@param factory_names The names of the factories that the query asks for, or NULL. The updater owns the list and its
strings, and gives them back with the answer.
*/
void vine_manager_catalog_post(struct vine_manager_catalog *c, const char *hosts, char *status, char *lean, char *factory_filter, struct list *factory_names);

/** Take the answer of the last factory query that the thread completed, if one came since the last call.
@param c The updater.
@param factory_names Set to the names that the query asked for, which the caller then owns.
@param records Set to the list of factory records (struct jx) that the catalog returned, which the caller then owns,
or to NULL if the catalog could not be queried.
@return One if an answer was taken, zero otherwise.
*/
int vine_manager_catalog_take_factories(struct vine_manager_catalog *c, struct list **factory_names, struct list **records);

/** Stop the updater. The thread sends the update posted last, and the caller waits at most the given seconds for it.
If the thread has not finished by then, it is left to finish and free the updater on its own.
@param c The updater, or NULL.
@param seconds The longest wait.
*/
void vine_manager_catalog_delete(struct vine_manager_catalog *c, int seconds);

#endif
