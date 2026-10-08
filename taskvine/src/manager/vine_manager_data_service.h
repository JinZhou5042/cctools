/* Manage the Manager data port and asynchronous transfers in both directions.
 * The Manager thread owns file lookup and completion callbacks. Executors only perform network and file I/O.
 * All entry points except executor functions run on the Manager thread. */
#ifndef VINE_MANAGER_DATA_SERVICE_H
#define VINE_MANAGER_DATA_SERVICE_H

struct vine_manager;
struct vine_manager_data_service;
struct link_info;

/* Create a data listener and a bounded executor pool. */
struct vine_manager_data_service *vine_manager_data_service_create(void);
/* Cancel outstanding transfers and release their resources without invoking callbacks. */
void vine_manager_data_service_delete(struct vine_manager_data_service *ds);
/* Return the data port advertised to Workers. */
int vine_manager_data_service_port(struct vine_manager_data_service *ds);
/* Fill two poll entries for the listener and completion notification. */
void vine_manager_data_service_poll(struct vine_manager_data_service *ds, struct link_info *entries);
/* Accept connections and process a bounded batch of executor results. */
void vine_manager_data_service_handle(struct vine_manager *manager);
/* Fetch one regular file atomically. Return zero when busy or unable to submit.
 * A successful submission calls complete on the Manager thread with one for success or zero for failure.
 * The caller owns argument until completion or Manager deletion. */
int vine_manager_data_service_get(struct vine_manager *manager, const char *ip, int port, const char *name, const char *path, void (*complete)(void *, int), void *argument);

#endif
