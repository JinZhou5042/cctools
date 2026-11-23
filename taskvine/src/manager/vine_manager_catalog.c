/*
Copyright (C) 2026 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager_catalog.h"

#include "catalog_query.h"
#include "debug.h"
#include "jx.h"
#include "jx_parse.h"
#include "xxmalloc.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Seconds that a factory query may take, as when the manager's thread ran it. */
#define VINE_CATALOG_QUERY_TIMEOUT 5

/* One update that the manager posted: what to send and to whom, and what to ask. */
struct catalog_update {
	char *hosts;		    /* Own the list of catalog servers. */
	char *status;		    /* Own the status sent to them. */
	char *lean;		    /* Own the shorter status sent when the first is too large. */
	char *factory_filter;	    /* Own the text of a factory query, or NULL for none. */
	struct list *factory_names; /* Own the names of the factories that the query asks for, or NULL. */
};

/* State shared by the manager's thread and the updater's thread, under lock. Each of the two holds a reference, and
 * the last to let go frees it, so a thread that is still waiting for a catalog server when the manager is deleted
 * finishes on its own. */
struct vine_manager_catalog {
	pthread_t thread;
	pthread_mutex_t lock;
	pthread_cond_t changed;		     /* Signal a new update, a stop, or the end of the thread. */
	struct catalog_update *pending;	     /* The update posted last that the thread has not started, or NULL. */
	struct list *answer_names;	     /* Names of the last completed query whose answer was not taken. */
	struct list *answer_records;	     /* Its factory records, or NULL if the catalog could not be queried. */
	int answered;			     /* Set while an answer waits to be taken. */
	int stopping;			     /* Set when the manager no longer posts updates. */
	int finished;			     /* Set when the thread has sent its last update. */
	int references;			     /* Holders of the updater: the manager and the thread. */
};

/* catalog_query and domain_name_cache keep process-wide state without a lock of their own, so the updaters of all
 * managers in a process take turns. */
static pthread_mutex_t catalog_turn = PTHREAD_MUTEX_INITIALIZER;

static void free_names(struct list *names)
{
	if (names) {
		list_clear(names, free);
		list_delete(names);
	}
}

static void free_records(struct list *records)
{
	if (records) {
		list_clear(records, (void *)jx_delete);
		list_delete(records);
	}
}

static void update_delete(struct catalog_update *u)
{
	if (!u) {
		return;
	}
	free(u->hosts);
	free(u->status);
	free(u->lean);
	free(u->factory_filter);
	free_names(u->factory_names);
	free(u);
}

static void updater_release_locked(struct vine_manager_catalog *c, int *last)
{
	c->references--;
	*last = c->references == 0;
}

static void updater_free(struct vine_manager_catalog *c)
{
	update_delete(c->pending);
	free_names(c->answer_names);
	free_records(c->answer_records);
	pthread_cond_destroy(&c->changed);
	pthread_mutex_destroy(&c->lock);
	free(c);
}

/* Send one update, and run its factory query unless the manager stops. Return the query's records, or NULL if the
 * catalog could not be queried, in which case no factory counts as gone. Called with catalog_turn held. */
static struct list *send_update(struct catalog_update *u, int query)
{
	debug(D_VINE, "Advertising manager status to the catalog server(s) at %s ...", u->hosts);
	/* Without a background send, the update is sent over a connection of this thread, without a fork of the
	 * manager process. A full status too large for a datagram is replaced by the lean one. */
	if (!catalog_query_send_update(u->hosts, u->status, CATALOG_UPDATE_CONDITIONAL)) {
		catalog_query_send_update(u->hosts, u->lean, 0);
	}

	if (!query || !u->factory_filter) {
		return NULL;
	}

	debug(D_VINE, "Retrieving factory info from catalog server(s) at %s ...", u->hosts);
	struct jx *filter = jx_parse_string(u->factory_filter);
	time_t stoptime = time(0) + VINE_CATALOG_QUERY_TIMEOUT;
	struct catalog_query *cq = catalog_query_create(u->hosts, filter, stoptime);
	if (!cq) {
		jx_delete(filter);
		return NULL;
	}
	struct list *records = list_create();
	struct jx *j;
	while ((j = catalog_query_read(cq, stoptime))) {
		list_push_tail(records, j);
	}
	catalog_query_delete(cq);
	return records;
}

static void *updater_run(void *argument)
{
	struct vine_manager_catalog *c = argument;

	pthread_mutex_lock(&c->lock);
	while (1) {
		while (!c->pending && !c->stopping) {
			pthread_cond_wait(&c->changed, &c->lock);
		}
		struct catalog_update *u = c->pending;
		c->pending = NULL;
		if (!u) {
			break;
		}
		int query = !c->stopping;
		pthread_mutex_unlock(&c->lock);

		pthread_mutex_lock(&catalog_turn);
		struct list *records = send_update(u, query);
		pthread_mutex_unlock(&catalog_turn);

		pthread_mutex_lock(&c->lock);
		if (query && u->factory_filter) {
			/* A newer answer replaces one that the manager has not taken. */
			free_names(c->answer_names);
			free_records(c->answer_records);
			c->answer_names = u->factory_names;
			c->answer_records = records;
			c->answered = 1;
			u->factory_names = NULL;
		}
		update_delete(u);
	}
	c->finished = 1;
	pthread_cond_broadcast(&c->changed);
	int last;
	updater_release_locked(c, &last);
	pthread_mutex_unlock(&c->lock);
	if (last) {
		updater_free(c);
	}
	return NULL;
}

struct vine_manager_catalog *vine_manager_catalog_create(void)
{
	struct vine_manager_catalog *c = calloc(1, sizeof(*c));
	if (!c) {
		return NULL;
	}
	pthread_mutex_init(&c->lock, NULL);
	pthread_cond_init(&c->changed, NULL);
	c->references = 2;

	/* The thread inherits a mask that blocks every signal, so signals meant for the application, such as SIGINT,
	 * reach its own threads. */
	sigset_t all, previous;
	sigfillset(&all);
	pthread_sigmask(SIG_BLOCK, &all, &previous);
	int result = pthread_create(&c->thread, NULL, updater_run, c);
	pthread_sigmask(SIG_SETMASK, &previous, NULL);
	if (result != 0) {
		debug(D_NOTICE | D_VINE, "could not start the catalog updater: %s", strerror(result));
		pthread_cond_destroy(&c->changed);
		pthread_mutex_destroy(&c->lock);
		free(c);
		return NULL;
	}
	return c;
}

void vine_manager_catalog_post(struct vine_manager_catalog *c, const char *hosts, char *status, char *lean, char *factory_filter, struct list *factory_names)
{
	struct catalog_update *u = malloc(sizeof(*u));
	if (!u) {
		free(status);
		free(lean);
		free(factory_filter);
		free_names(factory_names);
		return;
	}
	u->hosts = xxstrdup(hosts);
	u->status = status;
	u->lean = lean;
	u->factory_filter = factory_filter;
	u->factory_names = factory_names;

	pthread_mutex_lock(&c->lock);
	/* An update that the thread has not started is out of date. */
	update_delete(c->pending);
	c->pending = u;
	pthread_cond_signal(&c->changed);
	pthread_mutex_unlock(&c->lock);
}

int vine_manager_catalog_take_factories(struct vine_manager_catalog *c, struct list **factory_names, struct list **records)
{
	/* The manager's thread asks in every step, and the thread holds the lock only to hand over pointers. */
	pthread_mutex_lock(&c->lock);
	int answered = c->answered;
	if (answered) {
		*factory_names = c->answer_names;
		*records = c->answer_records;
		c->answer_names = NULL;
		c->answer_records = NULL;
		c->answered = 0;
	}
	pthread_mutex_unlock(&c->lock);
	return answered;
}

void vine_manager_catalog_delete(struct vine_manager_catalog *c, int seconds)
{
	if (!c) {
		return;
	}
	struct timespec deadline;
	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += seconds;

	pthread_mutex_lock(&c->lock);
	c->stopping = 1;
	pthread_cond_broadcast(&c->changed);
	while (!c->finished) {
		if (pthread_cond_timedwait(&c->changed, &c->lock, &deadline) == ETIMEDOUT) {
			break;
		}
	}
	int finished = c->finished;
	/* Once the lock is released, an unfinished thread may free the updater at any time. */
	pthread_t thread = c->thread;
	int last;
	updater_release_locked(c, &last);
	pthread_mutex_unlock(&c->lock);

	if (finished) {
		pthread_join(thread, NULL);
	} else {
		debug(D_VINE, "the catalog server(s) did not take the last update within %d s", seconds);
		pthread_detach(thread);
	}
	if (last) {
		updater_free(c);
	}
}
