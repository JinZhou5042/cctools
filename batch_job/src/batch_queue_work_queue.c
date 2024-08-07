/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "batch_queue.h"
#include "batch_queue_internal.h"
#include "work_queue.h"
#include "work_queue_internal.h" /* EVIL */
#include "debug.h"
#include "path.h"
#include "stringtools.h"
#include "macros.h"
#include "rmsummary.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

/* For each input and output file, add the file specification to the task. */

static void specify_files(struct batch_queue *q, struct work_queue_task *t, struct list *input_files, struct list *output_files, int caching_flag)
{
	struct batch_file *bf;

	if (input_files) {
		LIST_ITERATE(input_files, bf)
		{
			work_queue_task_specify_file(t, bf->outer_name, bf->inner_name, WORK_QUEUE_INPUT, caching_flag);
		}
	}

	if (output_files) {
		LIST_ITERATE(output_files, bf)
		{
			work_queue_task_specify_file(t, bf->outer_name, bf->inner_name, WORK_QUEUE_OUTPUT, caching_flag);
		}
	}
}

static void specify_envlist(struct work_queue_task *t, struct jx *envlist)
{
	if (envlist) {
		struct jx_pair *p;
		for (p = envlist->u.pairs; p; p = p->next) {
			work_queue_task_specify_environment_variable(t, p->key->u.string_value, p->value->u.string_value);
		}
	}
}

static batch_queue_id_t batch_queue_wq_submit(struct batch_queue *q, struct batch_job *bt)
{
	struct work_queue_task *t;

	int caching_flag = WORK_QUEUE_CACHE;

	const char *caching_option = hash_table_lookup(q->options, "caching");
	if (!strcmp(caching_option, "never")) {
		caching_flag = WORK_QUEUE_NOCACHE;
	} else {
		caching_flag = WORK_QUEUE_CACHE;
	}

	t = work_queue_task_create(bt->command);

	specify_files(q, t, bt->input_files, bt->output_files, caching_flag);

	if (bt->envlist) {
		specify_envlist(t, bt->envlist);
		const char *category = jx_lookup_string(bt->envlist, "CATEGORY");
		if (category) {
			work_queue_task_specify_category(t, category);
		}
	}

	if (bt->resources) {
		work_queue_task_specify_resources(t, bt->resources);
	}

	work_queue_submit(q->wq_manager, t);

	return t->taskid;
}

static batch_queue_id_t batch_queue_wq_wait(struct batch_queue *q, struct batch_job_info *info, time_t stoptime)
{
	static int try_open_log = 0;
	int timeout, taskid = -1;

	if (!try_open_log) {
		try_open_log = 1;
		if (!work_queue_specify_log(q->wq_manager, q->logfile)) {
			return -1;
		}

		const char *transactions = batch_queue_get_option(q, "batch_log_transactions_name");
		if (transactions) {
			work_queue_specify_transactions_log(q->wq_manager, transactions);
		}
	}

	if (stoptime == 0) {
		timeout = WORK_QUEUE_WAITFORTASK;
	} else {
		timeout = MAX(0, stoptime - time(0));
	}

	struct work_queue_task *t = work_queue_wait(q->wq_manager, timeout);
	if (t) {
		info->submitted = t->time_when_submitted / 1000000;
		info->started = t->time_when_commit_end / 1000000;
		info->finished = t->time_when_done / 1000000;
		info->exited_normally = 1;
		info->exit_code = t->return_status;
		info->exit_signal = 0;
		info->disk_allocation_exhausted = t->disk_allocation_exhausted;

		/*
		   If the standard ouput of the job is not empty,
		   then print it, because this is analogous to a Unix
		   job, and would otherwise be lost.  Important for
		   capturing errors from the program.
		 */

		if (t->output && t->output[0]) {
			if (t->output[1] || t->output[0] != '\n') {
				string_chomp(t->output);
				printf("%s\n", t->output);
			}
		}

		taskid = t->taskid;
		work_queue_task_delete(t);
	}

	if (taskid >= 0) {
		return taskid;
	}

	if (work_queue_empty(q->wq_manager)) {
		return 0;
	} else {
		return -1;
	}
}

static int batch_queue_wq_remove(struct batch_queue *q, batch_queue_id_t jobid)
{
	return 0;
}

static int batch_queue_wq_create(struct batch_queue *q)
{
	strncpy(q->logfile, "wq.log", sizeof(q->logfile));

	const char *ssl_key_file = batch_queue_get_option(q, "ssl_key_file");
	const char *ssl_cert_file = batch_queue_get_option(q, "ssl_cert_file");

	if (ssl_key_file && ssl_cert_file) {
		q->wq_manager = work_queue_ssl_create(0, ssl_key_file, ssl_cert_file);
	} else {
		q->wq_manager = work_queue_create(0);
	}

	if (!q->wq_manager)
		return -1;

	work_queue_enable_process_module(q->wq_manager);
	batch_queue_set_feature(q, "absolute_path", NULL);
	batch_queue_set_feature(q, "remote_rename", "%s=%s");
	batch_queue_set_feature(q, "batch_log_name", "%s.wqlog");
	batch_queue_set_feature(q, "batch_log_transactions", "%s.tr");
	return 0;
}

static int batch_queue_wq_free(struct batch_queue *q)
{
	if (q->wq_manager) {
		work_queue_delete(q->wq_manager);
		q->wq_manager = NULL;
	}
	return 0;
}

static int batch_queue_wq_port(struct batch_queue *q)
{
	return work_queue_port(q->wq_manager);
}

static void batch_queue_wq_option_update(struct batch_queue *q, const char *what, const char *value)
{
	if (strcmp(what, "password") == 0) {
		if (value)
			work_queue_specify_password(q->wq_manager, value);
	} else if (strcmp(what, "manager-mode") == 0) {
		if (strcmp(value, "catalog") == 0)
			work_queue_specify_manager_mode(q->wq_manager, WORK_QUEUE_MANAGER_MODE_CATALOG);
		else if (strcmp(value, "standalone") == 0)
			work_queue_specify_manager_mode(q->wq_manager, WORK_QUEUE_MANAGER_MODE_STANDALONE);
	} else if (strcmp(what, "name") == 0) {
		if (value)
			work_queue_specify_name(q->wq_manager, value);
	} else if (strcmp(what, "debug") == 0) {
		if (value)
			work_queue_specify_debug_path(q->wq_manager, value);
	} else if (strcmp(what, "tlq-port") == 0) {
		if (value)
			work_queue_specify_tlq_port(q->wq_manager, atoi(value));
	} else if (strcmp(what, "priority") == 0) {
		if (value)
			work_queue_specify_priority(q->wq_manager, atoi(value));
		else
			work_queue_specify_priority(q->wq_manager, 0);
	} else if (strcmp(what, "fast-abort") == 0) {
		if (value)
			work_queue_activate_fast_abort(q->wq_manager, atof(value));
	} else if (strcmp(what, "estimate-capacity") == 0) {
		work_queue_specify_estimate_capacity_on(q->wq_manager, string_istrue(value));
	} else if (strcmp(what, "keepalive-interval") == 0) {
		if (value)
			work_queue_specify_keepalive_interval(q->wq_manager, atoi(value));
		else
			work_queue_specify_keepalive_interval(q->wq_manager, WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL);
	} else if (strcmp(what, "keepalive-timeout") == 0) {
		if (value)
			work_queue_specify_keepalive_timeout(q->wq_manager, atoi(value));
		else
			work_queue_specify_keepalive_timeout(q->wq_manager, WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT);
	} else if (strcmp(what, "manager-preferred-connection") == 0) {
		if (value)
			work_queue_manager_preferred_connection(q->wq_manager, value);
		else
			work_queue_manager_preferred_connection(q->wq_manager, "by_ip");
	} else if (strcmp(what, "category-limits") == 0) {
		struct rmsummary *s = rmsummary_parse_string(value);
		if (s) {
			work_queue_specify_category_max_resources(q->wq_manager, s->category, s);
			rmsummary_delete(s);
		} else {
			debug(D_NOTICE, "Could no parse '%s' as a summary of resorces encoded in JSON\n", value);
		}
	} else if (!strcmp(what, "scheduler")) {
		if (!strcmp(value, "files")) {
			work_queue_specify_algorithm(q->wq_manager, WORK_QUEUE_SCHEDULE_FILES);
		} else if (!strcmp(value, "time")) {
			work_queue_specify_algorithm(q->wq_manager, WORK_QUEUE_SCHEDULE_TIME);
		} else if (!strcmp(value, "fcfs")) {
			work_queue_specify_algorithm(q->wq_manager, WORK_QUEUE_SCHEDULE_FCFS);
		} else if (!strcmp(value, "random")) {
			work_queue_specify_algorithm(q->wq_manager, WORK_QUEUE_SCHEDULE_RAND);
		} else {
			debug(D_NOTICE | D_BATCH, "unknown scheduling mode %s\n", optarg);
		}
	}
}

const struct batch_queue_module batch_queue_wq = {
		BATCH_QUEUE_TYPE_WORK_QUEUE,
		"wq",

		batch_queue_wq_create,
		batch_queue_wq_free,
		batch_queue_wq_port,
		batch_queue_wq_option_update,

		batch_queue_wq_submit,
		batch_queue_wq_wait,
		batch_queue_wq_remove,
};

/* vim: set noexpandtab tabstop=8: */
