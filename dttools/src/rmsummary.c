/*
  Copyright (C) 2022 The University of Notre Dame This software is
  distributed under the GNU General Public License.  See the file
  COPYING for details.
*/

/* In a summary file, all time fields are written as double, with
   units in seconds. Internally, however, time fields are kept as
   int64_t, with units in microseconds. Therefore we need to convert back
   and forth when reading/printing summaries.

   Similarly, disk is reported as double, in megabytes,
   but it is kept internally as int64_t, in bytes.*/

#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ctype.h>

#include "buffer.h"
#include "debug.h"
#include "hash_table.h"
#include "int_sizes.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"
#include "jx_print.h"
#include "list.h"
#include "macros.h"
#include "rmsummary.h"
#include "stringtools.h"
#include "xxmalloc.h"

struct resource_info {
	const char *name;
	const char *units;
	int decimals;
	size_t offset;
};

// name                       units decimals   offset
static const struct resource_info resources_info[] = {{"start", "s", 6, offsetof(struct rmsummary, start)},
		{"end", "s", 6, offsetof(struct rmsummary, end)},
		{"wall_time", "s", 6, offsetof(struct rmsummary, wall_time)},
		{"cpu_time", "s", 6, offsetof(struct rmsummary, cpu_time)},
		{"memory", "MB", 0, offsetof(struct rmsummary, memory)},
		{"virtual_memory", "MB", 0, offsetof(struct rmsummary, virtual_memory)},
		{"swap_memory", "MB", 0, offsetof(struct rmsummary, swap_memory)},
		{"disk", "MB", 0, offsetof(struct rmsummary, disk)},
		{"bytes_read", "MB", 0, offsetof(struct rmsummary, bytes_read)},
		{"bytes_written", "MB", 0, offsetof(struct rmsummary, bytes_written)},
		{"bytes_received", "MB", 0, offsetof(struct rmsummary, bytes_received)},
		{"bytes_sent", "MB", 0, offsetof(struct rmsummary, bytes_sent)},
		{"bandwidth", "Mbps", 3, offsetof(struct rmsummary, bandwidth)},
		{"gpus", "gpus", 0, offsetof(struct rmsummary, gpus)},
		{"cores", "cores", 3, offsetof(struct rmsummary, cores)},
		{"cores_avg", "cores", 3, offsetof(struct rmsummary, cores_avg)},
		{"machine_cpus", "cores", 3, offsetof(struct rmsummary, machine_cpus)},
		{"machine_load", "procs", 0, offsetof(struct rmsummary, machine_load)},
		{"context_switches", "switches", 0, offsetof(struct rmsummary, context_switches)},
		{"max_concurrent_processes", "procs", 0, offsetof(struct rmsummary, max_concurrent_processes)},
		{"total_processes", "procs", 0, offsetof(struct rmsummary, total_processes)},
		{"total_files", "files", 0, offsetof(struct rmsummary, total_files)},
		{"fs_nodes", "nodes", 0, offsetof(struct rmsummary, fs_nodes)},
		{"workers", "workers", 0, offsetof(struct rmsummary, workers)}};

static char **resources_names = NULL;

/* reverse map for resource_info. Lookup by resource name rather than
 * sequential access. Use to print resources with the correct number of
 * decimals. */
struct hash_table *info_of_resource_table = NULL;

/* Using this very cheap hash function, as we only use it for the small set of
 * resources */
unsigned hash_fn(const char *key)
{
	return (key[0] << 8) + strlen(key);
}

/* The table is created once, because managers in different threads of one process look up resources at once. */
static pthread_once_t info_of_resource_once = PTHREAD_ONCE_INIT;

static void info_of_resource_create(void)
{
	info_of_resource_table = hash_table_create(0, 0);
	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		hash_table_insert(info_of_resource_table, resources_info[i].name, (void *)&resources_info[i]);
	}
}

static const struct resource_info *info_of_resource(const char *resource_name)
{
	pthread_once(&info_of_resource_once, info_of_resource_create);
	return hash_table_lookup(info_of_resource_table, resource_name);
}

const char *rmsummary_resource_units(const char *resource_name)
{
	const struct resource_info *info = info_of_resource(resource_name);

	if (!info) {
		return NULL;
	}

	return info->units;
}

int rmsummary_resource_decimals(const char *resource_name)
{
	const struct resource_info *info = info_of_resource(resource_name);

	if (!info) {
		return 0;
	}

	return info->decimals;
}

size_t rmsummary_resource_offset(const char *resource_name)
{
	const struct resource_info *info = info_of_resource(resource_name);

	if (!info) {
		fatal("No such resource.");
	}

	return info->offset;
}

double rmsummary_get_by_offset(const struct rmsummary *s, size_t offset)
{
	return (*((double *)((char *)s + offset)));
}

void rmsummary_set_by_offset(struct rmsummary *s, size_t offset, double value)
{
	*((double *)((char *)s + offset)) = value;
}

static int set_meta_char_field(struct rmsummary *s, const char *key, char *value)
{
	if (strcmp(key, "category") == 0) {
		free(s->category);
		s->category = xxstrdup(value);
		return 1;
	}

	if (strcmp(key, "command") == 0) {
		free(s->command);
		s->command = xxstrdup(value);
		return 1;
	}

	if (strcmp(key, "exit_type") == 0) {
		free(s->exit_type);
		s->exit_type = xxstrdup(value);
		return 1;
	}

	if (strcmp(key, "taskid") == 0) {
		free(s->taskid);
		s->taskid = xxstrdup(value);
		return 1;
	}

	if (strcmp(key, "task_id") == 0) {
		free(s->taskid);
		s->taskid = xxstrdup(value);
		return 1;
	}

	if (strcmp(key, "snapshot_name") == 0) {
		free(s->snapshot_name);
		s->snapshot_name = xxstrdup(value);
		return 1;
	}

	return 0;
}

static int set_meta_int_field(struct rmsummary *s, const char *key, int64_t value)
{
	if (strcmp(key, "signal") == 0) {
		s->signal = value;
		return 1;
	}

	if (strcmp(key, "exit_status") == 0) {
		s->exit_status = value;
		return 1;
	}

	if (strcmp(key, "last_error") == 0) {
		s->last_error = value;
		return 1;
	}

	if (strcmp(key, "snapshots_count") == 0) {
		s->snapshots_count = value;
		return 1;
	}

	return 0;
}

size_t rmsummary_num_resources()
{
	return sizeof(resources_info) / sizeof(resources_info[0]);
}

double rmsummary_get(const struct rmsummary *s, const char *resource)
{
	const struct resource_info *info = info_of_resource(resource);
	if (!info) {
		notice(D_RMON, "There is not a resource named '%s'.", resource);
		return -1;
	}

	return rmsummary_get_by_offset(s, info->offset);
}

int rmsummary_set(struct rmsummary *s, const char *resource, double value)
{
	const struct resource_info *info = info_of_resource(resource);
	if (!info) {
		notice(D_RMON, "There is not a resource named '%s'.", resource);
		return -1;
	}

	rmsummary_set_by_offset(s, info->offset, value);

	return 1;
}

void rmsummary_add_snapshots(struct rmsummary *s, struct jx *array)
{

	if (!array) {
		return;
	}

	int n = jx_array_length(array);

	if (n < 1) {
		return;
	}

	s->snapshots_count = n;
	s->snapshots = calloc(n + 1, sizeof(struct rmsummary *));
	s->snapshots[n] = NULL; /* terminate in NULL for convenience */

	int count = 0;
	struct jx *snapshot;
	for (void *i = NULL; (snapshot = jx_iterate_array(array, &i)); /* nothing */) {
		struct rmsummary *snap = json_to_rmsummary(snapshot);

		if (!snap) {
			fatal("malformed resource summary snapshot.");
		}

		s->snapshots[count] = snap;
		count++;
	}
}

int rmsummary_assign_summary_field(struct rmsummary *s, char *key, struct jx *value)
{

	if (strcmp(key, "limits_exceeded") == 0) {
		s->limits_exceeded = json_to_rmsummary(value);
		return 1;
	} else if (strcmp(key, "peak_times") == 0) {
		s->peak_times = json_to_rmsummary(value);
		return 1;
	}

	fatal("There is not a resource named '%s'.", key);

	return 0;
}

/* round to the specified number of decimals, for a resource that has decimals */
static double round_to_decimals(double value, int decimals)
{
	double factor = pow(10, decimals);
	int64_t scaled = (jx_int_t)((value * factor) + 0.5);

	return scaled / factor;
}

/* truncate to specified number of decimals */
struct jx *value_to_jx_number(double value, int decimals)
{
	if (decimals == 0) {
		return jx_integer((jx_int_t)value);
	}

	return jx_double(round_to_decimals(value, decimals));
}

struct jx *peak_times_to_json(struct rmsummary *s)
{
	struct jx *output = jx_object(NULL);

	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		const struct resource_info *info = &resources_info[i];

		const char *r = info->name;
		size_t offset = info->offset;

		double value = rmsummary_get_by_offset(s, offset);

		if (value < 0) {
			continue;
		}

		jx_insert(output, jx_string(r), jx_arrayv(value_to_jx_number(value, 3), jx_string("s"), NULL));
	}

	return output;
}

struct jx *rmsummary_to_json(const struct rmsummary *s, int only_resources)
{
	struct jx *output = jx_object(NULL);

	if (!only_resources) {
		if (s->snapshots_count > 0) {
			int i;
			struct jx *snapshots = jx_array(NULL);
			for (i = s->snapshots_count - 1; i >= 0; i--) {
				struct jx *j = rmsummary_to_json(s->snapshots[i], 1);
				jx_insert(j, jx_string("snapshot_name"), jx_string(s->snapshots[i]->snapshot_name));
				jx_array_insert(snapshots, j);
			}
			jx_insert(output, jx_string("snapshots"), snapshots);
		}

		if (s->peak_times) {
			struct jx *peaks = peak_times_to_json(s->peak_times);
			jx_insert(output, jx_string("peak_times"), peaks);
		}
	}

	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		/* inserting fields in reverse order, as it looks better */
		const struct resource_info *info = &resources_info[rmsummary_num_resources() - i - 1];
		const char *r = info->name;
		const char *units = info->units;
		int decimals = info->decimals;
		double value = rmsummary_get_by_offset(s, info->offset);

		/* do not output undefined values */
		if (value < 0) {
			continue;
		}

		struct jx *value_with_units = jx_arrayv(value_to_jx_number(value, decimals), jx_string(units), NULL);
		jx_insert(output, jx_string(r), value_with_units);
	}

	if (!only_resources) {
		if (s->exit_type) {
			if (strcmp(s->exit_type, "signal") == 0) {
				jx_insert_integer(output, "signal", s->signal);
				jx_insert_string(output, "exit_type", "signal");
			} else if (strcmp(s->exit_type, "limits") == 0) {
				if (s->limits_exceeded) {
					struct jx *lim = rmsummary_to_json(s->limits_exceeded, 1);
					jx_insert(output, jx_string("limits_exceeded"), lim);
				}
				jx_insert_string(output, "exit_type", "limits");
			} else {
				jx_insert_string(output, "exit_type", s->exit_type);
			}
		}

		if (s->last_error) {
			jx_insert_integer(output, "last_error", s->last_error);
		}

		if (s->snapshot_name) {
			jx_insert_string(output, "snapshot_name", s->snapshot_name);
		} else {
			jx_insert_integer(output, "exit_status", s->exit_status);
		}

		if (s->command) {
			jx_insert_string(output, "command", s->command);
		}

		if (s->taskid) {
			jx_insert_string(output, "taskid", s->taskid);
		}

		if (s->category) {
			jx_insert_string(output, "category", s->category);
		}
	}

	return output;
}

static double json_number_of_array(struct jx *array)
{

	struct jx_item *first = array->u.items;
	if (!first) {
		/* return undefined if array is empty */
		return -1;
	}

	double number;
	if (jx_istype(first->value, JX_DOUBLE)) {
		number = first->value->u.double_value;
	} else if (jx_istype(first->value, JX_INTEGER)) {
		number = (double)first->value->u.integer_value;
	} else {
		number = -1;
	}

	return number;
}

struct rmsummary *json_to_rmsummary(struct jx *j)
{
	if (!j || !jx_istype(j, JX_OBJECT))
		return NULL;

	struct rmsummary *s = rmsummary_create(-1);

	struct jx_pair *head = j->u.pairs;
	while (head) {
		if (!jx_istype(head->key, JX_STRING))
			continue;

		char *key = head->key->u.string_value;
		struct jx *value = head->value;

		if (jx_istype(value, JX_STRING)) {
			set_meta_char_field(s, key, value->u.string_value);
		} else if (jx_istype(value, JX_INTEGER)) {
			set_meta_int_field(s, key, value->u.integer_value);
		} else if (jx_istype(value, JX_ARRAY) && strcmp(key, "snapshots") == 0) {
			rmsummary_add_snapshots(s, value);
		} else if (jx_istype(value, JX_ARRAY)) {
			/* finally we get to resources... */
			double number = json_number_of_array(value);
			rmsummary_set(s, key, number);
		} else if (jx_istype(value, JX_OBJECT)) {
			rmsummary_assign_summary_field(s, key, value);
		}

		head = head->next;
	}

	// compute avg cores
	double wall_time = rmsummary_get(s, "wall_time");
	double cpu_time = rmsummary_get(s, "cpu_time");
	if (wall_time > 0 && cpu_time >= 0) {
		rmsummary_set(s, "cores_avg", cpu_time / wall_time);
	}

	return s;
}

/* Parse the file, assuming there is a single summary in it. */
struct rmsummary *rmsummary_parse_file_single(const char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");

	if (!stream) {
		debug(D_RMON, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	struct jx *j = jx_parse_stream(stream);
	fclose(stream);

	if (!j)
		return NULL;

	struct rmsummary *s = json_to_rmsummary(j);
	jx_delete(j);

	return s;
}

struct rmsummary *rmsummary_parse_string(const char *str)
{
	if (!str)
		return NULL;

	struct jx *j = jx_parse_string(str);

	if (!j)
		return NULL;

	struct rmsummary *s = json_to_rmsummary(j);

	jx_delete(j);
	return s;
}

/* Parse the file assuming there are multiple summaries in it. Summary
   boundaries are lines starting with # */
struct list *rmsummary_parse_file_multiple(const char *filename)
{
	FILE *stream;
	stream = fopen(filename, "r");
	if (!stream) {
		debug(D_RMON, "Cannot open resources summary file: %s : %s\n", filename, strerror(errno));
		return NULL;
	}

	struct jx_parser *p = jx_parser_create(0);
	jx_parser_read_stream(p, stream);

	struct list *lst = list_create();
	struct rmsummary *s;

	do {
		struct jx *j = jx_parser_yield(p);

		if (!j)
			break;

		s = json_to_rmsummary(j);
		jx_delete(j);

		if (s)
			list_push_tail(lst, s);
	} while (s);

	fclose(stream);
	jx_parser_delete(p);

	return lst;
}

/* Parse the stream for the next summary */
struct rmsummary *rmsummary_parse_next(FILE *stream)
{
	struct jx *j = jx_parse_stream(stream);
	if (!j)
		return NULL;

	struct rmsummary *s = json_to_rmsummary(j);

	jx_delete(j);
	return s;
}

void rmsummary_print(FILE *stream, struct rmsummary *s, int pprint, struct jx *verbatim_fields)
{
	struct jx *jsum = rmsummary_to_json(s, 0);

	if (verbatim_fields) {
		if (!jx_istype(verbatim_fields, JX_OBJECT)) {
			fatal("Vebatim fields are not a json object.");
		}
		struct jx_pair *head = verbatim_fields->u.pairs;

		while (head) {
			jx_insert(jsum, jx_copy(head->key), jx_copy(head->value));
			head = head->next;
		}
	}

	if (pprint) {
		jx_pretty_print_stream(jsum, stream);
	} else {
		jx_print_stream(jsum, stream);
	}

	jx_delete(jsum);
}

int rmsummary_print_buffer_members(struct buffer *B, const struct rmsummary *s, int count)
{
	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		const struct resource_info *info = &resources_info[i];
		double value = rmsummary_get_by_offset(s, info->offset);

		/* do not output undefined values */
		if (value < 0) {
			continue;
		}

		if (count++ > 0) {
			buffer_putliteral(B, ",");
		}
		buffer_putliteral(B, "\"");
		buffer_putstring(B, info->name);
		buffer_putliteral(B, "\":[");
		if (info->decimals == 0) {
			jx_print_integer((jx_int_t)value, B);
		} else {
			jx_print_double(round_to_decimals(value, info->decimals), B);
		}
		buffer_putliteral(B, ",\"");
		buffer_putstring(B, info->units);
		buffer_putliteral(B, "\"]");
	}

	return count;
}

void rmsummary_print_buffer(struct buffer *B, const struct rmsummary *s, int only_resources)
{
	if (!s)
		return;

	/* Resources alone are printed without building a JX object, which dominated the cost of the TaskVine
	 * transactions log. The output is the same as that of rmsummary_to_json(). */
	if (only_resources) {
		buffer_putliteral(B, "{");
		rmsummary_print_buffer_members(B, s, 0);
		buffer_putliteral(B, "}");
		return;
	}

	/* Print into the caller's buffer directly instead of formatting a copy of the whole string. */
	struct jx *jsum = rmsummary_to_json(s, only_resources);

	if (jsum) {
		jx_print_buffer(jsum, B);
		jx_delete(jsum);
	}
}

char *rmsummary_print_string(const struct rmsummary *s, int only_resources)
{
	if (!s)
		return NULL;

	struct jx *jsum = rmsummary_to_json(s, only_resources);

	if (jsum) {
		char *str = jx_print_string(jsum);
		jx_delete(jsum);

		return str;
	}

	return NULL;
}

static void rmsummary_fill(struct rmsummary *s, double default_value);

/* Summaries of the usual default values, made once for the process and copied by rmsummary_create(), which fills a
 * summary field by field otherwise: the manager creates several summaries for every task it submits and dispatches. */
static struct rmsummary rmsummary_minus_one;
static struct rmsummary rmsummary_zero;
static pthread_once_t rmsummary_defaults_once = PTHREAD_ONCE_INIT;

static void rmsummary_defaults_create(void)
{
	rmsummary_fill(&rmsummary_minus_one, -1);
	rmsummary_fill(&rmsummary_zero, 0);
}

/* Create summary filling all numeric fields with default_value, and
all string fields with NULL. Usual values are 0, or -1. */
struct rmsummary *rmsummary_create(double default_value)
{
	struct rmsummary *s = malloc(sizeof(struct rmsummary));
	if (!s) {
		return NULL;
	}
	if (default_value == -1 || default_value == 0) {
		pthread_once(&rmsummary_defaults_once, rmsummary_defaults_create);
		memcpy(s, default_value == 0 ? &rmsummary_zero : &rmsummary_minus_one, sizeof(*s));
	} else {
		rmsummary_fill(s, default_value);
	}
	return s;
}

static void rmsummary_fill(struct rmsummary *s, double default_value)
{
	memset(s, default_value, sizeof(struct rmsummary));

	s->command = NULL;
	s->category = NULL;
	s->exit_type = NULL;
	s->taskid = NULL;
	s->limits_exceeded = NULL;
	s->peak_times = NULL;

	s->last_error = 0;
	s->exit_status = 0;
	s->signal = 0;

	s->snapshot_name = NULL;
	s->snapshots_count = 0;
	s->snapshots = NULL;

	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		size_t offset = resources_info[i].offset;
		rmsummary_set_by_offset(s, offset, default_value);
	}
}

void rmsummary_delete(struct rmsummary *s)
{
	if (!s)
		return;

	if (s->command)
		free(s->command);

	if (s->category)
		free(s->category);

	if (s->exit_type)
		free(s->exit_type);

	if (s->taskid)
		free(s->taskid);

	rmsummary_delete(s->limits_exceeded);
	rmsummary_delete(s->peak_times);

	size_t i;
	for (i = 0; i < s->snapshots_count; i++) {
		rmsummary_delete(s->snapshots[i]);
	}

	free(s->snapshots);
	free(s);
}

void rmsummary_read_env_vars(struct rmsummary *s)
{
	char *value;
	value = getenv(RESOURCES_CORES);
	if (value) {
		rmsummary_set(s, "cores", atoi(value));
	}

	value = getenv(RESOURCES_MEMORY);
	if (value) {
		rmsummary_set(s, "memory", atoi(value));
	}

	value = getenv(RESOURCES_DISK);
	if (value) {
		rmsummary_set(s, "disk", atoi(value));
	}

	value = getenv(RESOURCES_GPUS);
	if (value) {
		rmsummary_set(s, "gpus", atoi(value));
	}

	value = getenv(RESOURCES_WALL_TIME);
	if (value) {
		rmsummary_set(s, "wall_time", atoi(value));
	}
}

#define RM_BIN_OP(dest, src, fn) \
	{ \
		if (!src || !dest) \
			return; \
		size_t i; \
		for (i = 0; i < rmsummary_num_resources(); i++) { \
			const struct resource_info *info = &resources_info[i]; \
			double dest_value = *((double *)((char *)dest + info->offset)); \
			double src_value = *((double *)((char *)src + info->offset)); \
			double result = fn(dest_value, src_value); \
			*(double *)((char *)dest + info->offset) = result; \
		} \
	}

/* Only operate on the fields that TaskVine actually uses;
 * cores, gpu, memory, disk. */
#define RM_BIN_OP_BASIC(dest, src, fn) \
	{ \
		if (!src || !dest) \
			return; \
		dest->cores = fn(dest->cores, src->cores); \
		dest->gpus = fn(dest->gpus, src->gpus); \
		dest->memory = fn(dest->memory, src->memory); \
		dest->disk = fn(dest->disk, src->disk); \
	}

/* Copy the value for all the fields in src > -1 to dest */
static inline double override_field(double d, double s)
{
	return (s > -1) ? s : d;
}

void rmsummary_merge_override(struct rmsummary *dest, const struct rmsummary *src)
{
	if (!src) {
		return;
	}

	RM_BIN_OP(dest, src, override_field);
}

void rmsummary_merge_override_basic(struct rmsummary *dest, const struct rmsummary *src)
{
	if (!src) {
		return;
	}

	RM_BIN_OP_BASIC(dest, src, override_field);
}

/* Copy the value for all the fields in src to dest when dest < 0 */
static inline double default_field(double d, double s)
{
	return (d > -1) ? d : s;
}

void rmsummary_merge_default(struct rmsummary *dest, const struct rmsummary *src)
{
	if (!src) {
		return;
	}

	RM_BIN_OP(dest, src, default_field);
}

struct rmsummary *rmsummary_copy(const struct rmsummary *src, int deep_copy)
{
	struct rmsummary *dest = rmsummary_create(-1);

	if (!src) {
		return dest;
	}

	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		// copy resource values
		size_t offset = resources_info[i].offset;
		rmsummary_set_by_offset(dest, offset, rmsummary_get_by_offset(src, offset));
	}

	if (deep_copy) {
		// copy other data only for deep copies
		if (src->command) {
			dest->command = xxstrdup(src->command);
		}

		if (src->category) {
			dest->category = xxstrdup(src->category);
		}

		if (src->taskid) {
			dest->taskid = xxstrdup(src->taskid);
		}

		if (src->limits_exceeded) {
			dest->limits_exceeded = rmsummary_copy(src->limits_exceeded, 0);
		}

		if (src->peak_times) {
			dest->peak_times = rmsummary_copy(src->peak_times, 0);
		}

		if (src->snapshot_name) {
			dest->snapshot_name = xxstrdup(src->snapshot_name);
		}

		if (src->snapshots_count > 0) {
			dest->snapshots = malloc(sizeof(struct rmsummary *) * src->snapshots_count);
			size_t i;
			for (i = 0; i < src->snapshots_count; i++) {
				dest->snapshots[i] = rmsummary_copy(src->snapshots[i], 1);
			}
		}
	}

	return dest;
}

static void merge_limits(struct rmsummary *dest, const struct rmsummary *src)
{
	if (!dest || !src) {
		return;
	}

	if (!dest->limits_exceeded && !src->limits_exceeded) {
		return;
	}

	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		size_t o = resources_info[i].offset;

		double src_value = rmsummary_get_by_offset(src, o);
		double dest_value = rmsummary_get_by_offset(dest, o);

		/* only update limit when new field value is larger than old, regardless of old
		 * limits. */
		if (src_value >= dest_value && src_value > -1) {
			if (!dest->limits_exceeded) {
				dest->limits_exceeded = rmsummary_create(-1);
			}

			double src_lim = src->limits_exceeded ? rmsummary_get_by_offset(src->limits_exceeded, o) : -1;
			double dest_lim = dest->limits_exceeded ? rmsummary_get_by_offset(dest->limits_exceeded, o) : -1;

			rmsummary_set_by_offset(dest->limits_exceeded, o, src_lim < 0 ? -1 : MAX(src_lim, dest_lim));
		}
	}
}

/* Select the max of the fields */
static inline double max_field(double d, double s)
{
	return (d > s) ? d : s;
}

/* Select the min of the fields, ignoring negative numbers */
static inline double min_field(double d, double s)
{
	if (d < 0 || s < 0) {
		return MAX(-1, MAX(s, d)); /* return at least -1. treat -1 as undefined.*/
	} else {
		return MIN(s, d);
	}
}

void rmsummary_merge_max(struct rmsummary *dest, const struct rmsummary *src)
{
	if (!dest || !src)
		return;

	RM_BIN_OP(dest, src, max_field);
	merge_limits(dest, src);

	if (src->peak_times) {
		if (!dest->peak_times) {
			dest->peak_times = rmsummary_create(-1);
		}
		rmsummary_merge_max(dest->peak_times, src->peak_times);
	}
}

void rmsummary_merge_max_w_time(struct rmsummary *dest, const struct rmsummary *src)
{
	if (!src || !dest)
		return;

	if (!dest->peak_times) {
		dest->peak_times = rmsummary_create(-1);
	}

	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		const struct resource_info *info = &resources_info[i];
		size_t offset = info->offset;

		double dest_value = rmsummary_get_by_offset(dest, offset);
		double src_value = rmsummary_get_by_offset(src, offset);

		/* if dest < src, then dest is updated with a new peak */
		if (dest_value < src_value) {
			rmsummary_set_by_offset(dest, offset, src_value);
			rmsummary_set_by_offset(dest->peak_times, offset, dest->wall_time);
		}
	}

	/* update peak times of start and end special cases */
	dest->peak_times->start = 0;
	dest->peak_times->end = dest->wall_time;
}

void rmsummary_merge_min(struct rmsummary *dest, const struct rmsummary *src)
{
	if (!dest || !src)
		return;

	RM_BIN_OP(dest, src, min_field);
	merge_limits(dest, src);

	if (src->peak_times) {
		if (!dest->peak_times) {
			dest->peak_times = rmsummary_create(-1);
		}
		rmsummary_merge_min(dest->peak_times, src->peak_times);
	}
}

/* Add summaries together, ignoring negative numbers */
static double plus(double d, double s)
{
	if (d < 0 || s < 0) {
		return MAX(0, MAX(s, d)); /* return at least 0 */
	} else {
		return s + d;
	}
}

void rmsummary_add(struct rmsummary *dest, const struct rmsummary *src)
{
	if (!dest || !src)
		return;

	RM_BIN_OP(dest, src, plus);
}

void rmsummary_debug_report(const struct rmsummary *s)
{
	if (!s)
		return;

	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		const struct resource_info *info = &resources_info[i];

		const char *r = info->name;
		const char *units = info->units;
		int decimals = info->decimals;
		double value = rmsummary_get_by_offset(s, info->offset);

		if (value > -1) {
			debug(D_DEBUG, "max resource %-18s   : %.*f %s\n", r, decimals, value, units);
		}
	}
}

struct rmsummary *rmsummary_get_snapshot(const struct rmsummary *s, size_t i)
{
	if (!s || i > s->snapshots_count) {
		return NULL;
	}

	return s->snapshots[i];
}

/* return 0 means above limit, 1 means limist ok */
int rmsummary_check_limits(struct rmsummary *measured, struct rmsummary *limits)
{
	measured->limits_exceeded = NULL;

	/* Consider errors as resources exhausted. Used for ENOSPC, ENFILE, etc. */
	if (measured->last_error) {
		return 0;
	}

	if (!limits) {
		return 1;
	}

	size_t i;
	for (i = 0; i < rmsummary_num_resources(); i++) {
		const struct resource_info *info = &resources_info[i];

		double l = rmsummary_get_by_offset(limits, info->offset);
		double m = rmsummary_get_by_offset(measured, info->offset);
		double f = 0;

		if (!strcmp(info->name, "cores")) {
			/* 'forgive' 1/4 of a core when doing measurements. As have been
			 * observed, tasks sometimes go above their cores declared usage
			 * for very short periods of time. */
			f = 0.25;
		}

		// if there is a limit, and the resource was measured, and the
		// measurement is larger than the limit, report the broken limit.
		if (l > -1 && m > 0 && l < (m - f)) {
			debug(D_DEBUG, "Resource limit for %s has been exceeded: %.*f > %.*f %s\n", info->name, info->decimals, m, info->decimals, l, info->units);

			if (!measured->limits_exceeded) {
				measured->limits_exceeded = rmsummary_create(-1);
			}
			rmsummary_set_by_offset(measured->limits_exceeded, info->offset, l);
		}
	}

	if (measured->limits_exceeded) {
		return 0;
	} else {
		return 1;
	}
}

const char **rmsummary_list_resources()
{
	if (!resources_names) {
		resources_names = calloc(rmsummary_num_resources() + 1, sizeof(char *));

		size_t i;
		for (i = 0; i < rmsummary_num_resources(); i++) {
			resources_names[i] = xxstrdup(resources_info[i].name);
		}
	}

	return (const char **)resources_names;
}

/* Do not use this more than once in a single printf statement */
/* the static output array means that multiple uses in a single printf */
/* will overwrite the previous calls leading to incorrect results */
const char *rmsummary_resource_to_str(const char *resource, double value, int include_units)
{
	/* One array per thread, because managers in different threads of one process format resources at once. */
	static __thread char output[256];

	const struct resource_info *info = info_of_resource(resource);
	if (!info) {
		notice(D_RMON, "There is not such a resource: %s", resource);
		return NULL;
	}
	const char *separator = include_units ? " " : "";
	const char *units = include_units ? info->units : "";

	/* A whole number, as resources usually are, is printed as an integer and zeros, which is what %.*f prints for it,
	 * without the floating-point conversion of printf. The manager formats four resources for every task it sends. */
	if (value == floor(value) && fabs(value) < 1e15 && !(value == 0 && signbit(value))) {
		int decimals = MIN(info->decimals, 15);
		size_t separator_length = strlen(separator);
		size_t units_length = strlen(units);
		if (separator_length + units_length < sizeof(output) - 40) {
			/* At most 16 digits and a sign, a point, 15 zeros, and the units, so it fits. */
			char digits[20];
			int count = 0;
			long long whole = (long long)value;
			unsigned long long magnitude = whole < 0 ? -(unsigned long long)whole : (unsigned long long)whole;
			do {
				digits[count++] = '0' + magnitude % 10;
				magnitude /= 10;
			} while (magnitude);
			char *p = output;
			if (whole < 0) {
				*p++ = '-';
			}
			while (count) {
				*p++ = digits[--count];
			}
			if (decimals) {
				*p++ = '.';
				memset(p, '0', decimals);
				p += decimals;
			}
			memcpy(p, separator, separator_length);
			p += separator_length;
			memcpy(p, units, units_length);
			p[units_length] = 0;
			return output;
		}
		string_nformat(output, sizeof(output), "%.*f%s%s", info->decimals, value, separator, units);
	} else {
		string_nformat(output, sizeof(output), "%.*f%s%s", info->decimals, value, separator, units);
	}

	return output;
}

/* vim: set noexpandtab tabstop=8: */
