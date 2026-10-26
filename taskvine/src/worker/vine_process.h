/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef VINE_PROCESS_H
#define VINE_PROCESS_H

#include "vine_manager.h"
#include "vine_task.h"

#include "timestamp.h"
#include "link.h"
#include "link_writer.h"
#include "path_disk_size_info.h"

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>

/* The basic type of the process, controlling how it is stopped and started. */

typedef enum {
	VINE_PROCESS_TYPE_STANDARD,   // standard task with command line
	VINE_PROCESS_TYPE_LIBRARY,   // task providing serverless library
	VINE_PROCESS_TYPE_FUNCTION,  // task invoking serverless library
	VINE_PROCESS_TYPE_MINI_TASK, // internal task used to create file
	VINE_PROCESS_TYPE_TRANSFER,  // internal task used to transfer file
} vine_process_type_t;

/*
vine_process is a running instance of a vine_task.
This object is private to the vine_worker.
*/

struct vine_process {
	vine_process_type_t type; /* The basic type of the process, */
	pid_t pid;                /* If running, the Unix process ID. */
	vine_result_t result;     /* If complete, the TaskVine result. */
	int exit_code;            /* If successful, the Unix exit code. */

	struct rusage rusage;        /* If complete, the resources consumed. */
	timestamp_t execution_start; /* Start time in microseconds. */
	timestamp_t execution_end;   /* Stop time in microseconds. */

	char *sandbox;           /* The private sandbox directory to run in. */
	char *tmpdir; 	         /* A temp dir inside the private sandbox. */
	char *output_file_name;	 /* The intended standard output location. */
	
	/* If a normal task, the details of the task to execute. */
	struct vine_task *task;

	/* If a function-call task, this is the specific library process to invoke. */
	struct vine_process *library_process;
	struct list_cursor *input_cursor; /* first input not known to be in the cache, kept by vine_sandbox_ensure */
	
	/* If this is a library process, the links to communicate with the library. */
	struct link *library_read_link;
	struct link *library_write_link;

	/* If this is a library process, everything sent to it, written as its pipe takes it, so the worker never waits
	 * for a library that does not read. */
	struct link_writer *library_writer;

	/* If this is a library process, whether its writer holds data and it is in the list that the main loop polls. */
	int library_sending;

	/* If this is a library process, the number of functions it is currently running. */
	int functions_running;
	
	/* If this is a library process, whether the library is ready to execute functions. */
	int library_ready;

	/* If this is a library process, whether the library stops a function call on request. */
	int library_can_cancel;

	/* If this is a library process that is starting, its startup message as it arrives: allocated once the line with
	 * its length arrived, with that length and the bytes received so far. The worker reads it without waiting. */
	char *library_startup;
	int library_startup_length;
	int library_startup_received;

	/* expected disk usage by the process. If no cache is used, it is the same as in task. */
	int64_t disk;

	/* disk size and number of files found in the process sandbox. */
	int64_t sandbox_size;
	int64_t sandbox_file_count;
	
	/* size of the process' stdout file */
	int64_t output_length;

	/* state between complete disk measurements. */
	struct path_disk_size_info *disk_measurement_state;

	/* While the outputs of a completed process move into the cache (vine_sandbox_stageout), the next output to move,
	 * its mode and modification time once observed (mode zero before), and, if it is a directory, the state of its
	 * measurement. */
	struct list_cursor *output_cursor;
	struct path_disk_size_info *output_measurement_state;
	int output_mode;
	time_t output_mtime;
};

struct vine_process * vine_process_create( struct vine_task *task, vine_process_type_t type );
int   vine_process_execute( struct vine_process *p );
int   vine_process_cancel_function( struct vine_process *p );
int   vine_process_is_complete( struct vine_process *p );
int   vine_process_wait( struct vine_process *p );
void  vine_process_kill( struct vine_process *p );
void  vine_process_delete( struct vine_process *p );

int   vine_process_execute_and_wait( struct vine_process *p );

int   vine_process_library_get_result( struct vine_process *p, uint64_t *done_task_id, int *exit_code );

/* The number of libraries whose writers hold data. */
int   vine_process_libraries_sending( void );

/* Fill one entry per library whose writer holds data, waiting for room in its pipe. */
void  vine_process_libraries_poll( struct link_info *waits );

/* Write to the libraries whose writers hold data, without waiting, until a deadline of timestamp_monotonic(), and kill
 * a library whose pipe failed, which then ends as any library that exits. */
void  vine_process_libraries_send( timestamp_t deadline );

void  vine_process_compute_disk_needed( struct vine_process *p );

/* Measure the disk used by the sandbox of a process, resuming an earlier measurement, until the budget expires.
 * Update sandbox_size and sandbox_file_count, and return 1 once the measurement completed, or 0 if it continues in a
 * later call. */
int   vine_process_measure_disk(struct vine_process *p, const struct budget *b);

#endif
