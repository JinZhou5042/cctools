/* taskvine.i */
%module cvine

%include carrays.i
%array_functions(struct rmsummary *, rmsummayArray);

%begin %{
	#define SWIG_PYTHON_2_UNICODE
%}

%{
	#include "int_sizes.h"
	#include "taskvine.h"
	#include "vine_datavine_rpc.h"
%}

/* We compile with -D__LARGE64_FILES, thus off_t is at least 64bit.
long long int is guaranteed to be at least 64bit. */
%typemap(in) off_t = long long int;
%typemap(in) size_t = unsigned long long;

/* return a char*, enable automatic free */
%newobject vine_get_status;
%newobject vine_get_path_staging;
%newobject vine_get_path_cache;
%newobject vine_get_path_log;
%newobject vine_get_path_library_log;
%newobject vine_version_string;

/* These return pointers to lists defined in list.h. We aren't
 * wrapping methods in list.h and so ignore these. */
%ignore vine_cancel_all_tasks;
%ignore input_files;
%ignore output_files;
%ignore vine_datavine_rpc_get_u32;
%ignore vine_datavine_rpc_get_u64;
%ignore vine_datavine_rpc_put_u32;
%ignore vine_datavine_rpc_put_u64;

/* When we enounter buffer_length in the prototype of vine_task_get_output_buffer,
treat it as an output parameter to be filled in. */
%apply int *OUTPUT { int *buffer_length };


/* Convert a python buffer into a vine buffer */
/* Note!! This changes any C function with the signature f(struct vine_manager *m, const char *buffer, size_t size)
into a swig function f(data) */
%typemap(in, numinputs=1) (const char *buffer, size_t size) {
    if ($input == Py_None) {
        $1 = NULL;
        $2 = 0;
    } else {
        Py_buffer view;
        if (PyObject_GetBuffer($input, &view, PyBUF_SIMPLE) != 0) {
            PyErr_SetString(
                    PyExc_TypeError,
                    "in method '$symname', argument $argnum is not a simple buffer");
            SWIG_fail;
        }
        $1 = view.buf;
        $2 = view.len;
        PyBuffer_Release(&view);
    }
}
%typemap(doc) const char *data, int length "$1_name: a readable buffer (e.g. a bytes object)"

/* Convert a C array of binary data to Python bytes. */
%inline %{
	PyObject *vine_file_contents_as_bytes(struct vine_file *f) {
		return PyBytes_FromStringAndSize(vine_file_contents(f), vine_file_size(f));
	}

	PyObject *vine_datavine_rpc_server_metrics_as_dict(struct vine_datavine_rpc_server *server) {
		struct vine_datavine_directory_metrics metrics = {0};
		vine_datavine_rpc_server_get_metrics(server, &metrics);
		return Py_BuildValue(
				"{s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K}",
				"workers", metrics.workers,
				"replicas", metrics.replicas,
				"active_leases", metrics.active_leases,
				"source_selections", metrics.source_selections,
				"source_misses", metrics.source_misses,
				"releases", metrics.releases,
				"release_failures", metrics.release_failures,
				"idempotent_releases", metrics.idempotent_releases,
				"invalidations", metrics.invalidations,
				"restorations", metrics.restorations,
				"prunes", metrics.prunes,
				"stale_rejections", metrics.stale_rejections);
	}

	PyObject *vine_datavine_rpc_server_journal_metrics_as_dict(struct vine_datavine_rpc_server *server) {
		struct vine_datavine_journal_metrics metrics = {0};
		vine_datavine_rpc_server_get_journal_metrics(server, &metrics);
		return Py_BuildValue(
				"{s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K,s:K}",
				"commits", metrics.commits,
				"bytes", metrics.bytes,
				"syncs", metrics.syncs,
				"sync_nanoseconds", metrics.sync_nanoseconds,
				"maximum_group", metrics.maximum_group,
				"waits", metrics.waits,
				"replayed", metrics.replayed,
				"truncated_tails", metrics.truncated_tails,
				"durable_sequence", metrics.durable_sequence);
	}

	PyObject *vine_datavine_rpc_server_replicas_as_list(
			struct vine_datavine_rpc_server *server, char kind, int64_t data_id) {
		struct vine_datavine_replica_snapshot *records = 0;
		size_t count = 0;
		if (!vine_datavine_rpc_server_snapshot_replicas(
				server, kind, data_id, &records, &count)) {
			PyErr_SetString(PyExc_RuntimeError, "could not snapshot native replicas");
			return 0;
		}
		PyObject *result = PyList_New((Py_ssize_t)count);
		if (!result) {
			free(records);
			return 0;
		}
		for (size_t i = 0; i < count; i++) {
			struct vine_datavine_replica_record *record = &records[i].replica;
			char qualified[64];
			snprintf(qualified, sizeof(qualified), "%c:%lld", record->kind, (long long)record->data_id);
			const char *tier = record->tier == VINE_DATAVINE_WORKER_DRAM ? "worker-dram" : "worker-disk";
			const char *state = records[i].state == 2 ? "pruned" : records[i].state ? "available" : record->active_leases ? "retiring" : "invalid";
			PyObject *item = Py_BuildValue(
					"{s:s,s:s,s:K,s:i,s:s,s:s,s:L,s:s,s:I,s:s,s:K,s:s}",
					"data_id", qualified,
					"replica_id", record->replica_id,
					"generation", record->generation,
					"attempt", record->attempt,
					"tier", tier,
					"content_hash", record->content_hash,
					"size", (long long)record->size,
					"state", state,
					"load", record->active_leases,
					"worker_id", record->worker_id,
					"worker_epoch", record->worker_epoch,
					"source_endpoint", records[i].endpoint);
			if (!item) {
				free(records);
				Py_DECREF(result);
				return 0;
			}
			PyList_SET_ITEM(result, (Py_ssize_t)i, item);
		}
		free(records);
		return result;
	}
%}

%include "stdint.i"
%include "int_sizes.h"
%include "timestamp.h"

/* Return timestamp_t values as Python integers instead of leaking pointers. */
%typemap(in) timestamp_t {
  unsigned long long temp = PyLong_AsUnsignedLongLong($input);
  $1 = (timestamp_t)temp;
}
%typemap(out) timestamp_t {
  $result = PyLong_FromUnsignedLongLong((unsigned long long)$1);
}

%include "taskvine.h"
%include "vine_datavine_protocol.h"
%include "vine_datavine_rpc.h"
