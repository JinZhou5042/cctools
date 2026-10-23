/* SWIG interface for the vine_graph executor. The graph module is internal and is not wrapped. */
%module vine_graph_capi

%{
#include "int_sizes.h"
#include "vine_graph.h"
#include "vine_graph_executor.h"
%}

%include "stdint.i"
%include "int_sizes.h"

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../bindings/python3/taskvine.i"

/* The module is built with -threads, which makes every wrapper release the GIL around its call. A thread that releases
 * the GIL must wait for it again afterwards, which takes up to the interpreter's switch interval (5 ms) while another
 * thread runs Python, and the frontend's background thread makes several calls of a few microseconds for every node.
 * So only the calls that can wait release the GIL: vine_graph_executor_wait runs the Manager and polls its links,
 * vine_graph_executor_delete cancels tasks and joins receives, vine_graph_executor_declare_file computes the checksum
 * of a file the frontend provides, and vine_graph_executor_tune creates the vault's directory in a shared file system
 * for shared-directory. Every other call works on memory and queues messages to Workers without
 * waiting. The most it waits for is the removal of a vault entry when it prunes or undeclares a file: an unlink, and
 * the end of a connected receive of that file after its socket is shut down, whose thread never takes the GIL. No call
 * calls back into Python. */
%nothread;
%thread vine_graph_executor_wait;
%thread vine_graph_executor_delete;
%thread vine_graph_executor_declare_file;
%thread vine_graph_executor_tune;

/* vine_graph_executor_add_inputs() takes its mounts from one Python sequence of (file id, task path) pairs. The task
 * paths point into the strings of the sequence, which the call holds until it returns. */
%typemap(in) (int count, const uint64_t *file_ids, const char *const *task_paths) (PyObject *mounts = NULL)
{
	mounts = PySequence_Fast($input, "mounts must be a sequence of (file id, task path) pairs");
	if (!mounts) {
		SWIG_fail;
	}
	Py_ssize_t count = PySequence_Fast_GET_SIZE(mounts);
	if (count > INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "too many mounts");
		SWIG_fail;
	}
	$1 = (int)count;
	$2 = malloc(sizeof(uint64_t) * (count ? count : 1));
	$3 = malloc(sizeof(char *) * (count ? count : 1));
	if (!$2 || !$3) {
		PyErr_NoMemory();
		SWIG_fail;
	}
	PyObject **items = PySequence_Fast_ITEMS(mounts);
	for (Py_ssize_t i = 0; i < count; i++) {
		PyObject *pair = items[i];
		if (!PyTuple_Check(pair) || PyTuple_GET_SIZE(pair) != 2) {
			PyErr_SetString(PyExc_TypeError, "a mount is a (file id, task path) pair");
			SWIG_fail;
		}
		((uint64_t *)$2)[i] = PyLong_AsUnsignedLongLong(PyTuple_GET_ITEM(pair, 0));
		if (PyErr_Occurred()) {
			SWIG_fail;
		}
		((const char **)$3)[i] = PyUnicode_AsUTF8(PyTuple_GET_ITEM(pair, 1));
		if (!$3[i]) {
			SWIG_fail;
		}
	}
}
%typemap(freearg) (int count, const uint64_t *file_ids, const char *const *task_paths)
{
	free($2);
	free((void *)$3);
	Py_XDECREF(mounts$argnum);
}

/* vine_graph_executor_get_local_data() returns the bytes of a fetched output that the Manager holds in memory as a
 * Python bytes object, or None when it holds none. The bytes are copied before the call returns. */
%typemap(in, numinputs=0) (const char **data, int64_t *length) (const char *data_value = NULL, int64_t length_value = 0)
{
	$1 = ($1_ltype)&data_value;
	$2 = &length_value;
}
%typemap(argout) (const char **data, int64_t *length)
{
	Py_DECREF($result);
	if (*$1) {
		$result = PyBytes_FromStringAndSize(*$1, (Py_ssize_t)*$2);
	} else {
		Py_INCREF(Py_None);
		$result = Py_None;
	}
}

/* SWIG's stdint.i takes uint64_t for unsigned long long, which the call would otherwise be cast to. */
%feature("action") vine_graph_executor_add_inputs {
	result = vine_graph_executor_add_inputs(arg1, arg2, arg3, (const uint64_t *)arg4, (const char *const *)arg5);
}

%include "vine_graph_executor.h"
