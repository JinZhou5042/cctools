/* SWIG interface for local executor graph API bindings */
%module vine_graph_capi

%{
#include "int_sizes.h"
#include "vine_graph.h"
#include "vine_graph_executor.h"
%}

%include "stdint.i"
%include "int_sizes.h"

/* uint64_t[] + length: not mapped yet; expose via VineGraphExecutor when needed. */

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../bindings/python3/taskvine.i"

%include "vine_graph.h"
%include "vine_graph_executor.h"
