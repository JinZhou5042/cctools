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

%include "vine_graph_executor.h"
