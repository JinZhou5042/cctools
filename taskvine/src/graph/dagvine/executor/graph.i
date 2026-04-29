/* SWIG interface for local executor graph API bindings */
%module graph_capi

%{
#include "int_sizes.h"
#include "graph.h"
%}

%include "stdint.i"
%include "int_sizes.h"

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../../bindings/python3/taskvine.i"

%include "graph.h"
