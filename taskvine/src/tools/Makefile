include ../../../config.mk
include ../../../rules.mk

DATAVINE_LINKAGE=${CCTOOLS_HOME}/taskvine/src/datavine/libdatavine.a ${CCTOOLS_HOME}/taskvine/src/manager/libtaskvine.a ${CCTOOLS_HOME}/dttools/src/libdttools.a
LOCAL_LINKAGE+=$(DATAVINE_LINKAGE)
LOCAL_CCFLAGS+=-I ${CCTOOLS_HOME}/taskvine/src/manager -I ${CCTOOLS_HOME}/taskvine/src/datavine

PROGRAMS = vine_status vine_benchmark datavine_workflow datavine_executor
SCRIPTS = vine_plot_performance vine_plot_taskgraph vine_plot_workers vine_plot_txn_log vine_submit_workers vine_plot_compose vine_plot_run datavine_python_executor
TEST_PROGRAMS = vine_test datavine_parametric_test datavine_replica_benchmark datavine_controller_benchmark
TARGETS = $(PROGRAMS) $(TEST_PROGRAMS)

# These are useful development tools but not meant for end user consumption.
DEVEL_SCRIPTS = vine_profile_dispatch vine_transfer_plot_animate

all: $(TARGETS)

$(PROGRAMS) $(TEST_PROGRAMS): $(EXTERNALS) $(DATAVINE_LINKAGE)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/bin
	cp $(PROGRAMS) $(SCRIPTS) $(CCTOOLS_INSTALL_DIR)/bin/
	ln -sf datavine_workflow $(CCTOOLS_INSTALL_DIR)/bin/datavine

clean:
	rm -rf $(PROGRAMS) $(TEST_PROGRAMS) *.o

test: all

lint:

format:

.PHONY: all clean install test lint format
