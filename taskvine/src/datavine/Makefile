include ../../../config.mk
include ../../../rules.mk

# DataVine consumes TaskVine's public C API and is built as a separate archive.
LOCAL_CCFLAGS += -I ../manager

SOURCES = \
	vine_datavine_data_controller.c \
	vine_datavine_ir.c \
	vine_datavine_journal.c \
	vine_datavine_object_store.c \
	vine_datavine_parametric.c \
	vine_datavine_replica_table.c \
	vine_datavine_scheduler.c \
	vine_datavine_workflow.c \
	vine_datavine_workflow_store.c \
	vine_datavine_workflow_runtime.c \
	vine_datavine_rpc.c

PUBLIC_HEADERS = \
	vine_datavine_protocol.h \
	vine_datavine_workflow.h

OBJECTS = $(SOURCES:%.c=%.o)
LIBRARIES = libdatavine.a
TARGETS = $(LIBRARIES)

all: $(TARGETS)

libdatavine.a: $(OBJECTS)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/lib
	cp $(LIBRARIES) $(CCTOOLS_INSTALL_DIR)/lib/
	mkdir -p $(CCTOOLS_INSTALL_DIR)/include/cctools
	rm -f $(CCTOOLS_INSTALL_DIR)/include/cctools/vine_datavine_*.h
	cp $(PUBLIC_HEADERS) $(CCTOOLS_INSTALL_DIR)/include/cctools/

clean:
	rm -rf vine_datavine_*.o $(LIBRARIES) ../manager/vine_datavine_*.o

test: all

lint:
	@if ( ! clang-format -Werror --dry-run --style='file:../../../.clang-format' $(SOURCES) *.h); then \
		echo "DataVine formatting check failed"; \
		exit 1; \
	fi

format:
	clang-format -i $(SOURCES) *.h

.PHONY: all clean install test lint format
