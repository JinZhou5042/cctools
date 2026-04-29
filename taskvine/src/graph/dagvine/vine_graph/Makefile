include ../../../../../config.mk
include ../../../../../rules.mk

PROJECT_NAME := dagvine
SOURCE_DIR   := $(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME)/vine_graph
BUILD_DIR    := ../build
MODULE_DIR   := $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/$(PROJECT_NAME)/vine_graph
MODULE_NAME  := vine_graph_capi

SOURCES   := vine_node.c vine_graph.c
OBJECTS   := $(SOURCES:%.c=%.o)
SWIG_WRAP := $(BUILD_DIR)/vine_graph_wrap.c
WRAP_OBJ  := $(BUILD_DIR)/vine_graph_wrap.o
PYMODULE  := $(BUILD_DIR)/_$(MODULE_NAME).$(CCTOOLS_DYNAMIC_SUFFIX)

LOCAL_LINKAGE += $(CCTOOLS_HOME)/taskvine/src/manager/libtaskvine.a $(CCTOOLS_HOME)/dttools/src/libdttools.a
LOCAL_CCFLAGS += -I $(CCTOOLS_HOME)/taskvine/src/manager -I $(SOURCE_DIR)

.PHONY: all install clean lint format test

all: $(PYMODULE)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SWIG_WRAP): vine_graph.i vine_graph.h | $(BUILD_DIR)
	$(CCTOOLS_SWIG) -python -threads -relativeimport \
		-I$(CCTOOLS_HOME)/taskvine/src/manager \
		-I$(CCTOOLS_HOME)/dttools/src \
		-I$(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME) \
		-I$(CCTOOLS_HOME)/taskvine/src/graph/$(PROJECT_NAME)/vine_graph \
		-outdir $(BUILD_DIR) -o $@ $<

$(WRAP_OBJ): $(SWIG_WRAP)
	$(CCTOOLS_CC) -o $@ -c $(CCTOOLS_INTERNAL_CCFLAGS) $(LOCAL_CCFLAGS) $(CCTOOLS_PYTHON3_CCFLAGS) -w -fPIC -DNDEBUG $<

$(PYMODULE): $(WRAP_OBJ) $(OBJECTS)
	$(CCTOOLS_LD) -o $@ $(CCTOOLS_DYNAMIC_FLAG) $(CCTOOLS_INTERNAL_LDFLAGS) $(LOCAL_LDFLAGS) $^ $(LOCAL_LINKAGE) $(CCTOOLS_PYTHON3_LDFLAGS) $(CCTOOLS_EXTERNAL_LINKAGE)

install: all
	mkdir -p $(CCTOOLS_INSTALL_DIR)/graph/$(PROJECT_NAME)/include
	cp $(CCTOOLS_HOME)/taskvine/src/manager/taskvine.h $(CCTOOLS_INSTALL_DIR)/graph/$(PROJECT_NAME)/include/
	mkdir -p $(MODULE_DIR)
	cp $(PYMODULE) $(MODULE_DIR)
	cp $(BUILD_DIR)/$(MODULE_NAME).py $(MODULE_DIR)
	cp $(SOURCE_DIR)/__init__.py $(MODULE_DIR)
	cp $(SOURCE_DIR)/vine_graph_client.py $(MODULE_DIR)

clean:
	rm -f $(OBJECTS)
	rm -rf $(BUILD_DIR)

lint:
	clang-format -Werror --dry-run --style='file:$(CCTOOLS_HOME)/.clang-format' $(SOURCE_DIR)/*.[ch]

format:
	clang-format -i $(SOURCE_DIR)/*.[ch]

test:
