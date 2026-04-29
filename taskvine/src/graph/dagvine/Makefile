include ../../../../config.mk

MODULE_DIR := $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine
SUBDIRS    := vine_graph blueprint_graph

.PHONY: all install clean lint format test

all clean lint format test:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@ || exit $$?; \
	done

install: all
	mkdir -p $(MODULE_DIR)
	cp dagvine.py $(MODULE_DIR)
	cp __init__.py $(MODULE_DIR)
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir install || exit $$?; \
	done
