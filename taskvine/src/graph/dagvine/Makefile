include ../../../../config.mk

MODULE_DIR := $(CCTOOLS_PYTHON3_PATH)/ndcctools/taskvine/dagvine
SUBDIRS    := executor
PY_SOURCES := __init__.py adaptor.py adaptor_test.py manager.py proxy_functions.py proxy_library.py workflow.py

.PHONY: all install clean lint format test

all format test:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@ || exit $$?; \
	done

install: all
	mkdir -p $(MODULE_DIR)
	cp $(PY_SOURCES) $(MODULE_DIR)
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir install || exit $$?; \
	done

clean:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir clean || exit $$?; \
	done
	rm -rf __pycache__

lint:
	@for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir lint || exit $$?; \
	done
	flake8 --ignore=$(CCTOOLS_FLAKE8_IGNORE_ERRORS) \
		--exclude=$(CCTOOLS_FLAKE8_IGNORE_FILES) \
		$(PY_SOURCES)
