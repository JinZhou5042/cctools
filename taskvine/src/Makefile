include ../../config.mk
include ../../rules.mk

TARGETS=manager datavine worker tools bindings examples

all: $(TARGETS)

datavine: manager
worker: manager
bindings: manager
tools: manager datavine
examples: manager worker tools bindings

$(TARGETS): %:
	$(MAKE) -C $@ $(MAKECMDGOALS)

install:
	set -e; for d in $(TARGETS); do $(MAKE) -C $$d install; done

clean:
	set -e; for d in $(TARGETS); do $(MAKE) -C $$d clean; done

lint: $(TARGETS)

format: $(TARGETS)

.PHONY: all clean install test lint format $(TARGETS)
