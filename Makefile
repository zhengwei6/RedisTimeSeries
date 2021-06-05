ROOT=.
MK.pyver:=3
REDIS_PYTHON_DIR = $(REDIS_DIR)/ts_python/

ifeq ($(wildcard $(ROOT)/deps/readies/mk),)
$(error Submodules not present. Please run 'git submodule update --init --recursive')
endif
include $(ROOT)/deps/readies/mk/main

MK_CUSTOM_CLEAN=1
BINDIR=$(BINROOT)

include $(MK)/defs
include $(MK)/rules

.PHONY: all setup fetch build clean test pack help

all: fetch build

help:
	@$(MAKE) -C src help

setup:
	@echo Setting up system...
	@./deps/readies/bin/getpy3
	@./system-setup.py
	@./pip-setup.py

fetch:
	-@git submodule update --init --recursive

build:
	@$(MAKE) -C src all -j $(NCPUS)

clean:
	@$(MAKE) -C src clean

lint:
	@$(MAKE) -C src lint

format:
	@$(MAKE) -C src format

test:
	@$(MAKE) -C src tests

unittests:
	@$(MAKE) -C src unittests

pack:
	@$(MAKE) -C src package

run:
	@$(MAKE) -C src run

benchmark:
	@$(MAKE) -C src benchmark

# deploy:
#	@make -C src deploy

env:
	@if [ -z $(REDIS_DIR) ]; then\
		echo "usage: make env REDIS_DIR="<redis path>"";\
		exit 1;\
	fi
	@mkdir -p $(REDIS_PYTHON_DIR);
	@cp ./ts_python/* $(REDIS_PYTHON_DIR);
	@cp ./bin/redistimeseries.so $(REDIS_DIR);
