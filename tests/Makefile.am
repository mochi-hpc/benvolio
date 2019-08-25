#
# Copyright (C) 2019, Northwestern University and Argonne National Laboratory
# See COPYRIGHT notice in top-level directory.
#
# $Id$
#
# @configure_input@

SUFFIXES = .o .c .f
AM_DEFAULT_SOURCE_EXT = .c

AM_CPPFLAGS = -I$(top_srcdir)/include
LDADD  = $(top_srcdir)/lib/libmochio-client.la

check_PROGRAMS = simple \
	             strided \
                 null \
                 overlap

EXTRA_DIST = # wrap_runs.sh parallel_run.sh

OUT_FILES = $(check_PROGRAMS:%=%.bin)
SERVER_FILES = $(check_PROGRAMS:%=%.svc)

CLEANFILES = $(OUT_FILES) core core.* \
             $(SERVER_FILES)

TESTPROGRAMS = $(check_PROGRAMS)

TESTS_ENVIRONMENT  = export SED="$(SED)";
TESTS_ENVIRONMENT += export top_srcdir="$(top_srcdir)";
TESTS_ENVIRONMENT += export srcdir="$(srcdir)";
TESTS_ENVIRONMENT += export TESTOUTDIR="$(TESTOUTDIR)";
TESTS_ENVIRONMENT += export TESTPROGRAMS="$(TESTPROGRAMS)";
TESTS_ENVIRONMENT += export check_PROGRAMS="$(check_PROGRAMS)";

TEST_EXTENSIONS = .sh
LOG_COMPILER = $(srcdir)/wrap_runs.sh
SH_LOG_COMPILER =

TESTS = $(TESTPROGRAMS)

# build check targets but not invoke
tests-local: all $(check_PROGRAMS)

.PHONY: ptest ptests ptest2 ptest4 ptest6 ptest8 ptest10

