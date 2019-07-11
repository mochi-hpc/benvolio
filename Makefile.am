AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

AM_CPPFLAGS = -I$(top_srcdir)/include

bin_PROGRAMS = src/provider/mochio-server

check_PROGRAMS = tests/simple

src_provider_mochio_server_SOURCES = src/provider/server.c
src_provider_mochio_server_LDADD = lib/libmochio-provider.la


lib_LTLIBRARIES = lib/libmochio-client.la \
    lib/libmochio-provider.la

lib_libmochio_client_la_SOURCES = src/client/mochio-client.cc \
				  src/client/calc-request.cc

lib_libmochio_provider_la_SOURCES = src/provider/mochio-provider.cc \
                                    src/provider/lustre-utils.c

include_HEADERS = include/mochio.h \
	include/mochio-provider.h

noinst_HEADERS = include/io_stats.h \
		 include/access.h \
		 include/common.h \
		 include/calc-request.h \
		 include/file_stats.h \
		 include/lustre-utils.h

TESTS = tests/simple

tests_simple_SOURCES = tests/simple.c
# hack/trick to force linking with C++
# but all symbols are extern C so it should not be needed
#nodist_EXTRA_tests_simple_SOURCES = dummy.cc
tests_simple_LDADD = lib/libmochio-client.la


pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/mochio-provider.pc \
		 maint/mochio-client.pc
