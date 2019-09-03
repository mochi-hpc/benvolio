AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

AM_CPPFLAGS = -I$(top_srcdir)/include

bin_PROGRAMS = src/provider/bv-server \
	       src/client/bv-shutdown

src_provider_bv_server_SOURCES = src/provider/server.c
src_provider_bv_server_LDADD = lib/libbv-provider.la

src_client_bv_shutdown_SOURCES = src/client/bv-shutdown.c
src_client_bv_shutdown_LDADD = lib/libbv-client.la

lib_LTLIBRARIES = lib/libbv-client.la \
    lib/libbv-provider.la

lib_libbv_client_la_SOURCES = src/client/bv-client.cc \
				  src/client/calc-request.cc

lib_libbv_provider_la_SOURCES = src/provider/bv-provider.cc \
                                    src/provider/lustre-utils.c

include_HEADERS = include/bv.h \
	include/bv-provider.h

noinst_HEADERS = include/io_stats.h \
		 include/access.h \
		 include/common.h \
		 include/calc-request.h \
		 include/file_stats.h \
		 include/lustre-utils.h

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/bv-provider.pc \
		 maint/bv-client.pc

SUFFIXES=
LDADD=
check_PROGRAMS=
EXTRA_DIST=
CLEANFILES=
TESTS=

include $(top_srcdir)/tests/Makefile.subdir

# build check targets but not invoke
tests: all $(check_PROGRAMS)

