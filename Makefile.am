AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

AM_CPPFLAGS = -I$(top_srcdir)/include

bin_PROGRAMS = src/provider/mochio-server

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

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/mochio-provider.pc \
		 maint/mochio-client.pc

SUFFIXES=
LDADD=
check_PROGRAMS=
EXTRA_DIST=
CLEANFILES=
TESTS=

include $(top_srcdir)/tests/Makefile.subdir

# build check targets but not invoke
tests: all $(check_PROGRAMS)

