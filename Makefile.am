AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

AM_CPPFLAGS = -I$(top_srcdir)/include

bin_PROGRAMS = src/provider/server


src_provider_server_SOURCES = src/provider/server.c
src_provider_server_LDFLAGS = -Llib
src_provider_server_LDADD = -lromio-provider ${LIBS}


lib_LTLIBRARIES = lib/libromio-client.la \
    lib/libromio-provider.la

lib_libromio_client_la_SOURCES = src/client/romio-client.cc

lib_libromio_provider_la_SOURCES = src/provider/romio-provider.cc

include_HEADERS = include/romio-svc.h \
	include/romio-svc-provider.h

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/romio-svc.pc
