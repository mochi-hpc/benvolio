AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS="-Im4"

COMMON_INCLUDES = -I$(top_srcdir)/include
AM_CPPFLAGS = $(COMMON_INCLUDES)

bin_PROGRAMS = src/client/bv-shutdown \
	       src/client/bv-ping

# automake will use *either* AM_CPPFLAGS or mumble_CPPFLAGS.  Since we set
# per-object CPPFLAGS we need to repeat AM_CPPFLAGS
#src_provider_bv_server_CPPFLAGS= -I$(top_srcdir)/include

# we can get away with this because bv-server is a single source + provider
# convienence library.  If bv-server starts picking up more source files we
# might have to make a library
# (see https://www.gnu.org/software/automake/manual/html_node/Per_002dObject-Flags.html)
if USE_PMIX
    bin_PROGRAMS+=src/provider/bv-server.pmix
    src_provider_bv_server_pmix_SOURCES = src/provider/server.c \
				 src/provider/bv-provider.h
    src_provider_bv_server_pmix_CPPFLAGS = $(COMMON_INCLUDES) "-DUSE_PMIX"
    src_provider_bv_server_pmix_LDADD = lib/libbv-provider.la
endif

if USE_MPI
    bin_PROGRAMS+=src/provider/bv-server.mpi
    src_provider_bv_server_mpi_SOURCES = src/provider/server.c \
				 src/provider/bv-provider.h
    src_provider_bv_server_mpi_CPPFLAGS = $(COMMON_INCLUDES) "-DUSE_MPI"
    src_provider_bv_server_mpi_LDADD = lib/libbv-provider.la
endif


src_client_bv_shutdown_SOURCES = src/client/bv-shutdown.c
src_client_bv_shutdown_LDADD = lib/libbv-client.la

src_client_bv_ping_SOURCES = src/client/bv-ping.c
src_client_bv_ping_LDADD = lib/libbv-client.la

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
XFAIL_TESTS=

include $(top_srcdir)/tests/Makefile.subdir

# build check targets but not invoke
tests: all $(check_PROGRAMS)

