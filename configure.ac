
AC_PREREQ([2.59])
AC_INIT([benvolio], [0.0], [robl@mcs.anl.gov])
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects silent-rules])
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/client/bv-client.cc])
AC_CONFIG_HEADERS([include/bv-config.h])
#

# checks for programs
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_INSTALL
LT_INIT

# thallium depends on c++-14
AX_CXX_COMPILE_STDCXX(14, noext, mandatory)


# checks for libraries
PKG_PROG_PKG_CONFIG
if test "x$PKG_CONFIG" == "x"; then
	AC_MSG_ERROR([Could not find pkg-config utility!])
fi

# checks for mochi services:
PKG_CHECK_MODULES([THALLIUM],[thallium], [],
		  AC_MSG_ERROR([Could not find working thallium installation]))
LIBS="$THALLIUM_LIBS $LIBS"
CPPFLAGS="$THALLIUM_CFLAGS $CPPFLAGS"
CFLAGS="$THALLIUM_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([SSG],[ssg],[],
        AC_MSG_ERROR([Could not find working ssg installation!]) )
LIBS="$SSG_LIBS $LIBS"
CPPFLAGS="$SSG_CFLAGS $CPPFLAGS"
CFLAGS="$SSG_CFLAGS $CFLAGS"

PKG_CHECK_MODULES([ABTIO],[abt-io],[],
        AC_MSG_ERROR([Could not find working abt-io installation!]) )
LIBS="$ABTIO_LIBS $LIBS"
CPPFLAGS="$ABTIO_CFLAGS $CPPFLAGS"
CFLAGS="$ABTIO_CFLAGS $CFLAGS"


use_drc=0
PKG_CHECK_MODULES([DRC],[cray-drc],
        [AC_DEFINE([USE_DRC], 1, [use cray dynamic rdma credentials]) ]
	use_drc=1,
	[] )
LIBS="$DRC_LIBS $LIBS"
CPPFLAGS="$DRC_CFLAGS $CPPFLAGS"
CFLAGS="$DRC_CFLAGS $CFLAGS"

# Bootstrapping: we can do it with either MPI or PMIx, but both require SSG to
# support it.  We need to check for both SSG's level of support as well as
# general support in the environment.

# check if SSG was compiled with PMIX support
ssg_with_pmix=no
ssg_with_mpi=no
AC_CHECK_LIB([ssg],
	     [ssg_group_create_pmix],
	     [ssg_with_pmix=yes
	      AC_DEFINE([SSG_HAS_PMIX], 1, [Does SSG library have PMIx support?])],
             [],
             [-lpmix]
)

# almost all implementations provide a 'libmpi' ... except vendors
AC_SEARCH_LIBS([MPI_Init],
        [mpi mpich mpi_ibm])

AC_CHECK_LIB([ssg],
	     [ssg_group_create_mpi],
	     [ssg_with_mpi=yes
	      AC_DEFINE([SSG_HAS_MPI], 1,[Does SSG library have MPI support])],
	     []
)

# benvolio will use MPI, PMIx or both to bootstrap, but only if we find the
# necessary supporting libraries
pmix_ok=no
mpi_ok=no
AC_CHECK_LIB([pmix], [PMIx_Init])
AC_MSG_CHECKING([If PMIx programs can be compiled])
AC_LINK_IFELSE(
	       [AC_LANG_PROGRAM([[#include<pmix.h>]], [[PMIx_Init(NULL, NULL,0);]] )],
	       [AC_DEFINE([HAVE_PMIX], [1], [Define to 1 if compiled with PMIx support])
	           pmix_ok=yes
	           AC_MSG_RESULT([yes])],
	       [AC_MSG_RESULT([no])]
	    )

AC_MSG_CHECKING([If MPI programs can be compiled])
AC_LINK_IFELSE(
    [AC_LANG_PROGRAM([[#include<mpi.h>]], [[MPI_Init(0,0);]])],
    [AC_MSG_RESULT([yes])
     mpi_ok=yes
     AC_DEFINE([HAVE_MPI], [1], [Able to use MPI])],
    [AC_MSG_RESULT([no])]
    )

use_pmix=no
if test x$pmix_ok = xyes -a x$ssg_with_pmix = xyes ; then
	use_pmix=yes
fi

use_mpi=no
if test x$mpi_ok = xyes -a x$ssg_with_mpi = xyes ; then
	use_mpi=yes;
fi

AM_CONDITIONAL([USE_PMIX], [ test x$use_pmix = xyes ])
AM_CONDITIONAL([USE_MPI], [ test x$use_mpi = xyes ])

# we need *some* way to bootstrap: we can support one or the other or both but not none
AC_MSG_CHECKING([If at least one bootstrap method available])
if test x$use_pmix = xyes -o x$use_mpi = xyes ; then
	AC_MSG_RESULT(yes)
else
	AC_MSG_ERROR([Unable to detect MPI or PMIx ])
fi

AC_CHECK_LIB([lustreapi], [llapi_file_get_stripe])
# checks for header files
AC_CHECK_HEADERS([lustre/lustreapi.h])

# Checks for typedefs, structures, and compiler characteristics.

# checks for library functions

# statx: a lightweight way to ask for only a subset of information
AC_CHECK_FUNCS(statx)

AC_CONFIG_FILES([Makefile maint/bv-client.pc maint/bv-provider.pc])
AC_OUTPUT
