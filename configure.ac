
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


# check that SSG was compiled with PMIX support
AC_CHECK_LIB([ssg],
	     [ssg_group_create_pmix],
	     ,
             [AC_MSG_ERROR([SSG must be built with PMIx support!])],
             [-lpmix]
)

# check for PMIx: required to boostrap server provider processes

AC_CHECK_LIB([pmix], [PMIx_Init])
AC_MSG_CHECKING([If PMIx programs can be compiled])
AC_LINK_IFELSE(
	       [AC_LANG_PROGRAM([[#include<pmix.h>]], [[PMIx_Init(NULL, NULL,0);]] )],
	       [AC_DEFINE([HAVE_PMIX], [1], [Define to 1 if compiled with PMIx support])
	           AC_MSG_RESULT([yes])],
	       [AC_MSG_RESULT([no])]
	       [AC_MSG_ERROR([PMIx required to launch remote providers]) ])

AC_CHECK_LIB([lustreapi], [llapi_file_get_stripe])
# checks for header files
AC_CHECK_HEADERS([lustre/lustreapi.h])

# Checks for typedefs, structures, and compiler characteristics.

# checks for library functions

AC_CONFIG_FILES([Makefile maint/bv-client.pc maint/bv-provider.pc])
AC_OUTPUT
