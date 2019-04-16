
AC_PREREQ([2.59])
AC_INIT([romio-sds], [0.0], [robl@mcs.anl.gov])
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects silent-rules])
AM_SILENT_RULES([yes])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/romio-client.c])
AC_CONFIG_HEADERS([src/romio-sds-config.h])


# checks for programs
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG-CXX
AC_PROG_INSTALL

# thallium depends on c++-14
AX_CXX_COMPILE_STDCXX(14, noext, mandatory)


# checks for libraries

# checks for header files

# Checks for typedefs, structures, and compiler characteristics.

# checks for library functions

AC_CONFIG_FILES[[Makefile])
AC_OUTPUT
