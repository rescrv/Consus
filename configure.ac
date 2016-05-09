# Copyright (c) 2015, Robert Escriva
# All rights reserved.

AC_PREREQ([2.61])
AC_INIT([consus], [0.0.dev], [robert@rescrv.net])
m4_define([serial_tests], [
    m4_esyscmd([case `automake --version | head -n 1` in
                *1.11*);;
                *) echo serial-tests;;
                esac])
])
AM_INIT_AUTOMAKE(foreign serial_tests subdir-objects dist-bzip2)
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
AM_PATH_PYTHON([2.6])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([include/consus.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AC_LANG(C++)

ANAL_WARNINGS

AC_CHECK_HEADER([popt.h],,[AC_MSG_ERROR([
-------------------------------------------------
Consus relies upon the popt library.
Please install popt to continue.
-------------------------------------------------])])
AC_ARG_VAR(POPT_LIBS, [linker flags for popt])
AS_IF([test "x$POPT_LIBS" = x], [POPT_LIBS="-lpopt"])

AC_CHECK_HEADER([glog/logging.h],,[AC_MSG_ERROR([
-------------------------------------------------
Consus relies upon the glog library.
Please install glog to continue.
-------------------------------------------------])])
AC_ARG_VAR(GLOG_LIBS, [linker flags for glog])
AS_IF([test "x$GLOG_LIBS" = x], [GLOG_LIBS="-lglog"])

PKG_CHECK_MODULES([PO6], [libpo6 >= 0.8])
PKG_CHECK_MODULES([E], [libe >= 0.11])
PKG_CHECK_MODULES([TREADSTONE], [libtreadstone >= 0.0])
PKG_CHECK_MODULES([BUSYBEE], [busybee >= 0.7])
PKG_CHECK_MODULES([REPLICANT], [replicant >= 0.8])

AC_PYTHON_DEVEL([>= '2.6'])
AS_CASE([$PYTHON_VERSION], [3*], [pythonsym=PyInit_], [2*], [pythonsym=init], [])
AC_SUBST([PYTHON_SYMBOL], [${pythonsym}])

AC_ARG_ENABLE([log-all-messages], [AS_HELP_STRING([--enable-log-all-messages],
              [enable code to log all messages @<:@default: no@:>@])],
              [enable_logall=${enableval}], [enable_logall=no])
if test x"${enable_logall}" = xyes; then
    AC_DEFINE([CONSUS_LOG_ALL_MESSAGES], [], [Log all network traffic at the INFO level])
fi

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
