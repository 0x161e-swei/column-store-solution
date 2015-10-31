#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([RUM-SYSTEM], [0.1.0], [author@hidden])
AC_CONFIG_AUX_DIR([build_aux])
AM_INIT_AUTOMAKE([subdir-objects foreign])
AM_SILENT_RULES([yes])

#AC_CONFIG_SRCDIR([src/server.c])
AC_CONFIG_HEADERS([include/config.h])

: ${CFLAGS=""}

# Checks for programs.
AC_PROG_CC
AC_PROG_RANLIB

AC_DEFINE([LOG])
AC_DEFINE([LOG_ERR])
AC_DEFINE([LOG_INFO])
AH_TEMPLATE([LOG],[Define for log])
AH_TEMPLATE([LOG_ERR], [Define for log_err])
AH_TEMPLATE([LOG_INFO], [Define for log_info])

# Checks for libraries.
AC_CHECK_LIB([csv], [csv_init], [],
	AC_MSG_FAILURE([libcsv requested but library not found]), [])

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h stddef.h stdint.h stdlib.h string.h sys/socket.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_TYPE_SIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT8_T
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([memset regcomp select socket])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT