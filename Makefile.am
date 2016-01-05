# -*- makefile -*-

AUTOMAKE_OPTIONS = subdir-objects

bin_PROGRAMS = server client

SRCDIR = src
INCDIR = include

noinst_LIBRARIES = lib/libutil.a lib/librum.a lib/libdarray.a

O ?= 3

AM_CFLAGS = -std=c99 -g -ggdb3 -Wall -Wextra -pedantic -pthread -O$(O)
AM_CPPFLAGS = -I$(INCDIR)

###########################################
#
# src/util
#
###########################################

UTIL = src/util/utils.c

lib_libutil_a_CFLAGS = $(AM_CFLAGS)
lib_libutil_a_CPPFLAGS = $(AM_CPPFLAGS)
lib_libutil_a_SOURCES = $(UTIL)

###########################################
#
# src/darray
#
###########################################

DARRAY = \
	src/darray/darray.c

lib_libdarray_a_CFLAGS = $(AM_CFLAGS)
lib_libdarray_a_CPPFLAGS = $(AM_CPPFLAGS)
lib_libdarray_a_SOURCES = $(DARRAY)



###########################################
#
# src/database
#
###########################################

RUM = \
	src/database/db.c \
	src/database/table.c \
	src/database/column.c \
	src/database/dsl.c \
	src/database/fileparser.c \
	src/database/parser.c \
	src/database/query.c 

lib_librum_a_CFLAGS = $(AM_CFLAGS)
lib_librum_a_CPPFLAGS = $(AM_CPPFLAGS)
lib_librum_a_SOURCES = $(RUM)
lib_librum_a_LIBADD = lib/libdarray.a



###########################################
#
# executable programs 
#
###########################################

server_CFLAGS = $(AM_CFLAGS)
server_CPPFLAGS = $(AM_CPPFLAGS)
server_LDADD = lib/libutil.a \
	lib/librum.a \
	lib/libdarray.a
server_SOURCES = src/server/server.c

client_CFLAGS = $(AM_CFLAGS)
client_CPPFLAGS = $(AM_CPPFLAGS)
client_LDADD = lib/libutil.a
client_SOURCES = src/client/client.c


###########################################
#
# Clean files 
#
###########################################


