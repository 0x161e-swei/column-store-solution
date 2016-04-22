# -*- makefile -*-

AUTOMAKE_OPTIONS = subdir-objects

bin_PROGRAMS = opt_part_server client demo_opt_part_server

SRCDIR = src
INCDIR = include

noinst_LIBRARIES = lib/libutil.a lib/librum.a lib/libdarray.a lib/libfrequencymodel.a lib/libdemobase.a lib/libcmdsocket.a

O ?= 0 

AM_CFLAGS = -std=c99 -g -ggdb3 -Wall -Wextra -pedantic -pthread -O$(O)
AM_CPPFLAGS = -I$(INCDIR)
DEMO_AM_CPPFLAGS = $(AM_CPPFLAGS) -DDEMO

###########################################
#
# src/cmdsocket
#
###########################################

CMDSOC = src/cmdsocket/cmdsocket.c

lib_libcmdsocket_a_CFLAGS = $(AM_CFLAGS)
lib_libcmdsocket_a_CPPFLAGS = $(AM_CPPFLAGS)
lib_libcmdsocket_a_SOURCES = $(CMDSOC)

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
# src/frequencymodel
#
###########################################

FREQUENCYMODEL = \
	src/frequencymodel/customsort/custom_sort.c \
	src/frequencymodel/frequency_model.c
	

lib_libfrequencymodel_a_CFLAGS = $(AM_CFLAGS)
lib_libfrequencymodel_a_CPPFLAGS = $(AM_CPPFLAGS)
lib_libfrequencymodel_a_SOURCES = $(FREQUENCYMODEL)


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
	src/database/query.c \
	src/database/index.c

lib_librum_a_CFLAGS = $(AM_CFLAGS)
lib_librum_a_CPPFLAGS = $(AM_CPPFLAGS)
lib_librum_a_SOURCES = $(RUM)
lib_librum_a_LIBADD = lib/libdarray.a \
		lib/libfrequencymodel.a \
		lib/libutil.a


lib_libdemobase_a_CFLAGS = $(AM_CFLAGS)
lib_libdemobase_a_CPPFLAGS = $(DEMO_AM_CPPFLAGS)
lib_libdemobase_a_SOURCES = $(RUM)
lib_libdemobase_a_LIBADD = lib/libdarray.a \
		lib/libfrequencymodel.a \
		lib/libutil.a \
		lib/libcmdsocket.a


###########################################
#
# executable programs 
#
###########################################

opt_part_server_CFLAGS = $(AM_CFLAGS)
opt_part_server_CPPFLAGS = $(AM_CPPFLAGS)
opt_part_server_LDADD = lib/libutil.a \
	lib/librum.a \
	lib/libfrequencymodel.a \
	lib/libdarray.a
opt_part_server_SOURCES = src/server/server.c


demo_opt_part_server_CFLAGS = $(AM_CFLAGS)
demo_opt_part_server_CPPFLAGS = $(AM_CPPFLAGS)
demo_opt_part_server_LDADD =  	lib/libdemobase.a \
	lib/libfrequencymodel.a \
	lib/libdarray.a \
	lib/libutil.a \
	lib/libcmdsocket.a \
	-levent
demo_opt_part_server_SOURCES = src/server/rum_demo_server.c


client_CFLAGS = $(AM_CFLAGS)
client_CPPFLAGS = $(AM_CPPFLAGS)
client_LDADD = lib/libutil.a
client_SOURCES = src/client/client.c


###########################################
#
# Clean files 
#
###########################################


