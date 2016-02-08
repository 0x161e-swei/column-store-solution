// dsl.h
//
// CS165, Fall 2015
//
// This file defines some of the regular expressions to match the incoming DSL.
// For now, we implement regular expressions to match the create functions for
// you, and you are free to update this for all the other DSL commands.

#ifndef DSL_H__
#define DSL_H__

#include <stdlib.h>

// Currently we have 4 DSL commands to parse.
// TODO(USER): you will need to increase this to track the commands you support.
#define NUM_DSL_COMMANDS (16)

// This helps group similar DSL commands together.
// For example, some queries can be parsed together:
//
// create(col, <col_name>, <tbl_name>, unsorted)
// create(col, <col_name>, <tbl_name>, sorted)
//
// and
//
// p = select(col1, 5, 5) (point query)
// p = select(col1, 5, 10) (range query)
typedef enum DSLGroup {
	CREATE_DB_CMD,
	CREATE_TABLE_CMD,
	CREATE_COLUMN_CMD,
	QUIT_CMD,
	LOAD_FILE_CMD,
	SHOW_DB_CMD,
	SELECT_COL_CMD,
	SELECT_PRE_CMD,
	FETCH_CMD,
	TUPLE_CMD,
	SHUTDOWN_CMD,
	DELETE_CMD,
	INSERT_CMD,
	UPDATE_CMD,
	PARTITION_TEST

	// TODO(USER): Add more here...
} DSLGroup;

// A dsl is defined as the DSL listed on the project website.
// We use this to track the relevant string to parse, and its group.
typedef struct dsl {
	const char* c;
	DSLGroup g;
} dsl;

// This returns an array of all the DSL commands that you can match with.
dsl** dsl_commands_init(void);

// We define these in the dsl.c file.
// TODO(USER) Add more extern strings for new commands
extern const char* create_db_command;
extern const char* create_table_command;
extern const char* create_col_command_sorted;
extern const char* create_col_command_unsorted;
extern const char* quit_command;
extern const char* shutdown_command;
extern const char* load_command;
extern const char* select_from_col_command;
extern const char* select_from_pre_command;
extern const char* fetch_command;
extern const char* tuple_command;
extern const char* delete_command;
extern const char* insert_command;
extern const char* update_command;

extern const char* partition_test;

#endif // DSL_H__
