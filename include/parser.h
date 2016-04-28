#ifndef PARSER_H__
#define PARSER_H__


#define _GNU_SOURCE
#include <stdio.h>
#include "cs165_api.h"
#include "dsl.h"




#ifdef DEMO
#include "cmdsocket.h"
status parse_dsl(struct cmdsocket *cmdSoc, char* str, dsl* d, db_operator* op);
status parse_command_string(struct cmdsocket *cmdSoc, char* str, dsl** commands, db_operator* op);
#else
// Prototype for Helper function that executes that actual parsing after
// parse_command_string has found a matching regex.
status parse_dsl(char* str, dsl* d, db_operator* op);

// This parses the command string and then update the db_operator if it requires
// a specific query plan to be executed.
// This can automatically run jobs that don't require a query plan,
// (e.g., create_db, create_tbl, create_col);
//
// Usage: parse_command_string(input_query, commands, operator);
status parse_command_string(char* str, dsl** commands, db_operator* op);
#endif // DEMO

void workload_parse(const char *filename, int *ops, int *num1, int *num2);

#endif // PARSER_H__
