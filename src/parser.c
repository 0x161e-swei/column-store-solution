#include "parser.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>

#include "db.h"
#include "table.h"
#include "column.h"
#include "fileparser.h"

// Prototype for Helper function that executes that actual parsing after
// parse_command_string has found a matching regex.
status parse_dsl(char* str, dsl* d, db_operator* op);

// Finds a possible matching DSL command by using regular expressions.
// If it finds a match, it calls parse_command to actually process the dsl.
status parse_command_string(char* str, dsl** commands, db_operator* op)
{
	log_info("Parsing: %s\n", str);

	// Create a regular expression to parse the string
	regex_t regex;
	int ret;

	// Track the number of matches; a string must match all
	int n_matches = 1;
	regmatch_t m;

	for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
		dsl* d = commands[i];
		if (regcomp(&regex, d->c, REG_EXTENDED) != 0) {
			log_err("Could not compile regex\n");
		}

		// Bind regular expression associated with the string
		ret = regexec(&regex, str, n_matches, &m, 0);

		// If we have a match, then figure out which one it is!
		if (ret == 0) {
			log_info("Found Command: %d\n", i);
			// Here, we actually strip the command as appropriately
			// based on the DSL to get the variable names.
			return parse_dsl(str, d, op);
		}
	}

	// Nothing was found!
	status s;
	s.code = UNKNOWN_CMD;
	return s;
}

status parse_dsl(char* str, dsl* d, db_operator* op)
{
	// Use the commas to parse out the string
	char open_paren[2] = "(";
	char close_paren[2] = ")";
	char comma[2] = ",";
	char quotes[2] = "\"";
	// char end_line[2] = "\n";
	// char eq_sign[2] = "=";

	if (d->g == CREATE_DB_CMD) {
		// Create a working copy, +1 for '\0'
		char* str_cpy = malloc(strlen(str) + 1);
		strncpy(str_cpy, str, strlen(str) + 1);

		// This gives us everything inside the (db, "<db_name>")
		strtok(str_cpy, open_paren);
		char* args = strtok(NULL, close_paren);

		// This gives us "db", but we don't need to use it
		char* db_indicator = strtok(args, comma);
		(void) db_indicator;

		// This gives us the , before the quote
		// char* delimiter = strtok(NULL, quotes);
		// (void) delimiter;

		// This gives us "<db_name>"
		char* db_name = strtok(NULL, quotes);
		char* full_name = (char *) malloc(sizeof(char) * strlen(db_name) + 1);

		strcpy(full_name, db_name);
		printf("%s\n", full_name);

		// Here, we can create the DB using our parsed info!
		Db* db1 = NULL;
		status s = create_db(full_name, &db1);
		if (OK != s.code) {
			// Something went wrong
			log_err("fialed to create databse");
			return s;
		}

		// TODO(USER): You must track your variable in a variable pool now!
		// This means later on when I refer to <db_name>, I should get this
		// same db*.  You can do this in many ways, including associating
		// <db_name> -> db1

		// Free the str_cpy
		free(str_cpy);
		free(full_name);
		str_cpy = NULL;
		full_name = NULL;

		// No db_operator required, since no query plan
		(void) op;
		status ret;
		ret.code = CMD_DONE;
		return ret;
	} else if (d->g == CREATE_TABLE_CMD) {
		// Create a working copy, +1 for '\0'
		char* str_cpy = malloc(strlen(str) + 1);
		strncpy(str_cpy, str, strlen(str) + 1);

		// This gives us everything inside the (table, <tbl_name>, <db_name>, <count>)
		strtok(str_cpy, open_paren);
		char* args = strtok(NULL, close_paren);

		// This gives us "table"
		char* tbl_indicator = strtok(args, comma);
		(void) tbl_indicator;

		// This gives us <tbl_name>, we will need this to create the full name
		char* tbl_name = strtok(NULL, quotes);

		// This gives us <db_name>, we will need this to create the full name
		char* db_name = strtok(NULL, comma);

		// Generate the full name using <db_name>.<tbl_name>
		char* full_name = (char*)malloc(sizeof(char) * (strlen(tbl_name) + strlen(db_name) + 2));
		
		strncpy(full_name, db_name, strlen(db_name) + 1);
		strncat(full_name, ".", 1);
		strncat(full_name, tbl_name, strlen(tbl_name));

		// This gives us count
		char* count_str = strtok(NULL, comma);
		int count = 0;
		if (count_str != NULL) {
			count = atoi(count_str);
		}
		(void) count;

		log_info("create_table(%s, %s, %d)\n", full_name, db_name, count);
	
		// Here, we can create the table using our parsed info!
		// TODO(USER): You MUST get the original db* associated with <db_name>
		Db* db1 = NULL;
		status s = grab_db(db_name, &db1);
		if (OK != s.code) {
			// Something went wrong
			log_err("No database found");
			return s;
		}

		// TODO(USER): Uncomment this section after you're able to grab the db1
		Table* tbl1 = NULL;
		s = create_table(db1, full_name, count, &tbl1);
		if (OK != s.code) {
			// Something went wrong
			log_err("cannot create table");
			return s;
		}

		// TODO(USER): You must track your variable in a variable pool now!
		// This means later on when I refer to <full_name>, I should get this
		// same table*.  You can do this in many ways, including associating
		// <full_name> -> tbl1

		// Free the str_cpy
		free(str_cpy);
		free(full_name); // full name is used in the "name" field in struct
		str_cpy = NULL;
		full_name = NULL;

		// No db_operator required, since no query plan
		status ret;
		ret.code = CMD_DONE;
		return ret;
	} else if (d->g == CREATE_COLUMN_CMD) {
		// Create a working copy, +1 for '\0'
		char* str_cpy = malloc(strlen(str) + 1);
		strncpy(str_cpy, str, strlen(str) + 1);

		// This gives us everything inside the (col, <col_name>, <tbl_name>, unsorted)
		strtok(str_cpy, open_paren);
		char* args = strtok(NULL, close_paren);

		// This gives us "col"
		char* col_indicator = strtok(args, comma);
		(void) col_indicator;

		// This gives us <col_name>, we will need this to create the full name
		char* col_name = strtok(NULL, quotes);
		// This gives us <tbl_name>, we will need this to create the full name
		char* tbl_name = strtok(NULL, comma);
		// Generate the full name using <db_name>.<tbl_name>
		char* full_name = (char*)malloc(sizeof(char)*(strlen(tbl_name) + strlen(col_name) + 2));
		strncpy(full_name, tbl_name, strlen(tbl_name) + 1);
		strncat(full_name, ".", 1);
		strncat(full_name, col_name, strlen(col_name));

		// This gives us the "unsorted"
		char* sorting_str = strtok(NULL, comma);
		(void) sorting_str;
		log_info("create_column(%s, %s, %s)\n", full_name, tbl_name, sorting_str);

		// Here, we can create the column using our parsed info!
		// TODO(USER): You MUST get the original table* associated with <tbl_name>
		Table* tbl1 = NULL;
		status s = grab_table(tbl_name, &tbl1);
		if (OK != s.code) {
			// Something went wrong
			log_err("cannot grab the table");
			return s;
		}

		// TODO(USER): Uncomment this section after you're able to grab the tbl1
		Column* col1 = NULL;
		s = create_column(tbl1, full_name, &col1);
		if (OK != s.code) {
			// Something went wrong
			log_err("cannot create the column");
			return s;
		}

		// TODO(USER): You must track your variable in a variable pool now!
		// This means later on when I refer to <full_name>, I should get this
		// same col*.  You can do this in many ways, including associating
		// <full_name> -> col1

		// Free the str_cpy
		free(str_cpy);
		// free(full_name);  full_name is to be used in the "name" field in struct
		str_cpy = NULL;
		// full_name = NULL;
		
		// No db_operator required, since no query plan
		status ret;
		ret.code = CMD_DONE;
		return ret;
	}
	else if (d->g == LOAD_FILE_CMD) {
		status ret;
		// Create a working copy, +1 for '\0'
		char* str_cpy = malloc(strlen(str) + 1);
		strncpy(str_cpy, str, strlen(str) + 1);

		// This gives us everything inside the ("filename")
		strtok(str_cpy, open_paren);
		char* args = strtok(NULL, close_paren);

		// This gives us the filename
		const char* filename = strtok(args, quotes);
		
		log_info("load(\"%s\")", filename);
		size_t line_count = count_file_lines(filename);
	
		if (1 >= line_count) {
			ret.code = ERROR;
		}  
		else {
			ret = load_data4file(filename, line_count - 1);
		}

		// Free the str_cpy
		free(str_cpy);
		// free(full_name);  full_name is to be used in the "name" field in struct
		str_cpy = NULL;
		// full_name = NULL;

		return ret;
	}
	else if (d->g == SELECT_COL_CMD) {
		status s = query_prepare(str, d, op);
		if (OK != s.code) {
			log_err("cannnot prepare the query for select column!");
			return s;
		}
		else return s;
		// char* str_cpy = malloc(strlen(str) + 1);
		// strncpy(str_cpy, str, strlen(str) + 1);
		// char* pos_name = strtok(str_cpy, equal);

		// char* pos_var = malloc(sizeof(char) * (strlen(pos_name) + 1));
		// strncpy(pos_var, pos_name, strlen(pos_name) + 1); 
		// strtok(NULL, open_paren);

		// // This gives us everything inside the '(' ')'
		// char* args = strtok(NULL, close_paren);

		// // This gives us the column name specified
		// char* col_name = strtok(args, comma);

		// char* low_str = strtok(NULL, comma);
		// int low = 0;
		// if (NULL != low_str) {
		//	 low = atoi(low_str);
		// }
		// (void) low;

		// char* high_str  = strtok(NULL, comma);
		// int high = 0;
		// if (NULL != high_str) {
		//	 high = atoi(high_str);
		// }
		// (void) high;
		// log_info("%s=select(%s,%d,%d)", pos_var, col_name, low, high);

		// // Grab the column
		// status s;
		// Column *tmp_col = NULL;
		// s = grab_column(col_name, &tmp_col);

		// if (OK != s.code) {
		//	 log_err("cannot grab the column!\n");
		//	 return s;
		// }

		// op->type = SELECT_COL;
		// op->tables = NULL;

		// op->columns = malloc(sizeof(Col_ptr));
		// op->columns[0] = tmp_col;
		
		// op->pos1 = NULL;
		// op->pos2 = NULL;

		// // Keep track of the range
		// op->value1 = malloc(sizeof(int));
		// op->value2 = malloc(sizeof(int));
		// op->value1 = low;
		// op->value2 = high;

		// // Keep track of the name for further refering
		// op->res_name = pos_var;

		// status ret; 
		// ret.code = OK;
		// return ret;

	}
	else if (d->g == SELECT_PRE_CMD) {
		status s = query_prepare(str, d, op);
		if (OK != s.code) {
			log_err("cannnot prepare the query for select previous!");
			return s;
		}
		else return s;
	}
	else if (d->g == FETCH_CMD) {
		status s = query_prepare(str, d, op);
		if (OK != s.code) {
			log_err("cannnot prepare the query for getch!");
			return s;
		}
		else return s;	
	}
	else if (d->g == QUIT_CMD) {
		status ret;
		ret = sync_db(NULL);
		return ret;
	}
	else if (d->g == SHOW_DB_CMD) {
		status ret;
		ret.code = OK;
		op->type = SHOWDB;
		return ret;
	}
	else {
		status ret;
		ret.code = UNKNOWN_CMD;
		(void)op;
		return ret;
	}

	// Should have been caught earlier...
	status fail;
	fail.code = ERROR;
	return fail;
}
