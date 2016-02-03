#include "parser.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>

#include "db.h"
#include "table.h"
#include "column.h"
#include "fileparser.h"
#include "query.h"

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
		free(full_name);
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
		free(full_name);  
		str_cpy = NULL;
		full_name = NULL;
		
		// No db_operator required, since no query plan
		status ret;
		ret.code = CMD_DONE;
		return ret;
	}
	else if (LOAD_FILE_CMD == d->g) {
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
		
		size_t lineCount = 0;
		size_t fieldCount = 0;
		collect_file_info(filename, &lineCount, &fieldCount);
	
		if (1 >= lineCount) {
			ret.code = ERROR;
		}  
		else {
			ret = load_data4file(filename, lineCount, fieldCount);
		}

		// Free the str_cpy
		free(str_cpy);
		// free(full_name);  full_name is to be used in the "name" field in struct
		str_cpy = NULL;
		// full_name = NULL;

		return ret;
	}
	else if (SELECT_COL_CMD == d->g  || SELECT_PRE_CMD == d->g
			|| FETCH_CMD == d->g || TUPLE_CMD == d->g
			|| INSERT_CMD == d->g || DELETE_CMD == d->g
			|| UPDATE_CMD == d->g) {
		status s = query_prepare(str, d, op);
		if (OK != s.code) {
			switch (d->g) {
				case SELECT_COL_CMD: {
					log_err("cannnot prepare the query for select column!");
					break;
				}
				case SELECT_PRE_CMD: {
					log_err("cannnot prepare the query for select previous!");
					break;
				}
				case FETCH_CMD: {
					log_err("cannnot prepare the query for fetch!");
					break;
				}
				case TUPLE_CMD: {
					log_err("cannnot prepare the query for tuple!");
					break;
				}
				case DELETE_CMD: {
					log_err("cannnot prepare the query for delete!");
					break;
				}
				case INSERT_CMD: {
					log_err("cannnot prepare the query for insert!");
					break;
				}
				case UPDATE_CMD: {
					log_err("cannnot prepare the query for update!");
					break;
				}
				default: break;
			}
			return s;
		}
		else return s;
	}
	else if (QUIT_CMD == d->g || SHUTDOWN_CMD == d->g) {
		status ret;
		clear_res_list();
		ret = sync_db(NULL);
		if (OK != ret.code) {
			log_err("failed to sync database!");
		}
		else {
			ret.code = (QUIT_CMD == d->g)? QUIT: SHUTDOWN;
		}
		return ret;
	}
	else if (d->g == SHOW_DB_CMD) {
		status ret;
		ret.code = OK;
		op->type = SHOWDB;
		return ret;
	}
	else if (d->g == PARTITION_TEST) {

		// Create a working copy, +1 for '\0'
		char* str_cpy = malloc(strlen(str) + 1);
		strncpy(str_cpy, str, strlen(str) + 1);

		// This gives us everything inside the ("colname")
		strtok(str_cpy, open_paren);
		char* args = strtok(NULL, close_paren);

		// This gives us the column name
		const char* col_var = strtok(args, quotes);

		unsigned int i =0, flag = 0;
		while('\0' != col_var[i]) {
			if ('.' == col_var[i]) {
				flag++;
				if (2 == flag) {		// Find the second '.'
					break;
				}
			}
			i++;
		}
		char* tbl_var = malloc(sizeof(char) * (i + 1));
		strncpy(tbl_var, col_var, i);
		tbl_var[i] = '\0';
		printf("table name in partition_test %s\n", tbl_var);
		
		// Grab the table for further reference
		Table* tmp_tbl = NULL;
		status s = grab_table(tbl_var, &tmp_tbl);
		if (OK != s.code) {
			log_err("cannot grab the table!");
			return s;
		}
		free(tbl_var);

		// Grab the column
		Column* tmp_col = NULL;
		s = grab_column(col_var, &tmp_col);
		if (OK != s.code) {
			log_err("cannot grab the column!");
			return s;
		}

		// Load data from disk if not in memory
		if (NULL == tmp_col->data && NULL != tmp_tbl && 0 != tmp_tbl->length) {
			// load_column4disk(tmp_col, tmp_tbl->length);
			for (unsigned int j = 0; j < tmp_tbl->col_count; j++) {
				load_column4disk(tmp_tbl->cols[j], tmp_tbl->length);
			}
		}
		else if (0 == tmp_tbl->length) {
			log_err("empty table to partition");
			s.code = ERROR;
			return s;
		}

		status ret = create_index(tmp_tbl, tmp_col, PARTI);
		
		// Free the str_cpy
		free(str_cpy);
		str_cpy = NULL;

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
