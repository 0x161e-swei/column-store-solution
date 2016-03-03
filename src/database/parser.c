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

		// Here, we can create the DB using our parsed info!
		Db* db1 = NULL;
		status s = create_db(full_name, &db1);
		if (OK != s.code) {
			// Something went wrong
			log_err("fialed to create databse\n");
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
			log_err("No database found\n");
			return s;
		}

		// TODO(USER): Uncomment this section after you're able to grab the db1
		Table* tbl1 = NULL;
		s = create_table(db1, full_name, count, &tbl1);
		if (OK != s.code) {
			// Something went wrong
			log_err("cannot create table\n");
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
			log_err("cannot grab the table\n");
			return s;
		}

		// TODO(USER): Uncomment this section after you're able to grab the tbl1
		Column* col1 = NULL;
		s = create_column(tbl1, full_name, &col1);
		if (OK != s.code) {
			// Something went wrong
			log_err("cannot create the column\n");
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
		log_info("load(\"%s\")\n", filename);
		// size_t lineCount = 0;
		// size_t fieldCount = 0;
		// collect_file_info(filename, &lineCount, &fieldCount);
	
		// if (1 >= lineCount) {
		// 	ret.code = ERROR;
		// }
		// else {
		ret = parse_dataset_csv(filename);
			// ret = parse_dataset_csv(filename, lineCount, fieldCount);
			// ret = load_data4file(filename, lineCount, fieldCount);
		// }

		// Free the str_cpy
		free(str_cpy);
		// free(full_name);  full_name is to be used in the "name" field in struct
		str_cpy = NULL;
		// full_name = NULL;

		return ret;
	}
	else if (SELECT_COL_CMD == d->g  || SELECT_PRE_CMD == d->g
			|| FETCH_CMD == d->g || TUPLE_CMD == d->g
			|| INSERT_CMD == d->g || DELETE_CMD == d->g || DELETE_POS_CMD == d->g
			|| UPDATE_CMD == d->g) {
		status s = query_prepare(str, d, op);
		if (OK != s.code) {
			log_err("fail to prepare the query!\n");
			return s;
		}
		else return s;
	}
	else if (QUIT_CMD == d->g || SHUTDOWN_CMD == d->g) {
		status ret;
		clear_res_list();
		ret = sync_db(NULL);
		if (OK != ret.code) {
			log_err("failed to sync database!\n");
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
	else if (d->g == SHOWTBL_TEST) {
		status ret;
		// Create a working copy, +1 for '\0'
		char* str_cpy = malloc(strlen(str) + 1);
		strncpy(str_cpy, str, strlen(str) + 1);

		// This gives us everything inside the ("colname")
		strtok(str_cpy, open_paren);
		char* args = strtok(NULL, close_paren);
		
		// prepare table and column
		Table *tmp_tbl = NULL;
		Column *tmp_col = NULL;
		args = prepare_col(args, &tmp_tbl, &tmp_col);

		if (NULL == tmp_tbl || NULL == tmp_tbl) {
			log_err("wrong column/table name in show table\n");
			ret.code = ERROR;
			return ret;
		}

		if (tmp_tbl->length > 0) {
			for (unsigned int j = 0; j < tmp_tbl->col_count; j++) {
				if (NULL == tmp_tbl->cols[j]->data)
					load_column4disk(tmp_tbl->cols[j], tmp_tbl->length);
			}
		}
			
		op->type = SHOWTBL;
		op->tables = malloc(sizeof(Tbl_ptr));
		op->tables[0] = tmp_tbl;
		ret.code = OK;
		return ret;
	}
	else if (d->g == PARTITION_TEST) {
		status ret;
		// Create a working copy, +1 for '\0'
		char* str_cpy = malloc(strlen(str) + 1);
		strncpy(str_cpy, str, strlen(str) + 1);

		// This gives us everything inside the ("colname")
		strtok(str_cpy, open_paren);
		char* args = strtok(NULL, close_paren);

		// prepare table and column
		Table *tmp_tbl = NULL;
		Column *tmp_col = NULL;
		args = prepare_col(args, &tmp_tbl, &tmp_col);

		if (NULL == tmp_tbl || NULL == tmp_tbl) {
			log_err("wrong column/table name in partition test\n");
			ret.code = ERROR;
			return ret;
		}

		tmp_tbl->primary_indexed_col = tmp_col;
		// 
		const char* filename = strtok(args, quotes);
		log_info("workload file: \"%s\"\n", filename);
		size_t lineCount = 0;
		size_t fieldCount = 0;
		collect_file_info(filename, &lineCount, &fieldCount);

		if (1 >= lineCount) {
			log_err("cannot workload file\n");
			ret.code = ERROR;
			return ret;
		}
		
		int *op_type = malloc(sizeof(int) * (lineCount + 1));
		int *num1 = malloc(sizeof(int) * (lineCount + 1));
		int *num2 = malloc(sizeof(int) * (lineCount + 1));
		workload_parse(filename, op_type, num1, num2);
		
		Workload w;
		w.ops = op_type;
		w.num1 = num1;
		w.num2 = num2;
		w.count = lineCount + 1;
		// Load data from disk if not in memory
		if (tmp_tbl->length != 0) {
			// do the loading later
			// for (unsigned int j = 0; j < tmp_tbl->col_count; j++) {
			// 	if (NULL == tmp_tbl->cols[j]->data)
			// 		load_column4disk(tmp_tbl->cols[j], tmp_tbl->length);
			// }
		}
		else {
			log_err("empty table to partition\n");
			ret.code = ERROR;
			return ret;
		}

		// TODO: make create_index a db operator?
		ret = create_index(tmp_tbl, tmp_col, PARTI, w);
		
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

void workload_parse(const char *filename, int *ops, int *num1, int *num2) {
	FILE* fp = fopen(filename, "r");
	char *line = NULL;
	ssize_t read;
	size_t len = 0;
	size_t count = 0;
	db_operator *op = malloc(sizeof(db_operator));	
	if (NULL != fp) {
		while (-1 != (read = getline(&line, &len, fp))) {
			parse_command_string(line, dsl_commands, op);
			switch(op->type) {
				case SELECT_COL: {
					// TODO: 
					if (((op->c[0]).p_val + 1) == (op->c[1]).p_val) {
						ops[count] = 0;
						num1[count] = (op->c[0]).p_val;
						num2[count] = -1;
					}
					else {
						ops[count] = 1;
						num1[count] = (op->c[0]).p_val;
						num2[count] = (op->c[1]).p_val;
					}
					// cleanups
					free(op->res_name);
					free(op->tables);
					free((op->domain).cols);
					free(op->c);
					break;
				}
				case INSERT: {
					ops[count] = 2;
					size_t i = 0;
					for (; i < op->tables[0]->col_count; i++) {
						if (op->tables[0]->cols[i] == op->tables[0]->primary_indexed_col) break;
					}
					num1[count] = op->value1[i];
					num2[count] = -1;
					free(op->tables);
					free(op->value1);
					break;
				}
				case UPDATE: {
					ops[count] = 3;
					num1[count] = op->value1[0];
					num2[count] = op->value2[0];
					free(op->tables);
					free((op->domain).cols);
					free(op->value1);
					free(op->value2);
					break;
				}
				case DELETE: {
					ops[count] = 4;
					num1[count] = op->value1[0];
					num2[count] = -1;
					free(op->tables);
					free((op->domain).cols);
					free(op->value1);
					break;
				}
				default: break;
				// case SELECT_PRE: {
				// 	break;
				// }
				// case DELETE_POS: {
				// 	break;
				// }
			}
			count++;
		}
		fclose(fp);
		// debug("after workload parsed:\n");
		// for (size_t ii = 0; ii < count; ii++) {
		// 	printf("%d %d %d\n", ops[ii], num1[ii], num2[ii]);
		// }
	}
}