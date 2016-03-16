#include "query.h"
#define BUFFERSIZE 3096

// Global results hash list to keep track of query results
Result *res_hash_list;

char open_paren[2] = "(";
char close_paren[2] = ")";
char comma[2] = ",";
// char quotes[2] = "\"";
char eq_sign[2] = "=";


/**
 * Grab the result from the result hash list 
 */
status grab_result(const char *res_name, Result **res) {
	status s;
	if (NULL == res_hash_list) {
		log_err("No results to grab from\n");
		*res = NULL;
		s.code = ERROR;
	}
	else {
		Result *tmp;
		HASH_FIND_STR(res_hash_list, res_name, tmp);
		if (NULL != tmp) {
			*res = tmp;
			s.code = OK;
		}
		else {
			log_err("No result named %s\n", res_name);
			*res = NULL;
			s.code = ERROR;
		}
	}
	return s;
}

/**
 * free up the space used in tracking previously queried results
 *
 */
status clear_res_list() {
	status ret;
	if (NULL != res_hash_list) {
		log_info("clearing the res_hash_list\n");
		Result *tmp, *res;
		HASH_ITER(hh, res_hash_list, res, tmp) {            
			if (NULL != res) {
				HASH_DEL(res_hash_list, res);
				free((void *)res->res_name);
				free(res->token);
				free(res);
			}
		}
		res_hash_list = NULL;
	}
	ret.code = OK;
	return ret;
}

/**
 * Prepare the db_operator for execution, get the Table and Column for select,
 * get the Result for fetch and tuple
 * By preparing the query, this function is responsible for taking all data needed
 * for the query into main memory
 * query:   the cmd string
 * d:       the matched dsl
 * op:      address of the db_operator 
 */ 
status query_prepare(const char* query, dsl* d, db_operator* op) {
	status s;
	// TODO: clean up memory space allocated in query preparation
	if (SELECT_COL_CMD == d->g) { 
		// TODO: combine the selcet_col_cmd  and select_pre_cmd together 
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
		char* pos_name = strtok(str_cpy, eq_sign);

		char* pos_var = malloc(sizeof(char) * (strlen(pos_name) + 1));
		strncpy(pos_var, pos_name, strlen(pos_name) + 1); 
		strtok(NULL, open_paren);

		// This gives us everything inside the '(' ')'
		char* args = strtok(NULL, close_paren);

		Table  *tmp_tbl = NULL;
		Column *tmp_col = NULL;
		args = prepare_col(args, &tmp_tbl, &tmp_col);
		// // This gives us <col_var>
		// char* col_var = strtok(args, comma);
		if (NULL == args) {
			log_err("wrong format in column name in select\n");
			s.code = ERROR;
			return s;
		}

		char *low_str = strtok(args, comma);
		// char* low_str = strtok(NULL, comma);
		int low = 0;
		if (NULL != low_str) {
			low = atoi(low_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("wrong select format\n");
			return s;
		}
		(void) low;

		char* high_str  = strtok(NULL, comma);
		int high = 0;
		if (NULL != high_str) {
			high = atoi(high_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("wrong select format\n");
			return s;   
		}
		(void) high;
		log_info("%s=select(%s,%d,%d)\n", pos_var, tmp_col->name, low, high);

		op->type = SELECT_COL;
		op->tables = malloc(sizeof(Tbl_ptr));
		op->tables[0] = tmp_tbl;
		(op->domain).cols = malloc(sizeof(Col_ptr));
		(op->domain).cols[0] = tmp_col;

		// op->pos1 = NULL;
		// op->pos2 = NULL;
		op->value1 = NULL;
		op->value2 = NULL;

		// Compose the comparator
		op->c = malloc(sizeof(comparator) * 2);
		(op->c[0]).p_val = low;
		(op->c[0]).type = GREATER_THAN | EQUAL;
		(op->c[0]).mode = AND;
		(op->c[1]).p_val = high;
		(op->c[1]).type = LESS_THAN;
		(op->c[1]).mode = NONE;
		(op->c[0]).next_comparator = &(op->c[1]);
		(op->c[1]).next_comparator = NULL;

		// Keep track of the name for further refering
		op->res_name = pos_var;

		// Cleanups
		free(str_cpy);
		s.code = OK;
		return s;
	}
	else if (SELECT_PRE_CMD == d->g) {
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
		char* res_name = strtok(str_cpy, eq_sign);

		char* pos_var = malloc(sizeof(char) * (strlen(res_name) + 1));
		strncpy(pos_var, res_name, strlen(res_name) + 1); 
		strtok(NULL, open_paren);

		// This gives us everything inside the '(' ')'
		char* args = strtok(NULL, close_paren);

		// This gives us <col_var> 
		char* col_var = strtok(args, comma);

		// This gives us <posn_vec>
		char* posn_vec = strtok(NULL, comma);

		char* low_str = strtok(NULL, comma);
		int low = 0;
		if (NULL != low_str) {
			low = atoi(low_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("wrong select format\n");
			return s;
		}
		(void) low;
		char* high_str  = strtok(NULL, comma);
		int high = 0;
		if (NULL != high_str) {
			high = atoi(high_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("wrong select format\n");
			return s;   
		}
		(void) high;

		log_info("%s=select(%s,%s,%d,%d)\n", pos_var, posn_vec, col_var, low, high);

		// Grab the position list
		Result *tmp_pos = NULL;
		s = grab_result(posn_vec, &tmp_pos);

		if (OK != s.code) {
			log_err("cannot grab the position in select_pre!\n");
			return s;
		}

		// Grab the sub column value list
		Result *tmp_val = NULL;
		s = grab_result(col_var, &tmp_val);
		if (OK != s.code) {
			log_err("cannot grab the value in select_pre!\n");
			return s;
		}

		op->type = SELECT_PRE;
		op->tables = NULL;

		(op->domain).res = malloc(sizeof(Res_ptr));
		(op->domain).res[0] = tmp_val;
		op->position = tmp_pos;

		// op->pos1 = NULL;
		// op->pos2 = NULL;
		op->value1 = NULL;
		op->value2 = NULL;

		// Compose the comparator
		op->c = malloc(sizeof(comparator) * 2);
		(op->c[0]).p_val = low;
		(op->c[0]).type = GREATER_THAN | EQUAL;
		(op->c[0]).mode = AND;
		(op->c[1]).p_val = high;
		(op->c[1]).type = LESS_THAN;
		(op->c[1]).mode = NONE;
		(op->c[0]).next_comparator = &(op->c[1]);
		(op->c[1]).next_comparator = NULL;

		// Keep track of the name for further refering
		op->res_name = pos_var;

		// Cleanups
		free(str_cpy);
		s.code = OK;
		return s;
	}
	else if (FETCH_CMD == d->g) {
		
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
		char* res_name = strtok(str_cpy, eq_sign);

		char* val_var = malloc(sizeof(char) * (strlen(res_name) + 1));
		strncpy(val_var, res_name, strlen(res_name) + 1); 
		strtok(NULL, open_paren);

		// This gives us everything inside the '(' ')'
		char* args = strtok(NULL, close_paren);

		// // THis gives us <col_var>
		// char* col_var = strtok(args, comma);

		// // This gives us <vec_pos>
		// char* vec_pos = strtok(NULL, comma);
		Table  *tmp_tbl = NULL;
		Column *tmp_col = NULL;
		Result *tmp_pos = NULL;
		args = prepare_col(args, &tmp_tbl, &tmp_col);
		if (NULL == args) {
			log_err("cannot grab column in fetch!\n");
			s.code = WRONG_FORMAT; 
			return s;
		}
		args = prepare_res(args, &tmp_pos);
		if (NULL == args) {
			log_err("cannot grab pos_vec in fetch!\n");
			s.code = WRONG_FORMAT; 
			return s;
		}
		
		log_info("%s=fetch(%s,%s)\n",val_var, tmp_col->name, tmp_pos->res_name);

		op->type = FETCH;
		op->tables = malloc(sizeof(Tbl_ptr));
		op->tables[0] = tmp_tbl;
		(op->domain).cols = malloc(sizeof(Col_ptr));
		(op->domain).cols[0] = tmp_col;
		op->position = tmp_pos;

		// op->pos1 = NULL;
		// op->pos2 = NULL;
		op->value1 = NULL;
		op->value2 = NULL;
		op->c = NULL;

		// Keep track of the name for further refering
		op->res_name = val_var;
		
		// Cleanups
		free(str_cpy);
		s.code = OK;
		return s;
	}
	else if (TUPLE_CMD == d->g) {
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
	
		// This gives us <col_var>  
		strtok(str_cpy, open_paren);
		char* col_var = strtok(NULL, close_paren);

		Result* tmp_res = NULL;
		s = grab_result(col_var, &tmp_res);

		if (OK != s.code) {
			log_err("tuple object not found\n");
			return s;
		}

		(op->domain).res = malloc(sizeof(Res_ptr));
		(op->domain).res[0] = tmp_res;

		op->type = TUPLE;
		op->tables = NULL;
		op->position = NULL;
		// op->pos1 = NULL;
		// op->pos2 = NULL;
		op->value1 = NULL;
		op->value2 = NULL;
		op->res_name = NULL;
		op->c = NULL;

		free(str_cpy);
		s.code = OK;
		return s;
	}
	else if (DELETE_CMD == d->g) {
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
		strtok(str_cpy, open_paren);

		// This gives us everything inside the '(' ')'
		char* args = strtok(NULL, close_paren);

		// Prepare the table, column and position vector
		Table *tmp_tbl = NULL;
		Column *tmp_col = NULL;
		args = prepare_col(args, &tmp_tbl, &tmp_col);

		if (NULL == args) {
			log_err("wroing format in delete command\n");
			s.code = WRONG_FORMAT;
			return s;
		}
		char* num_str = strtok(NULL, comma);
		int num = 0;
		if (NULL != num_str) {
			num = atoi(num_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("wrong select format\n");
			return s;
		}

		op->type = DELETE;
		op->tables = malloc(sizeof(Tbl_ptr));
		op->tables[0] = tmp_tbl;
		(op->domain).cols = malloc(sizeof(Col_ptr));
		(op->domain).cols[0] = tmp_col;
		op->value1 = malloc(sizeof(int));
		op->value1[0] = num;
		op->value2 = NULL;
		op->res_name = NULL;
		op->position = NULL;
		op->c = NULL;
		// op->c = malloc(sizeof(comparator) * 1);
		// (op->c[0]).p_val = num;
		// (op->c[0]).type = GREATER_THAN | EQUAL;
		// (op->c[0]).mode = NONE;

		// TODO: cleanup tables and op->domain.cols in query_exec
		free(str_cpy);
		s.code = OK;
		return s;
	}
	else if (DELETE_POS_CMD == d->g) {
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
		strtok(str_cpy, open_paren);

		// This gives us everything inside the '(' ')'
		char* args = strtok(NULL, close_paren);

		// Prepare the table, column and position vector
		Table *tmp_tbl = NULL;
		Column *tmp_col = NULL;
		Result *tmp_pos = NULL;
		args = prepare_col(args, &tmp_tbl, &tmp_col);
		args = prepare_res(args, &tmp_pos);

		if (NULL == tmp_tbl || NULL == tmp_col || NULL == tmp_pos) {
			log_err("cannot grab column or pos_vec in delete_pos!\n");
			s.code = ERROR; 
			return s;
		}
		log_info("delete vector %s in column %s\n", tmp_pos->res_name, tmp_col->name);

		op->type = DELETE_POS;
		op->tables = malloc(sizeof(Tbl_ptr));
		op->tables[0] = tmp_tbl;
		op->value1 = NULL;
		op->value2 = NULL;
		op->res_name = NULL;
		op->position = tmp_pos;
		op->c = NULL;

		free(str_cpy);
		s.code = OK;
		return s;
	}
	else if (INSERT_CMD == d->g) {
		// It is a relational insertion
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
		strtok(str_cpy, open_paren);

		// This gives us everything inside the '(' ')'
		char *args = strtok(NULL, close_paren);
		
		log_info("insert(%s)\n", args);

		Table *tmp_tbl = NULL;
		char *tbl_var = strtok(args, comma);
		s = grab_table(tbl_var, &tmp_tbl);
		if (OK != s.code) {
			log_err("cannot grab the table %s in insert!\n", tbl_var);
			return s;
		}

		// log_info("insert tbale found, %s\n", tmp_tbl->name);

		uint i = 0;
		op->value1 = malloc(sizeof(int) * tmp_tbl->col_count);
		// Grab all the integers
		
		while (i < tmp_tbl->col_count) {
			char* num_str = strtok(NULL, comma);
			if (NULL != num_str) {
				op->value1[i] = atoi(num_str);
				// log_info("insert number found %d\n", op->value1[i]);
				i++;
			}
			else {
				s.code = WRONG_FORMAT;
				log_err("cannot match %u th out of %zu integer in insert!\n", i, tmp_tbl->col_count);
				free(op->value1);
				return s;
			}
		}
		debug("numbers to insert in Table %s:\n",tmp_tbl->name);
		for (size_t i = 0; i < tmp_tbl->col_count; i++) {
			printf("%d ", op->value1[i]);
		}

		op->type = INSERT;
		op->tables = malloc(sizeof(Tbl_ptr));
		op->tables[0] = tmp_tbl;
		// op->pos1 = NULL;
		// op->pos2 = NULL;
		op->value2 = NULL;
		op->res_name = NULL;
		op->position = NULL;
		op->c = NULL;

		free(str_cpy);
		s.code = OK;
		return s;
	}
	else if (UPDATE_CMD == d->g) {
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
		strtok(str_cpy, open_paren);

		// This gives us everything inside the '(' ')'
		char* args = strtok(NULL, close_paren);

		// Prepare the table, column and position vector
		Table *tmp_tbl = NULL;
		Column *tmp_col = NULL;
		args = prepare_col(args, &tmp_tbl, &tmp_col);

		if (NULL == args) {
			log_err("cannot find the column to update\n");
			s.code = WRONG_FORMAT;
			return s;
		}

		// match the new value (integer)
		op->value1 = malloc(sizeof(int)); 
		char* num_str = strtok(args, comma);
		if (NULL != num_str) {
			*(op->value1) = atoi(num_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("cannot match old value in update\n");
			free(op->value1);
			return s;
		}

		// match the new value (integer)
		op->value2 = malloc(sizeof(int)); 
		num_str = strtok(NULL, comma);
		if (NULL != num_str) {
			*(op->value2) = atoi(num_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("cannot match new value in update\n");
			free(op->value2);
			return s;
		}

		op->type = UPDATE;
		op->tables = malloc(sizeof(Tbl_ptr));
		op->tables[0] = tmp_tbl;
		(op->domain).cols = malloc(sizeof(Col_ptr));
		(op->domain).cols[0] = tmp_col;
		op->res_name = NULL;
		op->position = NULL;
		op->c = NULL;

		free(str_cpy);
		s.code = OK;
		return s;
	}
	else {
		s.code = ERROR;
		return s;
	}

	// Should return earlier
	s.code = ERROR;
	return s;
}

/**
 * execute the query with the db_operator and put the output in results
 * op:      pointer to a db_operator storing detail of the operation
 * results: address of a Result to store the output
 */
status query_execute(db_operator* op, Result** results) {
	status s;
	switch (op->type) {
		case SELECT_COL: {
			if (NULL == (op->domain).cols[0]->data && 0 != op->tables[0]->length) {
				load_column4disk((op->domain).cols[0], op->tables[0]->length);
			}
			s = col_scan(op->c, (op->domain).cols[0], results);
			if (OK != s.code) {
				// Something Wrong
				return s;
			}
			(*results)->res_name = op->res_name;
			HASH_ADD_KEYPTR(hh, res_hash_list, ((*results)->res_name), 
				strlen((*results)->res_name), *results);
			break;
		}
		case SELECT_PRE: {
			s = col_scan_with_pos(op->c, (op->domain).res[0], op->position, results);
			if (OK != s.code) {
				// Something Wrong
				return s;
			}
			(*results)->res_name = op->res_name;
			HASH_ADD_KEYPTR(hh, res_hash_list, ((*results)->res_name), 
				strlen((*results)->res_name), *results);
			break;
		}
		case FETCH: {
			if (NULL == (op->domain).cols[0]->data && 0 != op->tables[0]->length) {
				load_column4disk((op->domain).cols[0], op->tables[0]->length);
			}
			s = fetch_val((op->domain).cols[0], op->position, results);
			if (OK != s.code) {
				// Something Wrong 
				return s;
			}
			(*results)->res_name = op->res_name;
			HASH_ADD_KEYPTR(hh, res_hash_list, ((*results)->res_name), 
				strlen((*results)->res_name), *results);
			break;
		}
		case DELETE: {
			if (0 != op->tables[0]->length) {
				for (size_t i = 0; i < op->tables[0]->col_count; i++) {
					if (NULL == (op->tables[0])->cols[i]->data) {
						load_column4disk((op->tables[0])->cols[i], op->tables[0]->length);
					}
				}
			}
			log_info("going to exec delete\n");
			delete_with_pointQuery(op->tables[0], (op->domain).cols[0], op->value1[0]);
			break;
		}
		case DELETE_POS: {
			log_info("going to exec delete_pos\n");
			if (0 != op->tables[0]->length) {
				for (size_t i = 0; i < op->tables[0]->col_count; i++) {
					if (NULL == (op->tables[0])->cols[i]->data) {
						load_column4disk((op->tables[0])->cols[i], op->tables[0]->length);
					}
				}
			}
			// TODO: make the delete relational by delete_with_pointQuery(op->tables[0], (op->domain).cols[0], val); 
			s = delete_with_pos(op->tables[0], op->position);
			if (OK != s.code) {
				log_err("cannot delete from table %s\n", (op->tables[0])->name);
				return s;
			}
			break;
		}
		case INSERT: {
			log_info("going to exec insert\n");
			if (0 != op->tables[0]->length) {
				for (size_t i = 0; i < op->tables[0]->col_count; i++) {
					if (NULL == (op->tables[0])->cols[i]->data) {
						load_column4disk((op->tables[0])->cols[i], op->tables[0]->length);
					}
				}
			}
			s = insert_tuple(op->tables[0], op->value1);
			if (OK != s.code) {
				log_err("cannot insert in table %s\n", (op->tables[0])->name);
				return s;
			}
			break;
		}
		case UPDATE: {
			log_info("going to exec update\n");
			if (NULL == (op->domain).cols[0] && 0 != op->tables[0]->length) {
				load_column4disk((op->domain).cols[0], op->tables[0]->length);
			}
			s = update_with_pointQuery((op->domain).cols[0], op->value1[0], op->value2[0]);
			if (OK != s.code) {
				// something wrong...
				return s;
			}
			break;
		}
		default:
			break;
	}
	return s;
}


/**
 * Comparasion used in SELECT operation
 * f:   pointer to the comparator containing all conditions
 * val: the integet to qualify
 * Return ture if val is qualified
 */
 bool compare(comparator *f, int val){
	bool res = false, undone = true, cur_res;
	Junction pre_junc = NONE;
	comparator *tmp_f = f;
	while (undone && NULL != tmp_f) {
		if (((val < tmp_f->p_val) && (tmp_f->type & LESS_THAN)) 
			|| ((val == tmp_f->p_val) && (tmp_f->type & EQUAL))
			|| ((val > tmp_f->p_val) && (tmp_f->type & GREATER_THAN))) {
			cur_res = true;
		}
		else {
			cur_res = false;
		}
		switch (pre_junc) {
			case NONE: {
				res = cur_res;  
				break;
			}
			case AND: {
				res &= cur_res;
				break;
			}
			case OR: {
				res |= cur_res;
				break;
			}
		}
		if (NONE != tmp_f->mode) {
			pre_junc = tmp_f->mode;
			tmp_f = tmp_f->next_comparator;
		}
		else {
			tmp_f = NULL;
			undone = false;
		}
	}
	return res;
}

/**
 * load the Column data from disk with length len
 * col: the Column structure to store the data
 * len: length of the Column to be loaded
 * BUFFERSIZE: the macro used here is exactually the size of the fs buffer size
 */
status load_column4disk(Column *col, size_t len) {
	status s;
	static char dataprefix[] = "data/";
	char *colfile = NULL;
	int namelen = strlen(col->name);
	colfile = malloc(sizeof(char) * (6 + namelen));
	strncpy(colfile, dataprefix, 6);
	strncat(colfile, col->name, namelen);
	// debug("loading column %s from disks!\n", colfile);

	col->data = darray_create(len);
	FILE *fp = fopen(colfile, "rb");
	free(colfile);

	size_t read = len;
	char *buffer = malloc(sizeof(int) * BUFFERSIZE);
	if (NULL == fp) {
		log_info("cannot open file!\n");
		s.code = ERROR;
		return s;
	}
	for (size_t i = 0; i <= (len - 1) / BUFFERSIZE; i++) {
		if (read < BUFFERSIZE) {
			size_t r = 0;
			while(r < read) {
				size_t t = fread(buffer + sizeof(int) * r, sizeof(int), read - r, fp);
				if (0 == t) {
					darray_destory(col->data); free(buffer); col->data = NULL;  
					if (ferror(fp)) {
						log_err("error loading from disks!\n");
						// darray_destory(col->data); free(buffer); col->data = NULL;
						fclose(fp);
						s.code = ERROR;
						return s;
					}
					else {
						fclose(fp);
						s.code = ERROR;
						return s;
					}
				}
				r += t;
			}
			fclose(fp);
			// append data into column
			darray_vec_push(col->data, buffer, read);
			free(buffer);
		}
		else {
			size_t r = 0;
			while (r < BUFFERSIZE) {
				size_t t = fread(buffer + sizeof(int) * r, sizeof(int), BUFFERSIZE - r, fp);
				if (0 == t) {
					darray_destory(col->data); 
					free(buffer); 
					col->data = NULL;
					if (ferror(fp) || feof(fp)) {
						log_err("error loading from disks!\n");
						// darray_destory(col->data);
						// free(buffer);
						// col->data = NULL;
						fclose(fp);
						s.code = ERROR;
						return s;
					}
					else {
						fclose(fp);
						s.code = ERROR;
						return s;   
					}
				}
				r += t;
			}
			read -= BUFFERSIZE;
			// append data into column
			darray_vec_push(col->data, buffer, BUFFERSIZE);
		}
	}

	s.code = OK;
	return s;
}

/** 
 * scan the whole Column with conditions
 * f:   pointer to the comparator containing all conditions
 * col: pointer to the Column to be scaned
 * r:   address of a pointer to Result to put to the output
 */
status col_scan(comparator *f, Column *col, Result **r) {
	status s;
	if (NULL != col) {
		*r = malloc(sizeof(Result));
		(*r)->token = NULL;
		(*r)->num_tuples = 0;
		for (uint i = 0; i < (col->data)->length; i++) {
			if (compare(f, (col->data)->content[i])) {
				// TODO: Results storing needs improving later
				(*r)->num_tuples++;
				(*r)->token = realloc((*r)->token, (*r)->num_tuples * sizeof(Payload));
				(*r)->token[(*r)->num_tuples - 1].pos = i;
			}
		}
		s.code = OK;
		log_info("col_scan %u tuple qualified\n", (*r)->num_tuples);
		for (uint ii = 0; ii < (*r)->num_tuples; ii++) {
			log_info("pos selected %u\n", (*r)->token[ii].pos);
		}
		return s;
	}
	s.code = ERROR;
	return s;
}

// TODO: is it necessary to have an extra point query for delete?
status col_point_query(Column *col, int val, Result **r) {
	status s;
	if (NULL != col) {
		pos_t beg = 0, end = 0;
		*r = malloc(sizeof(Result));
		(*r)->token = NULL;
		(*r)->num_tuples = 0;
		(*r)->partitionNum = NULL; 
		// for partitioned Column
		if (col->partitionCount > 1) {

			size_t partition_to_query = 0;
			while (partition_to_query < col->partitionCount 
				&& col->pivots[partition_to_query] < val) {
				partition_to_query++;
			}
			(*r)->partitionNum = malloc(sizeof(size_t));
			(*r)->partitionNum[0] = partition_to_query;
			beg = (partition_to_query == 0)? 0: (col->p_pos[partition_to_query - 1] + 1);
			// TODO: use NON_QUALIFYING_INT or mark the posision
			end = col->p_pos[partition_to_query] + 1;
			// #ifdef GHOST_VALUE
			// end = col->p_pos[partition_to_query] - col->ghost_count[partition_to_query] + 1;
			// #else
			// end = col->p_pos[partition_to_query] + 1;
			// #endif
		}
		else {	// unpartitioned case
			end = (col->data)->length;
		}
		for (pos_t i = beg; i < end; i++) {
			if (val == (col->data)->content[i]) {
				// TODO: Results storing needs improving later
				(*r)->num_tuples++;
				(*r)->token = realloc((*r)->token, (*r)->num_tuples * sizeof(Payload));
				(*r)->token[(*r)->num_tuples - 1].pos = i;
			}
		}
		s.code = OK;
		log_info("point query %u tuple qualified\n", (*r)->num_tuples);
		for (uint ii = 0; ii < (*r)->num_tuples; ii++) {
			log_info("pos selected %u\n", (*r)->token[ii].pos);
		}
		return s;
	}
	else {
		s.code = ERROR;
	}
	return s;
}

status col_range_query(Column *col, int low, int high, Result **r) {
	status s;
	if (NULL != col) {
		*r = malloc(sizeof(Result));
		(*r)->token = NULL;
		(*r)->num_tuples = 0;
		// for partitioned Column
		if (col->partitionCount > 1) {
			pos_t beg_l = 0, end_l = 0;
			pos_t beg_r = 0, end_r = 0;
			pos_t partition_to_query = 0;
			(*r)->partitionNum = malloc(sizeof(size_t) * 2);
			while (partition_to_query < col->partitionCount 
				&& col->pivots[partition_to_query] < low) {
				partition_to_query++;
			}
			(*r)->partitionNum[0] = partition_to_query;
			// TODO: check if in the same partition
			beg_l = (partition_to_query == 0)? 0: (col->p_pos[partition_to_query - 1] + 1);
			end_l = col->p_pos[partition_to_query] + 1;
			while (partition_to_query < col->partitionCount 
				&& col->pivots[partition_to_query] < high) {
				partition_to_query++;
			}
			(*r)->partitionNum[1] = partition_to_query;
			beg_r = (partition_to_query == 0)? 0: (col->p_pos[partition_to_query - 1] + 1);
			end_r = col->p_pos[partition_to_query] + 1;


			for (pos_t i = beg_l; i < end_l; i++) {
				if (low <= (col->data)->content[i] && high > (col->data)->content[i]) {
					// do something
				}
			}
			for (pos_t i = beg_r; i < end_r; i++) {
				if (low <= (col->data)->content[i] && high > (col->data)->content[i]) {
					// do something
				}
			}
		}
		else {	// unpartitioned case
			pos_t beg = 0, end = 0;
			end = (col->data)->length;
			for (pos_t i = beg; i < end; i++) {
				if (low <= (col->data)->content[i] && high > (col->data)->content[i]) {
					// TODO: Results storing needs improving later
					(*r)->num_tuples++;
					(*r)->token = realloc((*r)->token, (*r)->num_tuples * sizeof(Payload));
					(*r)->token[(*r)->num_tuples - 1].pos = i;
				}
			}
		}
		s.code = OK;
		log_info("point query %u tuple qualified\n", (*r)->num_tuples);
		for (uint ii = 0; ii < (*r)->num_tuples; ii++) {
			log_info("pos selected %u\n", (*r)->token[ii].pos);
		}
		return s;
	}
	else {
		s.code = ERROR;
	}
	return s;
}

/**
 * scan a Column at some positions specified
 * f:   pointer to the comparator containing all conditions
 * col: pointer to the Column to be scaned
 * pos: pointer to a Result as positions specified
 * r:   address of a pointer to Result to put to the output
 */
status col_scan_with_pos(comparator *f, Result *res, Result *pos, Result **r) {
	status s;
	if (NULL != res && NULL != pos) {
		*r = malloc(sizeof(Result));
		(*r)->token = NULL;
		(*r)->num_tuples = 0;
		uint i = 0;
		while (i < pos->num_tuples) {
			// if (compare(f, (col->data)->content[pos->token[i].pos])) {
			if (compare(f, res->token[i].val)) {    
				// TODO: Results storing needs improving later
				(*r)->num_tuples++;
				(*r)->token = realloc(((*r)->token), (*r)->num_tuples * sizeof(Payload));
				(*r)->token[(*r)->num_tuples - 1].pos = pos->token[i].pos;
			}
			i++;
		}
		s.code = OK;
		log_info("col_scan_with_pos %u tuple qualified\n", (*r)->num_tuples);
		return s;
	}

	s.code = ERROR;
	return s;
}

/**
 * fetch the value from a Column at positions specified
 * col: pointer to the Column to be scaned
 * pos: pointer to a Result as positions specified
 * r:   address of a pointer to Result to put to the output
 */
status fetch_val(Column *col, Result *pos, Result **r) {
	status s;
	if (NULL != col && NULL != pos) {
		*r = malloc(sizeof(Result));
		(*r)->num_tuples = pos->num_tuples;
		uint i = 0;
		(*r)->token = malloc((*r)->num_tuples * sizeof(Payload));
		log_info("fetched data:\n");
		while (i < pos->num_tuples) {
			(*r)->token[i].val = (col->data)->content[pos->token[i].pos];
			log_info(" %d\n", (*r)->token[i].val);
			i++;
		}
		s.code = OK;
		return s;
	}
	s.code = ERROR;
	return s;
}

/**
 * tuple the values requested
 * return a char array of integers separated by \n
 */
char* tuple(db_operator *query) {
	if (NULL != query && NULL != (query->domain).res[0]) {
		Result *r = (query->domain).res[0];
		uint total_space = 1024;
		uint used_space = 1, i = 0;
		char *ret = NULL;
		ret = realloc(ret, sizeof(char) * total_space);
		ret[0] = '\0';
		
		for (i = 0; i < r->num_tuples; i++) {
			char num[25];
			sprintf(num, "%d\n", r->token[i].val);
			used_space += strlen(num);
			if (used_space > total_space){
				total_space *= 2;   
				ret = realloc(ret, sizeof(char) * total_space);
			}
			strncat(ret, num, strlen(num));
		}
		return ret;     
	}
	return  NULL;
}


/** update operation, client interface
 * update old_v in col to new_v
 * col:		the Column pointer
 * old_v:	point query value
 * new_v:	the new updated value
 */
status update_with_pointQuery(Column *col, int old_v, int new_v) {
	status s;
	// perform the point query first...
	Result *pos = NULL;
	col_point_query(col, old_v, &pos);
	// then delete with a position vector
	if (pos->num_tuples > 0){
		// s = update_with_pos(col, new_v, pos);
		log_info("updating %d to %d\n", old_v, new_v);
	}
	else {
		log_info("no tuple satisfies the point query, nothing to update!\n");
		s.code = OK;
		return s;
	}
	// cleanup
	free(pos->token);
	free(pos);
	return s;
}

/** deletion operation, client interface
 * delete in tbl where col equals to val
 * tbl:	the Table pointer
 * col:	the Column pointers
 * val: point query value
 */
status delete_with_pointQuery(Table *tbl, Column *col, int val) {
	status s;
	// perform the point query first...
	Result *pos = NULL;
	col_point_query(col, val, &pos);
	// then delete with a position vector
	if (pos->num_tuples > 0){
		s = delete_with_pos(tbl, pos);
		if (OK != s.code) {
			log_err("cannot delete from Column %s at value %d\n", col->name, val);
		}
	}
	else {
		free(pos);
		log_info("no tuple satisfies the point query, nothing to delete!\n");
		s.code = OK;
		return s;
	}
	// clean up intermediate position vector
	free(pos->token);
	free(pos);
	return s;
}

/**
 * TODO: the fuction is implemented based on the assumption that all positions reside within a single partition,
 * TODO: which might not be true when the position vector is selected in a unpartitioned Column, i.e. bug will occur
 * update some tuple in col to val as pos specifies
 * col:	Column ppointer
 * val:	updated value
 * pos:	position vector
 */
// status update_with_pos(Column *col, int val, Result *pos) {
// 	status s;
// 	size_t partition_to_update = 0;
// 	size_t total_update = pos->num_tuples;
// 	DArray_INT *arr = col->data;
// 	if (pos->token[pos->num_tuples - 1].pos >= arr->length) {
// 		log_err("delete array boundary verflow!\n");
// 		s.code = ERROR;
// 		return s;
// 	}
// 	s.code = OK;
// 	return s;
// }

/** 
 * TODO: the function is implemented based on the assumption that all positions reside within a single partition,
 * TODO: which might not be true when the position vector is selected in a unpartitioned Column, i.e. bug will occur
 * delete some tuples within a table, with a position vector specifies
 * tbl: 	pointer to the Table to perform the delete
 * pos: 	specified position vector, this vector of position 
 * is supposed to be within a partition
 */
status delete_with_pos(Table *tbl, Result *pos) {
	// find the partitioned Column
	// do the deletion on the partitioned Column first
	// do the deletion afterwards on other Columns, could be in parallel
	status s;
	size_t partition_to_delete = 0;
	uint total_delete = pos->num_tuples;
	pos_t *swap_position_record_from = malloc(sizeof(pos_t) * total_delete);
	pos_t *swap_position_record_to = malloc(sizeof(pos_t) * total_delete);
	Column *partitionedCol = tbl->primary_indexed_col;
	DArray_INT *arr = partitionedCol->data;
	// Find the partition to perform the deletion
	if (pos->token[pos->num_tuples - 1].pos >= arr->length) {
		log_err("delete array boundary verflow!\n");
		s.code = ERROR;
		return s;
	}
	while (partition_to_delete < partitionedCol->partitionCount 
		&& partitionedCol->p_pos[partition_to_delete] < pos->token[0].pos) {
		partition_to_delete++;
	}

	log_info("deletion in partition %zu\n", partition_to_delete);
	pos_t head = 0;
	// pos_t beg = (partition_to_delete == 0)? 0: partitionedCol->p_pos[partition_to_delete - 1] + 1;
	#ifdef GHOST_VALUE
	pos_t end = partitionedCol->p_pos[partition_to_delete] - partitionedCol->ghost_count[partition_to_delete];
	#else
	pos_t end = partitionedCol->p_pos[partition_to_delete];
	#endif
	pos_t last_item_to_delete = total_delete - 1;
	uint delete_item_count = 0;

	// move the data specified by pos to the end of the partition
	while (delete_item_count < total_delete) {
		while (end == pos->token[last_item_to_delete].pos && delete_item_count < total_delete) {
			#ifdef GHOST_VALUE
			arr->content[end] = NON_QUALIFYING_INT;
			#endif
			// no need to swap, yet mark it for other columns
			swap_position_record_from[delete_item_count] = end;
			swap_position_record_to[delete_item_count] = end;
			end--;
			last_item_to_delete--;
			delete_item_count++;
		}
		if (delete_item_count < total_delete) {
			arr->content[pos->token[head].pos] = arr->content[end];
			#ifdef GHOST_VALUE
			arr->content[end] = NON_QUALIFYING_INT;
			#endif
			swap_position_record_from[delete_item_count] = end;
			swap_position_record_to[delete_item_count] = pos->token[head].pos;
			end--;
			head++;
			delete_item_count++;
		}
	}
	#ifdef GHOST_VALUE
	partitionedCol->ghost_count[partition_to_delete] += total_delete;
	delete_other_cols(tbl, swap_position_record_from, swap_position_record_to, total_delete);
	#else // GHOST_VALUE NOT DEFINED
	// Move data from other partitions
	int *dst = &(arr->content[partitionedCol->p_pos[partition_to_delete] - total_delete + 1]);
	int *src = NULL;
	size_t i = partition_to_delete;
	for (; i < partitionedCol->partitionCount - 1; i++) {
		int num_cpy = total_delete;
		int dest_inc = partitionedCol->p_pos[i + 1] - partitionedCol->p_pos[i];
		if (dest_inc < num_cpy){
			num_cpy = dest_inc;
		}
		// TODO: may trigger bug when a partition is empty, i.e. num_cpy = 0.. depends on how memcpy behaves when n = 0
		src = &(arr->content[partitionedCol->p_pos[i + 1] - num_cpy + 1]);
		memmove(dst, src, sizeof(int) * num_cpy);
		// move the holes to next partition
		dst += dest_inc;
		// decrease the boundary of the current partition
		partitionedCol->p_pos[i] -= total_delete;
	}
	// decrease the boundary of last partition
	partitionedCol->p_pos[i] -= total_delete;
	// decrease the size of the whole array
	arr->length -= total_delete;
	delete_other_cols(tbl, swap_position_record_from, swap_position_record_to, total_delete, partition_to_delete);
	#endif /* GHOST_VALUE */
	debug("partition %zu after deletion:\n", partition_to_delete);
	size_t k = partition_to_delete == 0? 0: partitionedCol->p_pos[partition_to_delete - 1] + 1;
	for (; k <= partitionedCol->p_pos[partition_to_delete]; k++) {
		printf("rid %zu: ", k);
		for (size_t j = 0; j < tbl->col_count; j++) 
			printf("%d ", tbl->cols[j]->data->content[k]);
		printf("\n");
	}
	tbl->length -= total_delete;
	s.code = OK;
	return s;
}

/**
 * delete in other Columns within the table
 * tbl: 				the Table to delete
 * from:				array of position specify the destination
 * to:					array of position specify the source
 * total_delete:		total number of tuples to delete
 * partition_to_delete:	index of the partion to perform the deletion, if no GHOST_VALUE defined
 */
#ifdef GHOST_VALUE
status delete_other_cols(Table *tbl, pos_t *from, pos_t *to, uint total_delete)
#else 
status delete_other_cols(Table *tbl, pos_t *from, pos_t *to, uint total_delete, size_t partition_to_delete)
#endif
{
	status s;
	Column *partitionedCol = tbl->primary_indexed_col;
	// for each Colunm in the table but the partitioned one
	for (uint k = 0; k < tbl->col_count; k++) {
		Column *tmp_col = tbl->cols[k];
		if (tmp_col == partitionedCol) continue;
		DArray_INT *arr = tmp_col->data;
		// do swap within the partition
		for (uint i = 0; i < total_delete; i++) {
			arr->content[to[i]] = arr->content[from[i]];
			#ifdef GHOST_VALUE
			arr->content[from[i]] = NON_QUALIFYING_INT;
			#endif /* GHOST_VALUE */
		}
		#ifndef GHOST_VALUE	
		// the destition of memcpy is the 
		int *dst = &(arr->content[partitionedCol->p_pos[partition_to_delete] + 1]);
		int *src = NULL;
		for (size_t i = partition_to_delete; i < partitionedCol->partitionCount - 1; i++) {
			uint num_cpy = total_delete;
			uint dest_inc = partitionedCol->p_pos[i + 1] - partitionedCol->p_pos[i];
			if (dest_inc < num_cpy){
				num_cpy = dest_inc;
			}
			// source address from the next partition
			src = &(arr->content[partitionedCol->p_pos[i + 1]+ total_delete - num_cpy + 1]);
			memcpy(dst, src, sizeof(int) * num_cpy);
			// Move the holes to next partition
			dst += dest_inc;
		}
		#endif /* GHOST_VALUE NOT DEFINED */
		arr->length -= total_delete;
	}
	s.code = OK;
	return s;
}

/**
 * insert a tuple into a Table
 * tbl: 	pointer to the Table to insert
 * cols:	array of Column pointers in the Table
 * str: 	array of integers to insert as a tuple
 */
status insert_tuple(Table *tbl, int *vals) {
	// find the partitioned Column in the table
	// this is relational
	// do the insertion on the partition Column first
	// do the insert afterwards on other Columns, could be in parallel
	status s;
	Column *partitionedCol = tbl->primary_indexed_col;
	DArray_INT *arr = partitionedCol->data;
	size_t partitionedCol_index = 0;
	for (; partitionedCol_index < tbl->col_count; partitionedCol_index++) {
		if (tbl->cols[partitionedCol_index] == partitionedCol) break;
	}
	// Find the partition to insert
	size_t partition_to_insert = 0;
	for (; partition_to_insert < partitionedCol->partitionCount; partition_to_insert++)
		if (partitionedCol->pivots[partition_to_insert] >= vals[partitionedCol_index]) break;

	log_info("insertion in partition %zu\n", partition_to_insert);
	#ifdef GHOST_VALUE
	// stealing values if necessary, breadth first search
	size_t *search_queue = malloc(sizeof(size_t) * 5);
	int head = 0, tail = 0;
	bool reach_end = false;
	size_t current_par = partition_to_insert;
	while (0 == partitionedCol->ghost_count[current_par]) {
		// add the left partition
		if (current_par <= partition_to_insert && current_par > 0) {
			search_queue[tail++] = current_par - 1;
			tail %= 5;
		}
		// add the right partition
		if (current_par >= partition_to_insert && current_par < partitionedCol->partitionCount - 1) {
			search_queue[tail++] = current_par + 1;
			tail %= 5;
		} else if (current_par == partitionedCol->partitionCount - 1) {	// hits the last partition
			reach_end = true;
			break;
		}
		current_par = search_queue[head++];
		head %= 5;
	}
	free(search_queue);
	// hit the last partition and no ghost value in it
	if (reach_end) {
		darray_push(arr, 0);	// add a single hole into the array
		partitionedCol->ghost_count[current_par] += 1; 
		partitionedCol->p_pos[current_par] += 1;
	}
	// start stealing 
	size_t partition_to_steal = current_par;
	if (current_par >= partition_to_insert) {
		// insert_pos points to the first ghost value in the current_partition
		pos_t insert_pos = partitionedCol->p_pos[current_par] - partitionedCol->ghost_count[current_par] + 1;
		partitionedCol->ghost_count[current_par]--;
		// following while will not execute if no need to steal, i.e. current_par == partition_to_insert
		while (current_par > partition_to_insert) {		// steal the hole forwards
			// from points to the fisrt number in current_partition
			pos_t from = ++partitionedCol->p_pos[current_par - 1];
			// move data from the head to the end, then the head becomes the end of the left partition
			arr->content[insert_pos] = arr->content[from];
			current_par--;
			insert_pos = from;
		}
		arr->content[insert_pos] = vals[partitionedCol_index];
	}
	else { 
		// insert_pos points to the last ghost value in the current_partition
		pos_t insert_pos = partitionedCol->p_pos[current_par]--;
		partitionedCol->ghost_count[current_par]--;
		while (current_par < partition_to_insert) { 	// steal the hole backwards
			// from points to the end of the current_partition
			pos_t from = partitionedCol->p_pos[current_par + 1]--;
			// move data from the end to the head, then the end becomes the head of the right partition
			arr->content[insert_pos] = arr->content[from];
			current_par++;
			insert_pos = from;
		}
		// insert at the end of this partition
		arr->content[insert_pos] = vals[partitionedCol_index];
		partitionedCol->p_pos[current_par]++;
	}
	insert_other_cols(tbl, vals, partition_to_insert, partition_to_steal);
	#else
	darray_push(arr, 0);	// make sure there is enough space
	// insert_pos points to the hole(end) in the partition i
	uint i = partitionedCol->partitionCount - 1;
	pos_t insert_pos = ++partitionedCol->p_pos[i];
	for (; i > partition_to_insert; i--) {
		// from points to the fisrt number in partition i
		pos_t from = ++partitionedCol->p_pos[i - 1];
		// move the data at the head to the end, thus the head becomes a hole
		arr->content[insert_pos] = arr->content[from];
		insert_pos = from;
	}
	arr->content[insert_pos] = vals[partitionedCol_index];
	// insert vals in other Columns
	insert_other_cols(tbl, vals, partition_to_insert);
	#endif /* GHOST_VALUE */
	tbl->length += 1;

	debug("partition %zu after insertion:\n", partition_to_insert);
	size_t k = partition_to_insert == 0? 0: partitionedCol->p_pos[partition_to_insert - 1] + 1;
	for (; k <= partitionedCol->p_pos[partition_to_insert]; k++) {
		printf("rid %zu: ", k);
		for (size_t j = 0; j < tbl->col_count; j++) 
			printf("%d ", tbl->cols[j]->data->content[k]);
		printf("\n");
	}

	s.code = OK;
	return s;
}

/**
 * insert in other Columns within the table
 * tbl: 				the Table to delete
 * partition_to_insert:
 * vals:				the tuple to insert
 * partition_to_steal:	
 */
#ifdef GHOST_VALUE
status insert_other_cols(Table *tbl, int *vals, size_t partition_to_insert, size_t partition_to_steal)
#else
status insert_other_cols(Table *tbl, int *vals, size_t partition_to_insert)
#endif
{
	status s;
	Column *partitionedCol = tbl->primary_indexed_col;
	// for each Colunm in the table except the partitioned one
	for (uint k = 0; k < tbl->col_count; k++) {
		Column *tmp_col = tbl->cols[k];
		if (tmp_col == partitionedCol) continue;
		DArray_INT *arr = tmp_col->data;
		#ifdef GHOST_VALUE
		if (partition_to_steal >= partition_to_insert) {
			// insert_pos points to the first hole in the partition_to_steal
			pos_t insert_pos = partitionedCol->p_pos[partition_to_steal] - partitionedCol->ghost_count[partition_to_steal];
			// following while will not execute if no need to steal, i.e. partition_to_steal == partition_to_insert
			while (partition_to_steal > partition_to_insert) {		// steal the hole forwards
				// from points to the fisrt number in partition_to_steal
				pos_t from = partitionedCol->p_pos[partition_to_steal - 1];
				// move data from the head to the end, then the head becomes the end of the left partition
				arr->content[insert_pos] = arr->content[from];
				partition_to_steal--;
				insert_pos = from;
			}
			arr->content[insert_pos] = vals[k];
		}
		else {
			// puls one to get the original last ghost value hole in the partition_to_steal
			pos_t insert_pos = partitionedCol->p_pos[partition_to_steal] + 1;
			pos_t from = partitionedCol->p_pos[partition_to_steal + 1] + 1;
			while (partition_to_steal < partition_to_insert - 1) {		// steal the hole forwards
				// from points to the original fisrt number in partition_to_steal
				from = partitionedCol->p_pos[partition_to_steal + 1] + 1;
				arr->content[insert_pos] = arr->content[from];
				partition_to_steal++;
				insert_pos = from;
			}
			// the inserting partition does not change the partition boundary
			from--;
			arr->content[insert_pos] = arr->content[from];
			insert_pos = from;
			arr->content[insert_pos] = vals[k];
		}
		#else
		for (size_t i = partitionedCol->partitionCount - 1; i > partition_to_insert; i--) {
			arr->content[partitionedCol->p_pos[i]] = arr->content[partitionedCol->p_pos[i - 1]];
		}
		arr->content[partitionedCol->p_pos[partition_to_insert]] = vals[k];
		#endif
	}
	s.code = OK;
	return s;
}

/**
 * scan a whole partition in a Column with partition id
 * col:     pointer to the Column to be scaned
 * part_id: partition id specified
 * r:       address of a pointer to Result to put to the output
 */
status scan_partition(Column *col, size_t part_id, Result **r) {
	status s;
	s.code = ERROR;
	if (NULL != col && part_id < col->partitionCount) {
		pos_t pos_s = (part_id == 0)?0 :col->p_pos[part_id - 1] + 1;
		pos_t pos_e = col->p_pos[part_id + 1]; 
		*r = malloc(sizeof(Result));
		(*r)->num_tuples = pos_e - pos_s + 1;
		uint j = 0;
		(*r)->token = malloc((*r)->num_tuples * sizeof(Payload));
		for (pos_t i = pos_s; i < pos_e; i++) {
			(*r)->token[j++].pos = i;
		}
		s.code = OK;
	}
	return s;
}

/**
 * scan a partition in a Column with greaterThan condition
 * col:     pointer to the Column to be scaned
 * val:     all qualified integers should be greater than val
 * part_id: partition id specified
 * r:       address of a pointer to Result to put to the output
 */
status scan_partition_greaterThan(Column *col, int val, size_t part_id, Result **r) {
	status s;
	s.code = ERROR;
	if (NULL != col && part_id < col->partitionCount) {
		pos_t pos_s;
		if (1 == col->partitionCount) {
			pos_s = 0;
		}
		else {
			pos_s = (part_id == 0)?0 :col->p_pos[part_id - 1] + 1;
		}
		pos_t pos_e = col->p_pos[part_id];
		*r = malloc(sizeof(Result));
		(*r)->num_tuples = 0;
		uint j = 0;
		for (pos_t i = pos_s; i < pos_e; i++) {
			if (col->data->content[i] > val) {
				// TODO: reallocate it in better way
				(*r)->token = realloc((*r)->token, sizeof(Payload) * (j + 1));
				(*r)->token[j].pos = i;
				j++;
			}
		}
		(*r)->num_tuples = j + 1;
		s.code = OK;
	}
	return s;
}

/**
 * scan a partition in a Column with lessThan condition
 * col:     pointer to the Column to be scaned
 * val:     all qualified integers should be less than val
 * part_id: partition id specified
 * r:       address of a pointer to Result to put to the output
 */
status scan_partition_lessThan(Column *col, int val, size_t part_id, Result **r) {
	status s;
	s.code = ERROR;
	if (NULL != col && part_id < col->partitionCount) {
		// start position
		pos_t pos_s = 0;
		if (1 == col->partitionCount) {
			pos_s = 0;
		}
		else {
			pos_s = (part_id == 0)?0 :col->p_pos[part_id - 1] + 1;
		}
		// end position
		pos_t pos_e = col->p_pos[part_id];
		*r = malloc(sizeof(Result));
		(*r)->num_tuples = 0;
		uint j = 0;
		for (pos_t i = pos_s; i < pos_e; i++) {
			if (col->data->content[i] < val) {
				// TODO: reallocate it in better way
				(*r)->token = realloc((*r)->token, sizeof(Payload) *(j + 1));
				(*r)->token[j].pos = i;
				j++;
			}
		}
		(*r)->num_tuples = j + 1;
		s.code = OK;
	}
	return s;
}

/**
 * scan a partition in a Column with equal condition
 * col:     pointer to the Column to be scaned
 * val:     all qualified integers should be equal to val
 * part_id: partition id specified
 * r:       address of a pointer to Result to put to the output
 */
status scan_partition_pointQuery(Column *col, int val, size_t part_id, Result **r) {
	status s;
	s.code = ERROR;
	if (NULL != col && part_id < col->partitionCount) {
		pos_t pos_s = (part_id == 0)?0 :col->p_pos[part_id - 1] + 1;
		pos_t pos_e = col->p_pos[part_id]; 
		*r = malloc(sizeof(Result));
		(*r)->partitionNum = malloc(sizeof(size_t));
		(*r)->partitionNum[0] = part_id;
		(*r)->num_tuples = 0;
		uint j = 0;
		for (pos_t i = pos_s; i < pos_e; i++) {
			if (col->data->content[i] == val) {
				(*r)->token = realloc((*r)->token, sizeof(Payload) * (j + 1));
				(*r)->token[j].pos = i;
				j++;
			}
		}
		(*r)->num_tuples = j + 1;
		s.code = OK;
	}
	return s;
}
