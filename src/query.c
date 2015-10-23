#include "query.h"

Result *res_hash_list;

status grab_result(const char *res_name, Result **res) {
	status s;
	if (NULL == res_hash_list) {
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
			s.code = ERROR;
		}
	}
	return s;
}

status clear_res_list() {
	status ret;
	if (NULL == res_hash_list) {
		ret.code = OK;
		return ret;
	}
	else {
		log_info("clearing the res_hash_list");
		Result *tmp, *res;
		HASH_ITER(hh, res_hash_list, res, tmp) {			
			if (NULL != res) {
				HASH_DEL(res_hash_list, res);
				free((void *)res->res_name);
				free(res->token);
				free(res);
			}
		}
	}
	ret.code = OK;
	return ret;
}

status query_prepare(const char* query, dsl* d, db_operator* op) {
	char open_paren[2] = "(";
	char close_paren[2] = ")";
	char comma[2] = ",";
	// char quotes[2] = "\"";
	char eq_sign[2] = "=";
	status s;

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

		// This gives us <col_var>
		char* col_var = strtok(args, comma);

		char* low_str = strtok(NULL, comma);
		int low = 0;
		if (NULL != low_str) {
			low = atoi(low_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("wrong select format");
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
			log_err("wrong select format");
			return s;	
		}
		(void) high;
		log_info("%s=select(%s,%d,%d)", pos_var, col_var, low, high);

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
		printf("table name in select %s\n", tbl_var);
		
		Table* tmp_tbl = NULL;
		s = grab_table(tbl_var, &tmp_tbl);
		if (OK != s.code) {
			log_err("cannot grab the table!");
			return s;
		}
		free(tbl_var);

		// Grab the column
		Column *tmp_col = NULL;
		s = grab_column(col_var, &tmp_col);

		if (OK != s.code) {
			log_err("cannot grab the column!\n");
			return s;
		}

		op->type = SELECT_COL;
		op->tables = malloc(sizeof(Tbl_ptr));
		op->tables[0] = tmp_tbl;

		(op->domain).cols = malloc(sizeof(Col_ptr));
		(op->domain).cols[0] = tmp_col;

		op->pos1 = NULL;
		op->pos2 = NULL;
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

		// This gives us <posn_vec> 
		char* posn_vec = strtok(args, comma);

		// This gives us <col_var>
		char* col_var = strtok(NULL, comma);

		char* low_str = strtok(NULL, comma);
		int low = 0;
		if (NULL != low_str) {
			low = atoi(low_str);
		}
		else {
			s.code = WRONG_FORMAT;
			log_err("wrong select format");
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
			log_err("wrong select format");
			return s;	
		}
		(void) high;

		log_info("%s=select(%s,%s,%d,%d)", pos_var, posn_vec, col_var, low, high);

		// Grab the position list
		Result *tmp_pos = NULL;
		s = grab_result(posn_vec, &tmp_pos);

		if (OK != s.code) {
			log_err("cannot grab the position!\n");
			return s;
		}

		// Grab the sub column value list
		Result *tmp_val = NULL;
		s = grab_result(col_var, &tmp_val);
		if (OK != s.code) {
			log_err("cannot grab the value!\n");
			return s;
		}

		op->type = SELECT_PRE;
		op->tables = NULL;

		(op->domain).res = malloc(sizeof(Res_ptr));
		(op->domain).res[0] = tmp_val;
		op->position = tmp_pos;

		op->pos1 = NULL;
		op->pos2 = NULL;
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
		// TODO:
		char* str_cpy = malloc(strlen(query) + 1);
		strncpy(str_cpy, query, strlen(query) + 1);
		char* res_name = strtok(str_cpy, eq_sign);

		char* val_var = malloc(sizeof(char) * (strlen(res_name) + 1));
		strncpy(val_var, res_name, strlen(res_name) + 1); 
		strtok(NULL, open_paren);

		// This gives us everything inside the '(' ')'
		char* args = strtok(NULL, close_paren);

		// THis gives us <col_var>
		char* col_var = strtok(args, comma);

		// This gives us <vec_pos>
		char* vec_pos = strtok(NULL, comma);

		log_info("%s=fetch(%s,%s)",val_var, col_var, vec_pos);

		Column* tmp_col = NULL;
		s = grab_column(col_var, &tmp_col);
		if (OK != s.code) {
			log_err("cannot grab the column!");
			return s;
		}

		Result* tmp_res = NULL;
		s = grab_result(vec_pos, &tmp_res);
		if (OK != s.code) {
			log_err("cannot grab the position!");
			return s;
		}

		op->type = FETCH;
		op->tables = NULL;

		

		(op->domain).cols = malloc(sizeof(Col_ptr));
		(op->domain).cols[0] = tmp_col;
		op->position = tmp_res;

		op->pos1 = NULL;
		op->pos2 = NULL;
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
		op->pos1 = NULL;
		op->pos2 = NULL;
		op->value1 = NULL;
		op->value2 = NULL;
		op->res_name = NULL;
		op->c = NULL;
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

status query_execute(db_operator* op, Result** results) {
	status s;
	switch (op->type) {
		case SELECT_COL: {
			s = col_scan(op->c, (op->domain).cols[0], (op->tables[0])->length, 
				results);
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
			s = col_scan_with_pos(op->c, (op->domain).cols[0], op->position, 
				results);
			if (OK != s.code) {
				// Something Wroing
				return s;
			}
			(*results)->res_name = op->res_name;
			HASH_ADD_KEYPTR(hh, res_hash_list, ((*results)->res_name), 
				strlen((*results)->res_name), *results);
			break;
		}
		case FETCH: {
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
		default:
			break;
	}
	s.code = ERROR;
	return s;
}

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

status col_scan(comparator *f, Column *col, size_t len, Result **r) {
	status s;
	if (NULL != col) {
		*r = malloc(sizeof(Result));
		(*r)->token = NULL;
		(*r)->num_tuples = 0;
		for (size_t i = 0; i < len; i++) {
			if (compare(f, col->data[i])) {
				(*r)->num_tuples++;
				(*r)->token = realloc((*r)->token, (*r)->num_tuples * sizeof(Payload));
				(*r)->token[(*r)->num_tuples - 1].pos = i;
			}
		}
		s.code = OK;
		log_info("col_scan %zu tuple qualified", (*r)->num_tuples);
		for (size_t ii = 0; ii < (*r)->num_tuples; ii++) {
			log_info("pos selected %zu ", (*r)->token[ii].pos);
		}
		return s;
	}

	s.code = ERROR;
	return s;
}

status col_scan_with_pos(comparator *f, Column *col, Result *pos, Result **r) {
	status s;
	if (NULL != col && NULL != pos) {
		*r = malloc(sizeof(Result));
		(*r)->token = NULL;
		(*r)->num_tuples = 0;
		size_t i = 0;
		while (i < pos->num_tuples) {
			if (compare(f, col->data[pos->token[i].pos])) {
				(*r)->num_tuples++;
				(*r)->token = realloc(((*r)->token), (*r)->num_tuples * sizeof(Payload));
				(*r)->token[(*r)->num_tuples - 1].pos = pos->token[i].pos;
			}
			i++;
		}
		s.code = OK;
		log_info("col_scan_with_pos %zu tuple qualified", (*r)->num_tuples);
		return s;
	}

	s.code = ERROR;
	return s;
}


status fetch_val(Column *col, Result *pos, Result **r) {
	status s;
	if (NULL != col && NULL != pos) {
		*r = malloc(sizeof(Result));
		(*r)->num_tuples = pos->num_tuples;
		size_t i = 0;
		(*r)->token	= malloc((*r)->num_tuples * sizeof(Payload));
		log_info("fetched data:\n");
		while (i < pos->num_tuples) {
			(*r)->token[i].val = col->data[pos->token[i].pos];
			log_info(" %d ", (*r)->token[i].val);
			i++;
		}
		s.code = OK;
		return s;
	}

	s.code = ERROR;
	return s;
}

char* tuple(db_operator *query){
	if (NULL != query && NULL != (query->domain).res[0]) {
		Result *r = (query->domain).res[0];
		size_t allocated_size = 1, i = 0;
		char *ret = NULL;
		ret = realloc(ret, sizeof(char) * allocated_size);
		ret[0] = '\0';
		for (i = 0; i < r->num_tuples; i++) {
			char num[20];
			sprintf(num, "%d\n", r->token[i].val);
			allocated_size += strlen(num);
			ret = realloc(ret, sizeof(char) * allocated_size);				
			strncat(ret, num, strlen(num));
		}
		return ret;		
	}
	return	NULL;
}