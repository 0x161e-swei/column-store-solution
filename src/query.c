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
		log_info("clearing the result hash list");
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
	return 	ret;
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

		// This gives us <col_name>
		char* col_name = strtok(args, comma);

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
		log_info("%s=select(%s,%d,%d)", pos_var, col_name, low, high);

		// Grab the column
		Column *tmp_col = NULL;
		s = grab_column(col_name, &tmp_col);

		if (OK != s.code) {
			log_err("cannot grab the column!\n");
			return s;
		}

		op->type = SELECT_COL;
		op->tables = NULL;
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
		s.code = OK;
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
		char* val_name = strtok(NULL, comma);

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

		log_info("%s=select(%s,%s,%d,%d)", pos_var, val_name, low, high);

		// Grab the position list
		Result *tmp_pos = NULL;
		s = grab_result(posn_vec, &tmp_pos);

		if (OK != s.code) {
			log_err("cannot grab the position!\n");
			return s;
		}

		// Grab the sub column value list
		Result *tmp_val = NULL;
		s = grab_result(val_name, &tmp_val);
		if (OK != s.code) {
			log_err("cannot grab the value!\n");
			return s;
		}

		op->type = SELECT_PRE;
		op->tables = NULL;

		(op->domain).res = malloc(sizeof(Res_ptr));
		(op->domain).res[0] = tmp_val;

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

		op->pos1 = NULL;
		op->pos2 = NULL;
		op->value1 = NULL;
		op->value2 = NULL;
		op->c = NULL;

		// Keep track of the name for further refering
		op->res_name = val_var;

		s.code = OK;
	}
	else {
		s.code = ERROR;
	}

	s.code = ERROR;
	return s;
}

// status query_execute(db_operator* op, Result** results) {
// 	status s;
// 	s.code = ERROR;
// 	return s;
// }
