#ifndef QUERY_H__
#define QUERY_H__

#include "cs165_api.h"
#include "column.h"
#include "table.h"
#include "db.h"
#include <string.h>

#define NON_QUALIFYING_INT 66000

extern char open_paren[2];
extern char close_paren[2];
extern char comma[2];
// extern char quotes[2];
extern char eq_sign[2];

status clear_res_list();
status grab_result(const char *res_name, Result **res);
bool compare(comparator *f, int val);
status load_column4disk(Column *col, size_t len);

status col_point_query(Column *col, int val, Result **r);
status col_range_query(Column *col, int low, int high, Result **r);
status delete_with_pos(Table *tbl, Result *pos);
status update_with_pos(Table *tbl, Column *col, int val, Result *pos);

#ifdef GHOST_VALUE
status delete_other_cols(Table *tbl, pos_t *from, pos_t *to, uint total_delete);
status insert_other_cols(Table *tbl, int *vals, size_t partition_to_insert, size_t partition_to_steal);
status update_other_cols(Table *tbl, pos_t *from, pos_t *to, uint total_update, size_t partition_to_update, size_t partition_to_insert, size_t partition_to_steal);
#else
status delete_other_cols(Table *tbl, pos_t *from, pos_t *to, uint total_delete, size_t partition_to_delete);
status insert_other_cols(Table *tbl, int *vals, size_t partition_to_insert);
status update_other_cols(Table *tbl, pos_t *from, pos_t *to, uint total_update, size_t partition_to_update, size_t partition_to_insert);
#endif


size_t binary_search_pivots(int *pivots, size_t len, int val);
/**
 * prepare the Column together with the Table it belongs to
 * args:	a char array to parse
 * tbl:		address of a Table to hold the Table pointer 
 * col:		address of a Table to hold the Column pointer 	
 */
static inline char *prepare_col(char *args, Table **tbl, Column **col) {
	char *col_var = strtok(args, comma);

	// unsigned int i =0, flag = 0;
	// while('\0' != col_var[i]) {
	// 	if ('.' == col_var[i]) {
	// 		flag++;
	// 		if (2 == flag) {		// Find the second '.'
	// 			break;
	// 		}
	// 	}
	// 	i++;
	// }

	// Find the second/last dot in the Column name in order to find the table name
	const char *second_dot = strrchr(col_var, '.');
	int i = (int)(second_dot - col_var);

	char* tbl_var = malloc(sizeof(char) * (i + 1));
	strncpy(tbl_var, col_var, i);
	tbl_var[i] = '\0';
	debug("table name %s\n", tbl_var);
	
	Table* tmp_tbl = NULL;
	status s = grab_table(tbl_var, &tmp_tbl);
	if (OK != s.code) {
		log_err("cannot grab the table!");
		return NULL;
	}
	free(tbl_var);
	*tbl = tmp_tbl;	

	// Grab the column
	Column *tmp_col = NULL;
	s = grab_column(col_var, &tmp_col);

	if (OK != s.code) {
		log_err("cannot grab the column!\n");
		return NULL;
	}

	// // Data of the column might not be in main memory
	// if (NULL != tmp_tbl && NULL == tmp_col->data && 0 != tmp_tbl->length) {
	// 	load_column4disk(tmp_col, tmp_tbl->length);
	// }
	*col = tmp_col;

	return col_var + strlen(col_var) + 1;	
}

/**
 * prepare the Result together with the Table it belongs to
 * args:	a char array to parse
 * res:		address of a Table to hold the Result pointer 
 */
static inline char *prepare_res(char *args , Result **res) {
	char *vec_res = strtok(args, comma);

	// Grab the position list
	Result *tmp_res = NULL;
	status s = grab_result(vec_res, &tmp_res);
	if (OK != s.code) {
		log_err("cannot grab the results!\n");
		return NULL;
	}
	*res = tmp_res;
	return vec_res + strlen(vec_res) + 1;
}



// extern Result *res_hash_list;

#endif  // QUERY_H__