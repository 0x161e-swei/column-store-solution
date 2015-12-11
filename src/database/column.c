#include "column.h"

// Global column hash list
Column *col_hash_list;

status create_column(Table *table, const char* name, Column** col) {
	status s;
	if (NULL != table) {
		if (NULL == (*col)) {
			(*col) = malloc(sizeof(Column));
		}
		(*col)->name = name;
		(*col)->data = NULL;
		(*col)->index = NULL;
		(*col)->partitionCount = 1;
		(*col)->pivot = NULL;
		(*col)->p_pos = NULL;

		size_t i = 0;
		for (; i < table->col_count; i++){
			if (NULL == table->cols[i]) break;
		}

		if (i < table->col_count) {
			/* Add ptr of the col to the list in table */
			table->cols[i] = *col;			
			/* Add the col itself into the global hash list */
			HASH_ADD_KEYPTR(hh, col_hash_list, ((*col)->name), strlen((*col)->name), (*col));
			s.code = OK;
		}
		else { 								// All columns have already been created
			free(*col);
			s.code = ERROR;
			log_err("table full of columns");
		}
	}
	else {
		s.code = ERROR;
	}
	
	return s;
}


status grab_column(const char* column_name, Column **col) {
	status s;
	if (NULL == col_hash_list) {
		*col = NULL;
		s.code = ERROR;
	}	
	else {
		Column *tmp = NULL;
		HASH_FIND_STR(col_hash_list, column_name, tmp);
		if (NULL != tmp) {
			*col = tmp;
			s.code = OK;
		}
		else {
			s.code = ERROR;
		}	
	}
	return s;
}