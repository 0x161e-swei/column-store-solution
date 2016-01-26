#include "column.h"

// Global column hash list, all in one
Column *col_hash_list;


/**
 * Create a new Column in the Table
 * table: 		pointer to the Table that the ctreated Column belongs to
 * name:		the name of the Column to create
 * col: 		address of the pointer that the Column was put in
 * Return 		the status.code as OK if Table created successfully
 */
status create_column(Table *table, const char* name, Column** col) {
	status s;
	if (NULL != table) {
		if (NULL == (*col)) {
			(*col) = malloc(sizeof(Column));
		}

		(*col)->name = malloc(sizeof(char) * (strlen(name) + 1));
		strcpy((char *)(*col)->name, name);
		(*col)->data = NULL;
		(*col)->index = NULL;
		(*col)->partitionCount = 1;
		(*col)->pivots = NULL;
		(*col)->p_pos = NULL;
		(*col)->isDirty = false;
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

/**
 * Grab the Column from the hash list
 * table_name: 	the name of the column
 * col: 		address of the pointer to the grabed Column
 * Return 		the status.code as OK if Column found
 */
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