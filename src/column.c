#include "column.h"

// Global column hash list
column *col_hash_list;

status create_column(table *table, const char* name, column** col) {
	status s;
	if (table != NULL) {
		if (*col == NULL) {
			(*col) = malloc(sizeof(column));
		}

		(*col)->name = name;
		(*col)->data = NULL;
		(*col)->index = NULL;

		size_t i = 0;
		for (; i < table->col_count; i++){
			if (table->cols[i] == NULL) break;
		}

		if (i < table->col_count) {
			table->cols[i] = *col;			// Add ptr of the col to the list in table
			HASH_ADD_KEYPTR(hh, col_hash_list, ((*col)->name), strlen((*col)->name), (*col));
			s.code = OK;
		}
		else { 								// All columns have already been created
			free(*col);
			s.code = ERROR;
		}
		
	}
	else {
		s.code = ERROR;	
	}
	
	return s;
}