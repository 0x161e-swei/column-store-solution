#include "table.h"


status create_table(db* db, const char* name, size_t num_columns, table** table) {
	status s;
	if (db != NULL) {
		if (*table == NULL) {
			*table = malloc(sizeof(table));
		}

		(*table)->name = name;
		(*table)->col_count = num_columns;
		(*table)->length = 0;
		// Store the pointers to the columns in this table
		(*table)->cols = (column **) malloc(num_columns * sizeof(column *));
		for (size_t i = 0; i < num_columns; i++) {
			(*table)->cols[i] = NULL;
		}

		// Add the table to the database table hash list
		HASH_ADD_KEYPTR(hh, db->tables, ((*table)->name), strlen((*table)->name), (*table));

		s.code = OK;
	}
	else {
		s.code = ERROR;
	}

	return s;
}

// status drop_table(db* db, table* table) {

// }

// status load_data(db* db, table* table, char *filename) {

// }