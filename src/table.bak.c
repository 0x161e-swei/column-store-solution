#include "table.h"
#include "db.h"

status grab_table(const char* table_name, table** tbl) {
	status s;
	if (NULL == database) {
		*tbl = NULL;
		s.code = ERROR;
	}
	else {
		table *tmp = NULL;
		HASH_FIND_STR(database->tables, table_name, tmp);
		if (NULL != tmp) {
			*tbl = tmp;
			s.code = OK;
		}
		else {
			s.code = ERROR;
		}
	}
	return s;
}

status create_table(db* db, const char* name, size_t num_columns, table** table) {
	status s;
	if (NULL != db) {
		printf("%s\n", "hello in create table");
		if (NULL == *table ) {
			*table = malloc(sizeof(table));
		}

		(*table)->name = name;
		(*table)->col_count = num_columns;
		(*table)->length = 0;
		/* Store the pointers to the columns in this table */
		printf("%s\n", "before malloc for cols");
		(*table)->cols = malloc(num_columns * sizeof(col_ptr));
		printf("%s\n", "after malloc for cols");
		
		for (size_t i = 0; i < num_columns; i++) {
			(*table)->cols[i] = NULL;
		}
		printf("%s\n", "what's up");
		db->table_count++;
		printf("%s\n", "what's up111");
		/* Add the table to the database table hash list */

		table *tb_list, *item;
		item = *table; 
		HASH_ADD_KEYPTR(hh, (db->tables), (item->name), strlen(item->name), item);
		printf("%s\n", "what's up2222");
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