#include "table.h"
#include "db.h"


/**
 * Grab the table from the hash list
 * table_name: 	the name of the table
 * tbl: 		address of the pointer to the grabed Table
 * Return 		the status.code as OK if Table found
 */
status grab_table(const char* table_name, Table** tbl) {
	status s;
	if (NULL == database) {
		*tbl = NULL;
		s.code = ERROR;
	}
	else {
		Table *tmp = NULL;
		HASH_FIND_STR(database->tables, table_name, tmp);
		if (NULL != tmp) {
			*tbl = tmp;
			s.code = OK;
		}
		else {
			*tbl = NULL;
			s.code = ERROR;
		}
	}
	return s;
}

/**
 * Create a new table in the databse db with
 * db: 			pointer to the Db that the ctreated Table belongs to
 * name:		the name of the Table to create
 * num_columns: the supposed number of columns in the created Table
 * table: 		address of the pointer that the Table was put in
 * Return 		the status.code as OK if Table created successfully
 */
status create_table(Db* db, const char* name, size_t num_columns, Table** table) {
	status s;
	if (NULL != db) {
		if (NULL == *table ) {
			*table = malloc(sizeof(Table));
		}
		(*table)->name = malloc(sizeof(char) * (strlen(name) + 1));
		strcpy((char *)(*table)->name, name);
		(*table)->col_count = num_columns;
		(*table)->length = 0;
		(*table)->primary_indexed_col = NULL;
		
		/* Store the pointers to the columns in this table */
		(*table)->cols = malloc(num_columns * sizeof(Col_ptr));

		for (size_t i = 0; i < num_columns; i++) {
			(*table)->cols[i] = NULL;
		}
		
		db->table_count++;
		
		/* Add the table to the database table hash list */
		
		HASH_ADD_KEYPTR(hh, (db->tables), ((*table)->name), strlen((*table)->name), (*table));
		log_info("table %s created\n", (*table)->name);
		s.code = OK;
	}
	else {
		s.code = ERROR;
	}

	return s;
}

status show_tbl(Table *tbl) {
	uint len = tbl->length;
	if (NULL != tbl->primary_indexed_col){
		// real length including ...extra data slots like ghost value
		len = tbl->primary_indexed_col->data->length;
		debug("partitionCount %zu\n", tbl->primary_indexed_col->partitionCount);
	}
	size_t cur_partition = 0;
	debug("show table %s data:(len %u)\n", tbl->name, tbl->length);
	for (uint i = 0; i < len; i++) {
		printf("rid %u: ", i);
		for (size_t j = 0; j < tbl->col_count; j++) {
			printf("%d\t", tbl->cols[j]->data->content[i]);
		}
		if (NULL != tbl->primary_indexed_col && NULL != tbl->primary_indexed_col->p_pos ) {
			#ifdef GHOST_VALUE
			if ( (tbl->primary_indexed_col->p_pos[cur_partition] - tbl->primary_indexed_col->ghost_count[cur_partition]) == i) {
				printf("data fence ");
			}
			#endif
			if ( tbl->primary_indexed_col->p_pos[cur_partition] == i) {
				printf("partition fence at pivot %d", tbl->primary_indexed_col->pivots[cur_partition]);
				cur_partition++;
			}
		}
		printf("\n");
	}
	status s;
	s.code = OK;
	return s;
}
// status drop_table(Db* db, Table* table) {

// }

// status load_data(Db* db, Table* table, char *filename) {

// }