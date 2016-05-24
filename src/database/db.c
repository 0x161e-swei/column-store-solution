#include "db.h"
#include "column.h"
#include "query.h"


// TODO(USER): Here we provide an incomplete implementation of the create_db.
// There will be changes that you will need to include here.

// the only global db 
Db *database = NULL;
char *data_path = NULL;

status grab_db(const char* db_name, Db** db) {
	status s;
	if (NULL == database) {
		*db = NULL;
		s.code = ERROR;
	}
	else if (0 == strcmp(database->name, db_name)){
		(*db) = database;
		s.code = OK;
	}
	else {
		*db = NULL;
		s.code = ERROR;
	}
	return s;
}

status create_db(const char* db_name, Db** db __attribute__((unused))) {
	status s;
	if (NULL == database) {
		database = malloc(sizeof(Db));
		// *db = database;
	}
	else {
		s.code = ERROR;
		return s;
	}
	database->name = malloc(sizeof(char) * (strlen(db_name) + 1));
	strcpy((char*)database->name, db_name);
	// (*db)->name = db_name;
	database->table_count = 0;
	database->tables = NULL;
	s.code = OK;
	// FILE *dbinfo;
	// if (NULL != (dbinfo = fopen("dbinfo", "r+"))) {
	// 	// Basically we have only one db
	//     fclose(dbinfo);
	//     s.code = ERROR;
	// }
	// else if (NULL != (dbinfo = fopen("dbinfo", "w+"))) {
	//     int len = strlen(db_name);
		/* Write length of database name and (*db)->name*/
		// fwrite(&len, sizeof(len), 1, dbinfo);
		// fwrite(db_name, sizeof(char), len, dbinfo);
		/* Write number of table */ 
		// len = 0;
		// fwrite(&len, sizeof(len), 1, dbinfo);
		// fclose(dbinfo);
		// s.code = OK;
	// }
	// else {
		// s.code = ERROR;
	// }

	return s;
}


/**
 * Synchronize the database (now the only global one) to the disk
 *
 */
status sync_db(Db* db __attribute__((unused))) {
	FILE *dbinfo;
	status s;
	clear_res_list();
	char *dbinfo_name = malloc(sizeof(char) * strlen(data_path) + 7);
	strncpy(dbinfo_name, data_path, strlen(data_path) + 1);
	strncat(dbinfo_name, "dbinfo", 6);
	if (NULL != database && NULL != (dbinfo = fopen(dbinfo_name, "w+"))) {
		log_info("saving the database %s...\n", database->name);
		/* Write length of database name, 
			database name and table count to dbinfo
		 */
		int len = strlen(database->name);
		fwrite(&len, sizeof(len), 1, dbinfo);
		fwrite(database->name, sizeof(char), len, dbinfo);
		fwrite(&database->table_count, sizeof(database->table_count), 1, dbinfo);	// size_t

		Table *tmp, *tbl;
		HASH_ITER(hh, (database->tables), tbl, tmp) {
			log_info("\tsaving the table %s...\n", tbl->name);

			/* Write length of table name, table name, 
				table size and column count to dbinfo 
			 */
			len = strlen(tbl->name);
			fwrite(&len, sizeof(len), 1, dbinfo);
			fwrite(tbl->name, sizeof(char), len, dbinfo);
			fwrite(&(tbl->length), sizeof(tbl->length), 1, dbinfo);				// uint
			fwrite(&(tbl->col_count), sizeof(tbl->col_count), 1, dbinfo);		// size_t

			for (size_t i = 0; i < tbl->col_count; i++) {
				if (NULL != tbl->cols[i]) {
					// TODO: need to check the dirty bit of the Column
					log_info("\t\tsaving the column %s...\n", (tbl->cols[i])->name);

					// Write length of column name, column name to dbinfo
					len = strlen((tbl->cols[i])->name);
					fwrite(&len, sizeof(len), 1, dbinfo);
					fwrite((tbl->cols[i])->name, sizeof(char), len, dbinfo);
					
					// Cleanups
					HASH_DEL(col_hash_list, tbl->cols[i]);
					if (NULL != (tbl->cols[i])->data) {
						char *dataname;
						dataname = malloc(sizeof(char) * (len + strlen(data_path) + 1));
						strncpy(dataname, data_path, strlen(data_path) + 1);
						strncat(dataname, (tbl->cols[i])->name, len);
						printf("%s\n", dataname);
						// FILE *fwp = fopen((tbl->cols[i])->name, "w+");
						FILE *fwp = fopen(dataname, "w+");
						if (NULL != fwp) {
							// Write as a whole or do it one by one
							fwrite(((tbl->cols[i])->data)->content, sizeof(int), ((tbl->cols[i])->data)->length, fwp);
							// for (size_t k = 0; k < tbl->length; k++) {
							//     fprintf(fwp, "%d\n", ((tbl->cols[i])->data)->content[k]);
							//     fwrite(&((tbl->cols[i])->data)->content[k], sizeof(int), 1, fwp);
							// }
							fclose(fwp);
						}
						else {
							log_err("cannot open column file to store.\n");
						}
						free(dataname);
						// free((tbl->cols[i])->data);
						darray_destory((tbl->cols[i])->data); (tbl->cols[i])->data = NULL;
					}
					if (NULL != (tbl->cols[i])->index) {
						free((tbl->cols[i])->index);
					}
					free((void *)(tbl->cols[i])->name);
					if ((tbl->cols[i])->partitionCount > 1) {
						free((tbl->cols[i])->pivots);
						free((tbl->cols[i])->p_pos);
					}
					#ifdef SWAPLATER
					if (NULL != (tbl->cols[i])->pos) {
						free((tbl->cols[i])->pos);
					}
					#endif
					#ifdef GHOST_VALUE
					if (NULL != (tbl->cols[i])->ghost_count) {
						free((tbl->cols[i])->ghost_count);
					}
					#endif
					free(tbl->cols[i]);
				}
				else {
					log_err("\t\tbroken table\n");
				}
				
			}
			
			HASH_DEL(database->tables, tbl);
			free(tbl->cols);
			free((void *)(tbl->name));
			free(tbl);
		}

		if (NULL != database->name) free((void *)database->name);
		free(database);
		database = NULL;
		fclose(dbinfo);
		s.code = OK;
	}
	return s;
}

char* show_db() {
	if (NULL == database) {
		return NULL;
	}
	else {
		char *res = NULL;
		// allocate the size of the char array, +2 for \n\0
		size_t allocated_size = strlen(database->name) + 2;
		// Write db name
		res = malloc(allocated_size * sizeof(char));
		strncpy(res, database->name, strlen(database->name) + 1);
		strncat(res, "\n", 1);

		// Find each table in db and write tbl name & tbl length
		Table *tmp, *tbl;
		HASH_ITER(hh, (database->tables), tbl, tmp) {
			// reallocate the size of the char array, +1 for \t
			allocated_size += strlen(tbl->name) + 1;
			res = realloc(res, allocated_size * sizeof(char));
			strncat(res, "\t", 1);
			strncat(res, tbl->name, strlen(tbl->name));

			char len_s[40];
			sprintf(len_s, " length: %u", tbl->length);

			// reallocate the size of the char array, +1 for \n
			allocated_size += strlen(len_s) + 1;
			res = realloc(res, allocated_size * sizeof(char));
			strncat(res, len_s, strlen(len_s));
			strncat(res, "\n", 1);

			for (size_t i = 0; i < tbl->col_count; i++) {
				// reallocate the size of the char array, +3 for \t\t & \n
				allocated_size += strlen((tbl->cols[i])->name) + 3;
				res = realloc(res, allocated_size * sizeof(char));
				strncat(res, "\t\t", 2);
				strncat(res, (tbl->cols[i])->name, strlen((tbl->cols[i])->name));
				strncat(res, "\n", 1);
			}
		}
		return res;
	}
}

status open_db(const char* filename, Db** db, OpenFlags flags) {
	FILE *dbinfo;
	status s;
	if (LOAD == flags) {
		/* When we are loading the database we are actually 
			re-creating the database according the dbinfo again
		 */
		if (NULL == database && NULL != (dbinfo = fopen(filename, "r+"))) {
			int len, num __attribute__((unused));
			num = fread(&len, sizeof(len), 1, dbinfo);        // length of db name
			char *db_name = malloc(len * sizeof(char) + 1);
			num = fread(db_name, sizeof(char), len, dbinfo);
			db_name[len] = '\0';
			
			s = create_db(db_name, &database);
			free(db_name);

			log_info("database found: %s\n", database->name);

			/* Read the number of tables
				Caution: sizeof(size_t) is different from sizeof(int) 
			 */
			size_t table_count = 0;
			num = fread(&table_count, sizeof(size_t), 1, dbinfo);
			
			for (unsigned int i = 0; i < table_count; i++){        
				Table *t = NULL;
				num = fread(&len, sizeof(len), 1, dbinfo);
				char *tbl_name = malloc(sizeof(char) * len + 1);
				num =  fread(tbl_name, sizeof(char), len, dbinfo);
				tbl_name[len] = '\0';
				// t->name = tbl_name;
				
				/* Read the length of columns in the table
					type: uint
				 */
				size_t col_len = 0;
				num = fread(&col_len, sizeof(uint), 1, dbinfo);
				
				/* Read the number of columns in the table
					Caution: sizeof(size_t) is different from sizeof(int) 
				 */
				size_t col_count;
				num = fread(&col_count, sizeof(size_t), 1, dbinfo);
				s = create_table(database, tbl_name, col_count, &t);
				t->length = col_len;
				free(tbl_name);
				log_info("\ttable %s found with length %u and %zu columns\n", t->name, t->length, t->col_count);

				for (unsigned int j = 0; j < t->col_count; j++) {
					int col_name_len;
					num = fread(&col_name_len, sizeof(col_name_len), 1, dbinfo);
					char *col_name = malloc(sizeof(char) * col_name_len + 1);
					num = fread(col_name, sizeof(char), col_name_len, dbinfo);
					col_name[col_name_len] = '\0';
					Column *c = NULL;
					
					s = create_column(t, col_name, &c);
					if (ERROR == s.code) {
						log_err("cannot load column info of %s", col_name);
						free(col_name);
						// TODO: potential mem leaks in metadata
						return s;
					}
					free(col_name);
					
					log_info("\t\tcolumn found: %s\n", c->name);
					// // Add the content of column into the global column hash list
					// HASH_ADD_KEYPTR(hh, col_hash_list, (c->name), strlen(c->name), c);
				}
				// // Add the contect of table into hash list in database
				// HASH_ADD_KEYPTR(hh, database->tables, (t->name), strlen(t->name), t);
			}
			*db = database;
			s.code = OK;
		}
		else {
			s.code = ERROR;                             // No database currently exists
		}
	}
	else {
		// Not implemented yet
		s.code = ERROR;
	}
		
	return s;
	
}

// TODO: shutDB clean up
