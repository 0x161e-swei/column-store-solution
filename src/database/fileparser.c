#define _GNU_SOURCE
#include <stdio.h>
#include "fileparser.h"
#include "db.h"
#include "table.h"
#include "column.h"

status parse_field(const char *fields, size_t lineCount, size_t fieldCount, Column **cols) {
	status ret;
	const char *str = fields;
	size_t i = 0;
	char *col_name;
	for (; i < fieldCount - 1; i++) {
		const char *comma = strchr(str, ',');
		int t = (int)(comma - str);
		col_name = malloc(sizeof(char) * (t + 1));
		strncpy(col_name, str, t);
		col_name[t] = '\0';
		// look for next comma
		str = comma + 1;
		cols[i]  = NULL;
		grab_column(col_name, &cols[i]);
		if (NULL != cols[i]) {
			log_info("init an array with %zu slots\n", lineCount);
			cols[i]->data = darray_create(lineCount);
			// mark length at first
			cols[i]->data->length = lineCount;
		}
		else {
			log_err("invalid column name %s!\n", col_name);
			for (int j = i - 1; j >= 0; j--) {
				darray_destory(cols[j]->data);
			}
		}
		free(col_name);
	}

	// last field
	const char *n = strchr(str, '\n');
	int t = (int)(n - str);
	col_name = malloc(sizeof(char) * (t + 1));
	strncpy(col_name, str, t);
	col_name[t] = '\0';

	grab_column(col_name, &cols[i]);
	if (NULL != cols[i]) {
		log_info("init an array with %zu slots\n", lineCount);
		cols[i]->data = darray_create(lineCount);
		// mark length at first
		cols[i]->data->length = lineCount;
	}

	char *dot = strrchr(col_name, '.');
	*dot = '\0';
	Table *tmp_tbl = NULL;
	grab_table(col_name, &tmp_tbl);
	// length == 0, in case of re-load
	if (NULL != tmp_tbl && 0 == tmp_tbl->length) {
		// Update the length of table according to the file loaded
		tmp_tbl->length = lineCount;
	}
	else{
		if (NULL == tmp_tbl) {
			log_err("Invalid table name!\n");
		}
		else {
			log_err("Data already loaded!\nOriginal table has length of %zu", tmp_tbl->length);
		}
		ret.code = ERROR;
		return ret;
	}
	*dot = '.';
	free(col_name);
	ret.code = OK;
	return ret;
}

status parse_dataset_csv(const char *filename) {
	status ret;
	if (NULL == database) {
		ret.code = ERROR;
		log_err("no database exists!\n");
		return ret;
	}

	FILE* fp = fopen(filename, "r");
	if (NULL != fp) {
		size_t len = 0;
		char *line = NULL;
		char *next = NULL;
		ssize_t read;
		read = getline(&line, &len, fp);
		size_t lineCount;
		size_t fieldCount;
		char *numbers = line;
		// first line with length of the Columns and number of the Columns
		lineCount = strtol(numbers, &next, 10);
		numbers = next + 1;
		fieldCount = strtol(numbers, &next, 10);
		if (lineCount <= 0 || fieldCount <= 0) {
			ret.code = ERROR;
			log_err("no data to load!\n");
			return ret;
		}

		Column **cols;
		cols = malloc(sizeof(Column *) * fieldCount);
		// handle line with Column names
		read = getline(&line, &len, fp);
		char *fields = line;
		if (0 != read && NULL != line) {
			ret = parse_field(fields, lineCount, fieldCount, cols);
			if (line) free(line);
			if (OK != ret.code) {
				free(cols);
				return ret;
			}
		}
		
		for (size_t i = 0; i < lineCount; i++) {
			char *line = NULL;
			read = getline(&line, &len, fp);
			if (0 != read) {
				char *str = line;
				size_t j = 0;
				for (; j < fieldCount; j++) {
					cols[j]->data->content[i] = strtol(str, &next, 10);
					str = next + 1;
				}
				if (line) {
					free(line);
				}
			}
		}
		fclose(fp);

		// for (size_t i = 0; i < fieldCount; i++) {
		// 	free(cols[i]);
		// }
		free(cols);
		log_info("parsing done\n");
	}
	else {
		ret.code = ERROR;
	}
	ret.code = CMD_DONE;
	return ret;
}