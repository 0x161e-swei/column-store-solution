#ifndef __FILEPARSER_H
#define __FILEPARSER_H

#include <string.h>
#include "cs165_api.h"
#include "utils.h"
#include "csv.h"

typedef struct parse_csv_info {
    char first_row;
    size_t cur_feild;
    size_t cur_row;
    size_t field_count;
    size_t line_count;
    Column **cols;
    int error;
} parse_csv_info; 

char *readfile(const char *filename, size_t *len);

void csv_process_fields(void *s,
	size_t len __attribute__((unused)),
	void *data);

void csv_process_row(int delim __attribute__((unused)), 
	void *data);

status load_data4file(const char* filename, size_t line_count);

#endif /* __FILEPARSER_H */