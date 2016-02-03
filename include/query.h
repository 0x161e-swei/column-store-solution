#ifndef QUERY_H__
#define QUERY_H__

#include "cs165_api.h"
#include "column.h"
#include "table.h"
#include "db.h"
#include <string.h>


typedef struct _swapargs {
	Column *col;
	int *pos;
} Swapargs;

status grab_result(const char *res_name, Result **res);
bool compare(comparator *f, int val);
status load_column4disk(Column *col, size_t len);

char *prepare_col(char *args, Table **tbl, Column **col);
char *prepare_res(char *args , Result **res);



// extern Result *res_hash_list;

#endif  // QUERY_H__