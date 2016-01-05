#ifndef QUERY_H__
#define QUERY_H__ 

#include "cs165_api.h"
#include "column.h"
#include "table.h"
#include "db.h"
#include <stdbool.h>
#include <string.h>

status grab_result(const char *res_name, Result **res);
bool compare(comparator *f, int val);
status load_column4disk(Column *col, size_t len);

// extern Result *res_hash_list;

#endif  // QUERY_H__