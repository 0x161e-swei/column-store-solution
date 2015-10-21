#ifndef QUERY_H__
#define QUERY_H__ 

#include "cs165_api.h"
#include "column.h"
#include "table.h"
#include "db.h"
#include <string.h>

status grab_result(const char *res_name, Result **res);

// extern Result *res_hash_list;

#endif  // QUERY_H__