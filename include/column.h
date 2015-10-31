#ifndef COLUMN_H__
#define COLUMN_H__ 

#include "cs165_api.h"

extern Column *col_hash_list;

status grab_column(const char* column_name, Column **col);

#endif // COLUMN_H__