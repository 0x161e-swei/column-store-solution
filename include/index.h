#ifndef INDEX_H__
#define INDEX_H__ 

#include "cs165_api.h"
#include "column.h"
#include "table.h"
#include "db.h"


#ifdef SWAPLATER
status nWayPartition(Column *col, int p_count, int pivots[]);
status swap_after_partition(Table *tbl, Column *col);
status doSwaps(Col_ptr *cols, const char *partitionedName, int *pos, int col_count);
void *swapsIncolumns(void *arg);	// for threads processing
#else
status nWayPartition(Table *tbl, Column *col, int p_count, int pivots[]);
#endif

#endif // INDEX_H__ 

