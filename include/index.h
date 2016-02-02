#ifndef INDEX_H__
#define INDEX_H__ 

#include "cs165_api.h"
#include "column.h"
#include "table.h"
#include "db.h"


// The instruction for partition on a single column
typedef struct _partition_inst{
	// pivot count
	int p_count;
	int *pivots;
	#ifdef GHOST_VALUE
	int *ghost_count;
	#endif
} Partition_inst;

#ifdef SWAPLATER
status nWayPartition(Column *col, Partition_inst *inst);
status swap_after_partition(Table *tbl, Column *col);
status doSwaps(Col_ptr *cols, const char *partitionedName, int *pos, int col_count);
void *swapsIncolumns(void *arg);	// for threads processing
#else
status nWayPartition(Table *tbl, Column *col, Partition_inst *inst);
#endif

#endif // INDEX_H__ 

