#ifndef INDEX_H__
#define INDEX_H__

#include "cs165_api.h"
#include "column.h"
#include "table.h"
#include "db.h"


// The instruction for partition on a single column
// typedef struct _partition_inst{
// 	// pivot count
// 	int p_count;
// 	int *pivots;
// 	#ifdef GHOST_VALUE
// 	int *ghost_count;
// 	#endif
// } Partition_inst;



#ifdef GHOST_VALUE
status nWayPartition(Table *tbl, Column *col, Partition_inst *inst);
status insert_ghost_values(Table *tbl, int total_ghost);
#else
status nWayPartition(Column *col, Partition_inst *inst);
#endif /* GHOST_VALUE */

#ifdef SWAPLATER
typedef struct _swapargs {
	Column *col;
	pos_t *pos;
	uint len;
} Swapargs;

status align_after_partition(Table *tbl, pos_t *pos);
void *swapsIncolumns(void *arg);	// for threads processing
#else

#endif

#endif // INDEX_H__
