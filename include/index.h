#ifndef INDEX_H__
#define INDEX_H__

#include "cs165_api.h"
#include "column.h"
#include "table.h"
#include "db.h"
#include "../src/frequencymodel/partition_data.h"

#define BINARY_SEARCH

#ifdef DEMO
#include "cmdsocket.h"
status do_physical_partition(struct cmdsocket *cmdSoc, Table *tbl, Column *col);
status do_parition_decision(struct cmdsocket *cmdSoc, Table *tbl, Column *col, int algo, const char *wordload);
#else
status do_physical_partition(Table *tbl, Column *col);
status do_parition_decision(Table *tbl, Column *col, int algo, const char *wordload);
#endif

// The instruction for partition on a single column
// typedef struct _partition_inst{
// 	// pivot count
// 	int p_count;
// 	int *pivots;
// 	#ifdef GHOST_VALUE
// 	int *ghost_count;
// 	#endif
// } Partition_inst;


extern Partition_inst *part_inst;

uint search_partition_pivots(void *from, size_t p_count, int key);
status physicalPartition_fast(Column *col, Partition_inst *inst);
status align_random_write(Table *tbl, pos_t *pos);

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
status align_test_col(Table *tbl, pos_t *pos);
void *swapsIncolumns(void *arg);	// for threads processing
#else

#endif

#endif // INDEX_H__
