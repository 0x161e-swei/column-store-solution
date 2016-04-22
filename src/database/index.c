#include "index.h"
#include "query.h"
#include "parser.h"
#include <time.h>


Partition_inst *part_inst = NULL;
frequency_model *freq_model = NULL;

#ifdef DEMO
status do_physical_partition(struct cmdsocket *cmdSoc, Table *tbl, Column *col)
#else
status do_physical_partition(Table *tbl, Column *col)
#endif
{
	status s;
	clock_t tic, toc;
	#ifdef SWAPLATER
		#ifdef GHOST_VALUE
			tic = clock();
			s = nWayPartition(tbl, col, part_inst);
			toc = clock();
			debug("partition with ghostvalue comsumed %lf\n", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
			#ifdef DEMO
				evbuffer_add_printf(cmdSoc->buffer, "{\"event\": \"message\",");
				evbuffer_add_printf(cmdSoc->buffer, "\"msg\": \"physical partition with ghost value done in %lf seconds!\"", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
				evbuffer_add_printf(cmdSoc->buffer, "}\n");
				flush_cmdsocket(cmdSoc);
			#endif // DEMO
			free(part_inst->ghost_count);
		#else
			tic = clock();
			s = nWayPartition(col, part_inst);
			toc = clock();

			debug("partition without ghostvalue comsumed %lf\n", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
			#ifdef DEMO
				evbuffer_add_printf(cmdSoc->buffer, "{\"event\": \"message\",");
				evbuffer_add_printf(cmdSoc->buffer, "\"msg\": \"physical partition without ghost value done in %lf seconds!\"", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
				evbuffer_add_printf(cmdSoc->buffer, "}\n");
				flush_cmdsocket(cmdSoc);
			#endif // DEMO
		#endif /* GHOST_VALUE */

		free(part_inst->pivots);
		if (CMD_DONE == s.code) {
			tic = clock();

			s = align_after_partition(tbl, col->pos);
			// s = align_test_col(tbl, col->pos);
			toc = clock();
			debug("align data comsumed %lf\n", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
			#ifdef DEMO
			evbuffer_add_printf(cmdSoc->buffer, "{\"event\": \"message\",");
			evbuffer_add_printf(cmdSoc->buffer, "\"msg\": \"aligning data in %lf seconds!\"", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
			evbuffer_add_printf(cmdSoc->buffer, "}\n");
			flush_cmdsocket(cmdSoc);
			#endif // DEMO
		}
		else {
			log_err("cannot partition on the data!\n");
		}
	#else /* SWAPLATER */
		#ifdef GHOST_VALUE
			s = nWayPartition(tbl, col, part_inst);
		#else /* GHOST_VALUE */
			s = nWayPartition(col, part_inst);
		#endif /* GHOST_VALUE */
	#endif /* SWAPLATER */

	return s;
}

#ifdef DEMO
status do_parition_decision(struct cmdsocket *cmdSoc, Table *tbl, Column *col, int algo, const char *workload)
#else
status do_parition_decision(Table *tbl, Column *col, int algo, const char *workload)
#endif
{
	status ret;
	uint lineCount = 0;
	uint fieldCount = 0;
	collect_file_info(workload, &lineCount, &fieldCount);

	if (1 >= lineCount) {
		log_err("cannot workload file %s\n", workload);
		ret.code = ERROR;
		return ret;
	}
	
	lineCount++;
	int *op_type = malloc(sizeof(int) * lineCount);
	int *num1 = malloc(sizeof(int) * lineCount);
	int *num2 = malloc(sizeof(int) * lineCount);
	workload_parse(workload, op_type, num1, num2);

	if (tbl->length != 0) {
		// do the loading later
		// for (unsigned int j = 0; j < tmp_tbl->col_count; j++) {
			if (NULL == col->data)
				load_column4disk(col, tbl->length);
		// }
	}
	if (NULL != part_inst) {
		free(part_inst);
		part_inst = NULL;
	}
	// get the frequency_model first
	// TODO: might move it to a independent function interface for future reuse 
	freq_model = sorted_data_frequency_model(col->data->content, col->data->length, op_type, num1, num2, lineCount);

	part_inst = malloc(sizeof(Partition_inst));
	partition_data(freq_model, 0, part_inst, col->data->length);
	debug("partition decision done\n");
	ret.code = PARTALGO_DONE;
	return ret;
}



/**
 * create an index over a Column
 * tbl: 		Table the index is in
 * col: 		the specific Column the index is on 
 * IndexType:	Specify the type, now partition ONLY
 */
status create_index(Table *tbl, Column *col, IndexType type, Workload w) {
	status s;
	clock_t tic, toc;
	if (NULL != col && NULL == col->index && NULL != col->data) {
		switch (type) {
			case PARTI: {
				// by 0 != partitionCount, we can repartition a partitioned column
				if (0 != col->partitionCount) {
					// TODO: do we need to free this one?
					part_inst = malloc(sizeof(Partition_inst));	
					debug("call partition decision function!!\n");
					#ifdef GHOST_VALUE
					// TODO: eighth parameter as algorithm type
					// call someone else
					#else
					tic = clock();
					
					// frequency_model *sorted_data_frequency_model
					// args: const int* data_in, size_t data_size, const int* type, const int* first, const int* second, size_t work_size
					freq_model = sorted_data_frequency_model(col->data->content, col->data->length, w.ops, w.num1, w.num2, w.count);
					
					// partition_data(frequency_model* fm,const int algo, Partition_inst *out, size_t data_size); 
					partition_data(freq_model, 0, part_inst, col->data->length);

					toc = clock();
					debug("partition decision function comsumed %lf\n", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
					free(w.ops);
					free(w.num1);
					free(w.num2);
					#endif

					debug("partition instruction: total %i partitions\n", part_inst->p_count);
					// for (int i = 0; i < part_inst->p_count; i++) {
					//	printf("%d\n", part_inst->pivots[i]);
					// }
					// TODO: following 6 lines of code are for tests ONLY
					// log_info("waiting for partition instruction\n");
					// scanf("%d", &(part_inst->p_count));
					// log_info("waiting for %d pivots\n", part_inst->p_count);
					// part_inst->pivots = malloc(sizeof(int) * part_inst->p_count);
					// for (int i = 0; i < part_inst->p_count; i++) {
					// 	scanf("%d", &(part_inst->pivots[i]));
					// }
					// #ifdef GHOST_VALUE
					// log_info("waiting for %d ghost_count\n", part_inst->p_count);
					// part_inst->ghost_count = malloc(sizeof(int) * part_inst->p_count);
					// for (int i = 0; i < part_inst->p_count; i++) {
					// 	scanf("%d", &(part_inst->ghost_count[i]));
					// }
					// #endif /* GHOST_VALUE */

					#ifdef SWAPLATER
					#ifdef GHOST_VALUE
					tic = clock();

					s = nWayPartition(tbl, col, part_inst);

					toc = clock();
					debug("partition with ghostvalue comsumed %lf\n", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);

					free(part_inst->ghost_count);
					#else
					tic = clock();

					s = nWayPartition(col, part_inst);

					toc = clock();
					debug("partition without ghostvalue comsumed %lf\n", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
					
					#endif /* GHOST_VALUE */
					free(part_inst->pivots);
					if (CMD_DONE == s.code) {
						tic = clock();

						// s = align_after_partition(tbl, col->pos);
						s = align_test_col(tbl, col->pos);
						toc = clock();
						debug("align without ghostvalue comsumed %lf\n", (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC);
					}
					#else
					#ifdef GHOST_VALUE
					s = nWayPartition(tbl, col, part_inst);
					#else
					s = nWayPartition(col, part_inst);
					#endif /* GHOST_VALUE */
					#endif /* SWAPLATER */
					debug("partitionCount %zu\n", col->partitionCount);
					free(part_inst);
					part_inst = NULL;
					free_frequency_model(freq_model);
					if (freq_model) free(freq_model);
					freq_model = NULL;	
					// for (size_t i = 0; i < col->partitionCount; i++) {
					//	printf("%d %zu\n", col->pivots[i], col->p_pos[i]);
					// }
				}
				else {
					s.code = ERROR;
				}
				break;
			}
			default: {
				// TODO: support more index types
				s.code = ERROR;
				break;
			}
		}	
	}
	return s;
}

#ifdef GHOST_VALUE
status insert_ghost_values(Table *tbl, int total_ghost) {
	status s;
	// Is it necessary to do it in parallel
	Col_ptr *cols = tbl->cols;
	for (uint i = 0; i < tbl->col_count; i++) {
		if (cols[i] == tbl->primary_indexed_col) {
			continue;
		}
		int *addon_ghosts = malloc(sizeof(int) *total_ghost);
		for (int j = 0; j < total_ghost; j++) {
			addon_ghosts[j] = NON_QUALIFYING_INT;
		}
		darray_vec_push((cols[i]->data), addon_ghosts, total_ghost);
		free(addon_ghosts);
	}
	s.code = OK;
	return s;
}
#endif
/** 
 * partition a Column and keep data align in other Column LATER in another function
 * tbl: 	the table where partitioned Column belongs to
 * col:		the Column that we are partitioning
 * inst:	the partition instruction containing pivot count, an array of pivots and
 * 			an array of ghost_value slots if applicable
 */
#ifdef SWAPLATER
#ifdef GHOST_VALUE
status nWayPartition(Table *tbl, Column *col, Partition_inst *inst)
#else
status nWayPartition(Column *col, Partition_inst *inst)
#endif /* GHOST_VALUE */
{
	status s;
	// TODO: make p_count in Partition_inst a type of uint instead of a int
	uint p_count = inst->p_count;
	int *pivots = inst->pivots;
	col->partitionCount = p_count;
	DArray_INT *arr = col->data;
	col->pivots = malloc(sizeof(int) * p_count);
	col->part_size = malloc(sizeof(int) * p_count);
	col->p_pos = malloc(sizeof(pos_t) * p_count);

	#ifdef GHOST_VALUE
	debug("before inserting ghost values, length of array %zu\n", arr->length);
	int total_gv = 0;
	uint actual_data_length = arr->length;
	col->ghost_count = malloc(sizeof(int) * p_count);
	for (uint i = 0; i < p_count; i++) {
		total_gv += inst->ghost_count[i];
	}
	int *addon_ghosts = malloc(sizeof(int) * total_gv);
	uint tmp_counter = 0;
	for (uint i = 0; i < p_count; i++)
		for (int j = 0; j < inst->ghost_count[i]; j++) {
			addon_ghosts[tmp_counter] = pivots[i];
			tmp_counter++;
		}
	darray_vec_push(arr, addon_ghosts, total_gv);
	free(addon_ghosts);
	// inseting NON_QUALIFYING_INT into other Columns
	insert_ghost_values(tbl, total_gv);
	debug("after inserting ghost values, length of array %zu\n", arr->length);
	#endif

	// idc used for indices to data during partitioning
	pos_t *idc = malloc(sizeof(pos_t) * p_count);
	// pos used to record the position-map, data now at index i used to be at pos[i]
	pos_t *pos = malloc(sizeof(pos_t) * arr->length);
	col->pos = pos;
	for (uint i = 0; i < p_count / 2; i++) 
		idc[i] = 0;
	for (uint i = p_count / 2; i < p_count; i++) 
		idc[i] = arr->length - 1;
	
	while (idc[p_count / 2 - 1] <= idc[p_count / 2]) {
		while (arr->content[idc[p_count / 2 - 1]] <= pivots[p_count / 2 - 1] && idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			pos_t tmp_idc = idc[p_count / 2 - 1]	;
			int tmp_val = arr->content[tmp_idc];
			// Init the position when the head pointers first hit the data
			pos[idc[p_count / 2 - 1]] = idc[p_count / 2 - 1];
			pos_t tmp_pos = pos[tmp_idc];
			int i = p_count / 2 - 2;
			while (i >= 0 && tmp_val <= pivots[i]) {
				arr->content[tmp_idc] = arr->content[idc[i]];
				pos[tmp_idc] = pos[idc[i]];
				tmp_idc = idc[i];
				idc[i]++;
				i--;
			}
			arr->content[tmp_idc] = tmp_val;
			pos[tmp_idc] = tmp_pos;
			// move the left head forward
			idc[p_count / 2 - 1]++;
		}

		while (arr->content[idc[p_count / 2]] > pivots[p_count / 2 - 1] && idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			pos_t tmp_idc = idc[p_count / 2];
			int tmp_val = arr->content[tmp_idc];

			// Init the position when the head pointers first hit the data
			pos[idc[p_count / 2]] = idc[p_count / 2];
			pos_t tmp_pos = pos[tmp_idc];
			uint i = p_count / 2;
			while ((i < p_count - 1) && tmp_val > pivots[i]) {
				arr->content[tmp_idc] = arr->content[idc[i + 1]];
				pos[tmp_idc] = pos[idc[i + 1]];
				tmp_idc = idc[i + 1];
				idc[i + 1]--;
				i++;
			}
			arr->content[tmp_idc] = tmp_val;
			pos[tmp_idc] = tmp_pos;
			// move the right head backward
			idc[p_count / 2]--;
		}

		if (idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			pos_t tmp_l_idc = idc[p_count / 2];
			pos_t tmp_r_idc = idc[p_count / 2 - 1];
			int r_val = arr->content[tmp_l_idc];
			int l_val = arr->content[tmp_r_idc];

			// Init the position when the head pointers first hit the data
			pos[tmp_r_idc] = tmp_r_idc;
			pos[tmp_l_idc] = tmp_l_idc;
			pos_t r_pos = pos[tmp_l_idc];
			pos_t l_pos = pos[tmp_r_idc];
			uint i = p_count / 2;

			while((i < p_count - 1) && l_val > pivots[i]) {
				arr->content[tmp_l_idc] = arr->content[idc[i + 1]];
				pos[tmp_l_idc] = pos[idc[i + 1]];
				tmp_l_idc = idc[i + 1];
				idc[i + 1]--;
				i++;
			}
			
			int j = p_count / 2 - 2;
			while(j >= 0 && r_val <= pivots[j]) {
				arr->content[tmp_r_idc] = arr->content[idc[j]];
				pos[tmp_r_idc] = pos[idc[j]];
				tmp_r_idc = idc[j];
				idc[j]++;
				j--;
			}
			
			arr->content[tmp_l_idc] = l_val;
			pos[tmp_l_idc] = l_pos;
			arr->content[tmp_r_idc] = r_val;
			pos[tmp_r_idc] = r_pos;
			// move the head pointers
			idc[p_count / 2 - 1]++;
			idc[p_count / 2]--;
		}
	}

	memcpy(col->pivots, inst->pivots, sizeof(int) * p_count);
	memcpy(col->part_size, inst->part_sizes, sizeof(int) * p_count);
	// write pivots positions to the sepcified Column
	// put the idcs into columns p_pos as positions of the pivots...
	for (uint i = 0; i < p_count / 2; i++)
		col->p_pos[i] = (idc[i] == 0)?0 :(idc[i] - 1);
	for (uint i = p_count / 2 + 1; i < p_count; i++)
		col->p_pos[i - 1] = idc[i];
	col->p_pos[p_count - 1] = arr->length - 1;
	free(idc);

	#ifdef GHOST_VALUE
	// move ghost values in each partition to the end of the partition
	// as they might end up at any position in the partition after partitioned
	// the partition algorithm is not order-preserving
	for (uint i = 0; i < p_count; i++) {
		int g_count = 0;
		pos_t beg = (i == 0)? 0 : col->p_pos[i - 1] + 1;
		pos_t end = col->p_pos[i];
		col->ghost_count[i] = inst->ghost_count[i];
		while (g_count < inst->ghost_count[i]) {
			// ghost Values have position lager than actual length
			// find the first ghost pos from the beginning of partition
			while (pos[beg] < actual_data_length && beg < end) beg++;
			// find the first non-ghost pos from the end of the partition
			while (pos[end] >= actual_data_length && end >= beg) {
				arr->content[end] = NON_QUALIFYING_INT;
				g_count++;
				end--;
			}
			if (beg > end || g_count >= inst->ghost_count[i]) {
				break;
			}
			arr->content[beg] = arr->content[end];
			arr->content[end] = NON_QUALIFYING_INT;
			pos[beg] = pos[beg] ^ pos[end];
			pos[end] = pos[beg] ^ pos[end];
			pos[beg] = pos[beg] ^ pos[end];
			end--;
			g_count++;
			beg++;
		}
	}
	#endif
	s.code = CMD_DONE;
	return s;
}

/**
 * function to be called by a single thread, swaping,
 * shared READ ONLY DATA
 */
void *swapsIncolumns(void *arg) {
	Swapargs *msg = (Swapargs *)arg;
	if (msg->len != 0 && NULL == msg->col->data) {
		load_column4disk(msg->col, msg->len);	
	}
	DArray_INT *arr = msg->col->data;
	// Create a new array...
	if (NULL != arr) {
		DArray_INT *newdata = darray_create(arr->length);
		for (uint i = 0; i < arr->length; i++) {
			newdata->content[i] = arr->content[msg->pos[i]];
		}
		newdata->length = arr->length;
		darray_destory(arr);
		msg->col->data = newdata;
	}
	pthread_exit(NULL);
}

/**
 * make data aligned in Same Table
 * tbl: 			the table where partitioned Column belongs to
 * pos:				supposed position of data
 */
status align_after_partition(Table *tbl, pos_t *pos){
	status s;
	Col_ptr *cols = tbl->cols;
	size_t col_count = tbl->col_count;
	if (NULL == cols) {
		s.code = ERROR;
		return s;
	}
	pthread_t *tids;
	Swapargs *args;
	tids = malloc(sizeof(pthread_t) * (col_count - 1));
	args = malloc(sizeof(Swapargs) * (col_count - 1));
	int t_count = 0;

	for (uint i = 0; i < col_count; i++) {
		args[t_count].col =	cols[i];
		args[t_count].pos = pos;
		args[t_count].len = tbl->length;
		if (tbl->primary_indexed_col != cols[i]) {
			t_count++;	
		}
	}

	t_count = 0;
	for (uint i = 0; i < col_count; i++) {
		if (cols[i] != tbl->primary_indexed_col) {
			pthread_create(&tids[t_count], NULL, swapsIncolumns, (void *)&args[t_count]);
			t_count++;
		}
	}
	for (uint i = 0; i < col_count - 1; i++) {
		pthread_join(tids[i], NULL);
	}
	// TODO: free threads or maintain thread pool
	free(args);
	s.code = CMD_DONE;
	return s;
}
/**
 * sequential aligning data in the same Table
 * first col_count / 2 Columns are aligned in sequential write and 
 * the rest of the Columns are aligned in random write
 */
status align_test_col(Table *tbl, pos_t *pos) {
	status s;
	clock_t tic, toc;
	Col_ptr *cols = tbl->cols;
	size_t col_count = tbl->col_count;
	if (NULL == cols) {
		s.code = ERROR;
		return s;
	}
	pos_t *inverse_pos = malloc(sizeof(pos_t) * tbl->length);
	for (uint i = 0; i < tbl->length; i++) {
		inverse_pos[pos[i]] = i;
	}
	
	// sequential write + sequential read and random read
	double period_1 = 0;
	for (size_t i = 1; i < col_count / 2; i++){
		if (tbl->length != 0 && NULL == cols[i]->data) {
			load_column4disk(cols[i], tbl->length);
		}
		DArray_INT *old_arr = cols[i]->data;
		DArray_INT *new_arr = darray_create(old_arr->length);
		tic = clock();
		for (uint j = 0; j < old_arr->length; j++) {
			new_arr->content[j] = old_arr->content[pos[j]];
		}
		toc = clock();
		period_1 = period_1 + (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC; 
		new_arr->length = old_arr->length;
		darray_destory(old_arr);
		cols[i]->data = new_arr;
	}
	period_1 /= (col_count / 2 - 1);

	// random write + two  sequential read
	double period_2 = 0;
	for (size_t i = col_count / 2; i < col_count; i++) {
		if (tbl->length != 0 && NULL == cols[i]->data) {
                        load_column4disk(cols[i], tbl->length);
                }
                DArray_INT *old_arr = cols[i]->data;
                DArray_INT *new_arr = darray_create(old_arr->length);
		tic = clock();		
		for (uint j = 0; j < old_arr->length; j++) {
			new_arr->content[inverse_pos[j]] = old_arr->content[j];
		}
		toc = clock();
		period_2 = period_2 + (double)(toc -tic) * 1000.0 / CLOCKS_PER_SEC; 
		new_arr->length = old_arr->length;
		darray_destory(old_arr);
		cols[i]->data = new_arr;
	}
	period_2 /= (col_count / 2);
	debug("time spent in both periods, %lf and %lf\n", period_1, period_2);

	s.code = CMD_DONE;
	return s;	
}
#else
/**
 * do partitioning on a Column and swap as partitioning goes on
 * col:		the Column that we are partitioning
 * inst:	the partition instruction containing pivot count, an array of pivots and
 * 			an array of ghost_value slots if applicable
 */
// TODO: ghost value not implemented in swap during partition time
#ifdef GHOST_VALUE
status nWayPartition(Table *tbl, Column *col, Partition_inst *inst)
#else
status nWayPartition(Column *col, Partition_inst *inst)
#endif /* GHOST_VALUE */
{
	// idc used for indices to data during partitioning, pos used to record the position-map
	pos_t *idc;
	status s;
	
	// TODO: write code for Ghost Values in swap during partitioning
	uint p_count = inst->p_count;
	int *pivots = inst->pivots;
	col->partitionCount = p_count;

	// the index of the parttioned column in the table
	size_t col_index = -1;
	size_t col_count = tbl->col_count;
	for (size_t k = 0; k < col_count; k++) {
		if (tbl->cols[k] == col) {
			col_index = k;
			break;
		}
	}

	DArray_INT *arr = col->data;
	idc = malloc(sizeof(pos_t) * p_count);

	for (uint i = 0; i < p_count / 2; i++) 
		idc[i] = 0;
	for (uint i = p_count / 2; i < p_count; i++) 
		idc[i] = arr->length - 1;

	int *tmp_val = malloc(sizeof(int) * col_count);
	int *r_val = malloc(sizeof(int) * col_count);
	int *l_val = malloc(sizeof(int) * col_count);

	while (idc[p_count / 2 - 1] <= idc[p_count / 2]) {
		while (arr->content[idc[p_count / 2 - 1]] <= pivots[p_count / 2 - 1] && idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			pos_t tmp_idc = idc[p_count / 2 - 1];
			for (size_t k = 0; k < col_count; k++) {
				tmp_val[k] = tbl->cols[k]->data->content[tmp_idc];
			}
			uint i = p_count / 2 - 2;
			while (i >= 0 && tmp_val[col_index] <= pivots[i]) {
				// Do all the swaps here
				for (size_t k = 0; k < col_count; k++) {
					tbl->cols[k]->data->content[tmp_idc] = tbl->cols[k]->data->content[idc[i]];
				}
				// arr->content[tmp_idc] = arr->content[idc[i]];
				tmp_idc = idc[i];
				idc[i]++;
				i--;
			}

			// move all the data
			for (size_t k = 0; k < col_count; k++) {
				tbl->cols[k]->data->content[tmp_idc] = tmp_val[k];
			}
			// arr->content[tmp_idc] = tmp_val;
			// move the left head forward
			idc[p_count / 2 - 1]++;
		}

		while (arr->content[idc[p_count / 2]] > pivots[p_count / 2 - 1] && idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			uint tmp_idc = idc[p_count / 2];
			for (size_t k = 0; k < col_count; k++) {
				tmp_val[k] = tbl->cols[k]->data->content[tmp_idc];
			}
			// int tmp_val = arr->content[tmp_idc];
			uint i = p_count / 2;
			while ((i < p_count - 1) && tmp_val[col_index] > pivots[i]) {
				for (size_t k = 0; k < col_count; k++) {
					tbl->cols[k]->data->content[tmp_idc] = tbl->cols[k]->data->content[idc[i]];
				}
				// arr->content[tmp_idc] = arr->content[idc[i + 1]];
				tmp_idc = idc[i + 1];
				idc[i + 1]--;
				i++;
			}

			// move all the data
			for (size_t k = 0; k < col_count; k++) {
				tbl->cols[k]->data->content[tmp_idc] = tmp_val[k];
			}
			// move the right head backward
			idc[p_count / 2]--;
		}

		if (idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			pos_t tmp_l_idc = idc[p_count / 2];
			// record value to be moved
			for (size_t k = 0; k < col_count; k++) {
				r_val[k] = tbl->cols[k]->data->content[tmp_l_idc];
			}
			
			pos_t tmp_r_idc = idc[p_count / 2 - 1];
			// record value to be moved
			for (size_t k = 0; k < col_count; k++) {
				l_val[k] = tbl->cols[k]->data->content[tmp_r_idc];
			}
			
			int i = p_count / 2;
			while((i < p_count - 1) && l_val[col_index] > pivots[i]) {
				// move pivots to make room for 
				for (size_t k = 0; k < col_count; k++) {
					tbl->cols[k]->data->content[tmp_l_idc] = tbl->cols[k]->data->content[idc[i + 1]];
				}
				tmp_l_idc = idc[i + 1];
				idc[i + 1]--;
				i++;
			}
			
			uint j = p_count / 2 - 2;
			while(j >= 0 && r_val[col_index] <= pivots[j]) {
				for (size_t k = 0; k < col_count; k++) {
					tbl->cols[k]->data->content[tmp_r_idc] = tbl->cols[k]->data->content[idc[j]];
				}
				// arr->content[tmp_r_idc] = arr->content[idc[j]];
				tmp_r_idc = idc[j];
				idc[j]++;
				j--;
			}
			
			for (size_t k = 0; k < col_count; k++) {
				tbl->cols[k]->data->content[tmp_l_idc] = l_val[k];
				tbl->cols[k]->data->content[tmp_r_idc] = r_val[k];
			}
			// arr->content[tmp_l_idc] = l_val;
			// arr->content[tmp_r_idc] = r_val;
			// move the head pointers
			idc[p_count / 2 - 1]++;
			idc[p_count / 2]--;
		}
	}

	free(tmp_val);
	free(l_val);
	free(r_val);

	// put the idcs into columns p_pos as positions of the pivots...
	for (uint i = 0; i < p_count / 2; i++)
		col->p_pos[i] = idc[i] - 1;
	for (uint i = p_count / 2 + 1; i < p_count; i++)
		col->p_pos[i - 1] = idc[i];
	col->p_pos[p_count - 1] = arr->length - 1;
	free(idc);
	col->partitionCount = p_count;
	s.code = CMD_DONE;
	return s;
}

#endif
