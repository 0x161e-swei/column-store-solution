#include "index.h"

/**
 * create an index over a Column
 * tbl: 		Table the index is in
 * col: 		the specific Column the index is on 
 * IndexType:	Specify the type, now partition ONLY
 */
status create_index(Table *tbl, Column *col, IndexType type) {
	status s;
	// TODO: copy the data first...
	if (NULL != col && NULL == col->index) {
		switch (type) {
			case PARTI: {
				if (1 == col->partitionCount) {
					// TODO: following 6 lines of code are for tests ONLY
					Partition_inst inst;
					scanf("%d", &(inst.p_count));
					inst.pivots = malloc(sizeof(int) * inst.p_count);
					for (int i = 0; i < inst.p_count; i++) {
						scanf("%d", &(inst.pivots[i]));
					}
					#ifdef GHOST_VALUE
					inst.ghost_count = malloc(sizeof(int) * inst.p_count);
					for (int i = 0; i < inst.p_count; i++) {
						scanf("%d", &(inst.ghost_count[i]));
					}
					#endif

					#ifdef SWAPLATER
					s = nWayPartition(col, &inst);
					if (OK == s.code) {
						s = swap_after_partition(tbl, col);
					}
					#else 
					s = nWayPartition(tbl, col, &inst);
					#endif
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

/** 
 * partition a Column and keep data align in other Column LATER in another function
 * col:		the Column that we are partitioning
 * inst:	the partition instruction containing pivot count, an array of pivots and
 * 			an array of ghost_value slots if applicable
 */
#ifdef SWAPLATER
status nWayPartition(Column *col, Partition_inst *inst) {
	
	status s;
	// TODO: code for Ghost Values
	int p_count = inst->p_count;
	int *pivots = inst->pivots;

	DArray_INT *arr = col->data;
	// idc used for indices to data during partitioning, pos used to record the position-map
	int *idc = malloc(sizeof(int) * p_count);
	int *pos = malloc(sizeof(int) * arr->length);
	col->pos = pos;

	for (int i = 0; i < p_count / 2; i++) 
		idc[i] = 0;
	for (int i = p_count / 2; i < p_count; i++) 
		idc[i] = arr->length - 1;

	while (idc[p_count / 2 - 1] <= idc[p_count / 2]) {
		while (arr->content[idc[p_count / 2 - 1]] <= pivots[p_count / 2 - 1] && idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			int tmp_idc = idc[p_count / 2 - 1];
			int tmp_val = arr->content[tmp_idc];
			// Init the position when the head pointers first hit the data
			pos[idc[p_count / 2 - 1]] = idc[p_count / 2 - 1];
			int tmp_pos = pos[tmp_idc];
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
			int tmp_idc = idc[p_count / 2];
			int tmp_val = arr->content[tmp_idc];

			// Init the position when the head pointers first hit the data
			pos[idc[p_count / 2]] = idc[p_count / 2];
			int tmp_pos = pos[tmp_idc];
			int i = p_count / 2;
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
			int tmp_l_idc = idc[p_count / 2];
			int tmp_r_idc = idc[p_count / 2 - 1];
			int r_val = arr->content[tmp_l_idc]			
			int l_val = arr->content[tmp_r_idc];

			// Init the position when the head pointers first hit the data
			pos[tmp_r_idc] = tmp_r_idc;
			pos[tmp_l_idc] = tmp_l_idc;
			int r_pos = pos[tmp_l_idc];
			int l_pos = pos[tmp_r_idc];
			int i = p_count / 2;

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

	// put the idcs into columns p_pos as positions of the pivots...
	for (int i = 0; i < p_count / 2; i++)
		col->p_pos[i] = idc[i] - 1;
	for (int i = p_count / 2 + 1; i <= p_count; i++)
		col->p_pos[i - 1] = idc[i];
	free(idc);
	col->partitionCount = p_count;
	s.code = CMD_DONE;
	return s;
}

status swap_after_partition(Table *tbl, Column *col) {

}

/**
 * function to be called by a single thread, swaping,
 * shared READ ONLY DATA
 */
void *swapsIncolumns(void *arg) {
	Swapargs *msg = (Swapargs *)arg;
	DArray_INT *data = msg->col->data;
	// TODO:
	// Create a new array...
	DArray_INT *newdata = darray_create(data->length);
	for (size_t i = 0; i < data->length; i++) {
		newdata->content[newdata->length++] = data->content[msg->pos[i]];
	}
	darray_destory(data);
	msg->col->data = newdata;
	return NULL;
}

/**
 * make data aligned in Same Table
 * cols: 			an array of Columns in the same Table
 * partitionedName: the name of the Column thas has already been partitioned
 * pos:				supposed position of data
 * col_count: 		number of Columns in the Table
 */
status doSwaps(Col_ptr *cols, const char *partitionedName, int *pos, int col_count){
	status s;
	if (NULL == cols) {
		s.code = ERROR;
		return s;
	}
	pthread_t *tids;
	Swapargs *args;
	tids = malloc(sizeof(pthread_t) * (col_count - 1));
	args = malloc(sizeof(Swapargs) * (col_count - 1));
	int t_count = 0;

	for (int i = 0; i < col_count; i++) {
		args[t_count].col =	cols[i];
		args[t_count].pos = pos;
		t_count += (0 != strcmp(partitionedName, cols[i]->name));
	}

	t_count = 0;
	for (int i = 0; i < col_count; i++) {
		if (0 != strcmp(partitionedName, cols[i]->name)) {
			pthread_create(&tids[t_count], NULL, swapsIncolumns, (void *)(args + t_count));
			t_count++;
		}
	}

	for (int i = 0; i < col_count - 1; i++) {
		pthread_join(tids[i], NULL);
	}

	s.code = OK;
	return s;
}

#else
/**
 * do partitioning on a Column and swap as partitioning goes on
 * col:		the Column that we are partitioning
 * inst:	the partition instruction containing pivot count, an array of pivots and
 * 			an array of ghost_value slots if applicable
 */
status nWayPartition(Table *tbl, Column *col, Partition_inst *inst) {
	// idc used for indices to data during partitioning, pos used to record the position-map
	int *idc;
	status s;
	
	// TODO: code for Ghost Values
	int p_count = inst->p_count;
	int *pivots = inst->pivots;

	// the index of the parttioned column in the table
	int col_index = -1;
	size_t col_count = tbl->col_count;
	for (size_t k = 0; k < col_count; k++) {
		if (tbl->cols[k] == col) {
			col_index = k;
			break;
		}
	}

	if (-1 == col_index) {
		s.code = ERROR;
		log_err("Column not in Table!\n");
		return s;
	}

	DArray_INT *arr = col->data;
	idc = malloc(sizeof(int) * p_count);

	for (int i = 0; i < p_count / 2; i++) 
		idc[i] = 0;
	for (int i = p_count / 2; i < p_count; i++) 
		idc[i] = arr->length - 1;

	int *tmp_val = malloc(sizeof(int) * col_count);
	int *r_val = malloc(sizeof(int) * col_count);
	int *l_val = malloc(sizeof(int) * col_count);

	while (idc[p_count / 2 - 1] <= idc[p_count / 2]) {
		while (arr->content[idc[p_count / 2 - 1]] <= pivots[p_count / 2 - 1] && idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			int tmp_idc = idc[p_count / 2 - 1];
			for (size_t k = 0; k < col_count; k++) {
				tmp_val[k] = tbl->cols[k]->data->content[tmp_idc];
			}
			int i = p_count / 2 - 2;
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
			int tmp_idc = idc[p_count / 2];
			for (size_t k = 0; k < col_count; k++) {
				tmp_val[k] = tbl->cols[k]->data->content[tmp_idc];
			}
			// int tmp_val = arr->content[tmp_idc];
			int i = p_count / 2;
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
			int tmp_l_idc = idc[p_count / 2];
			// record value to be moved
			for (size_t k = 0; k < col_count; k++) {
				r_val[k] = tbl->cols[k]->data->content[tmp_l_idc];
			}
			
			int tmp_r_idc = idc[p_count / 2 - 1];
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
			
			int j = p_count / 2 - 2;
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
	for (int i = 0; i < p_count / 2; i++)
		col->p_pos[i] = idc[i] - 1;
	for (int i = p_count / 2 + 1; i <= p_count; i++)
		col->p_pos[i - 1] = idc[i];
	free(idc);
	col->partitionCount = p_count;
	s.code = CMD_DONE;
	return s;
}

#endif

