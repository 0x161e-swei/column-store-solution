/*

 * frequency_model.cint
 *
 *  Created on: Jan 21, 2016
 *      Author: kenneth
 */
#include "frequency_model.h"
#include "customsort/custom_sort.h"
#include "customsort/custom_comparators.h"
#include "customsort/custom_swappers.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <float.h>
#include <math.h>

void simple_operation_api(const int* high_val, const int workload,
		const int histogram_size, int* histogram){


	const int next = workload;
	//check if the value is out of range for the dataset, and if so increment last bin
	if(next >= high_val[histogram_size-1]){
		histogram[histogram_size-1]++;
	} else {
		//iterate the high values of the dataset, to find the bin that should be incremented
		int j = 0;
		while (high_val[j] < next) {
			j++;
		}
		histogram[j]++;
	}
}

void range_query_api(const int* high_val,const int first, const int last,frequency_model *fm){


	//const int first = work->range_query_start[i];
	//const int last = work->range_query_end[i];
	//check if the value is out of range for the dataset, and if so increment last bin
	if(first >= high_val[fm->histogram_size-1]){
		fm->pq[fm->histogram_size-1]++;
	} else {
		//iterate the high values of the dataset, to find the bin that should be incremented for the start
		int j = 0;
		while (high_val[j] < first) {
			j++;
		}
		//do we cover more than one block? And do we have more blocks to iterate?
		if(high_val[j] < last && j < fm->histogram_size-1) {
			fm->rs[j]++;
			j++;
			//record scanned blocks
			while(high_val[j] < last && j < fm->histogram_size) {
				fm->sc[j]++;
				j++;
			}
			//scan hit then end
			if(j == fm->histogram_size) {
				j--;
				fm->sc[j]--;
				fm->re[j]++;
			} else {
				//indicate the last block of the range query.
				fm->re[j]++;
			}
		} else {
			//if we only cover a single block the cost is the same as for a point query
			fm->pq[j]++;
		}
	}

}

void update_api(const int* high_val, const int o, const int n,frequency_model *fm){

	/*
	 * TODO:
	 * find place of old value and mark it in ud
	 * go forwards or backwards marking uf or ub, on all blocks excluding the one we actually insert into.
	 */
	//first: find the block we will delete from
	int j = 0;
	while(high_val[j] < o && j < fm->histogram_size) {
		j++;
	}
	if(j == fm->histogram_size) {
		//we hit the end
		j--;
		if( n > high_val[j] || n > high_val[j-1] ){
			//out of range or hit the last partition - no extra cost.
			fm->ud[j]++;
			return;
		}
	}

	//mark delete
	fm->ud[j]++;
	//do we insert in the same block?
	if(j == 0 && n <= high_val[j]) {
		return;
	} else if(j != 0 && n <= high_val[j] && n > high_val[j-1]) {
		return;
	} else {
		//not same block
		if (n < o) {
			//backward
			//go to the previous block
			j--;
			while(j >= 0 && n < high_val[j]){
				fm->ub[j]++;
				j--;

			}

		} else {
			//o < n
			//go to the next block
			while(n > high_val[j] && j+1 < fm->histogram_size){
				j++;
				fm->uf[j]++;
			}
		}
	}


}


void initialize_frequency_model(frequency_model *fm, const int data_size, const int block_size){
	fm->block_size = block_size;
	if((data_size % fm->block_size) == 0) {
		fm->histogram_size = data_size/fm->block_size;
	} else {
		fm->histogram_size = (data_size/fm->block_size)+1;
	}
	fm->de = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->in = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->pq = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->re = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->rs = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->sc = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->ub = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->ud = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->uf = (int *) calloc(fm->histogram_size , sizeof(int));
	fm->max_val = (int *) calloc(fm->histogram_size , sizeof(int));
}

void free_frequency_model(frequency_model *fm){
	free(fm->de);
	free(fm->in);
	free(fm->pq);
	free(fm->re);
	free(fm->rs);
	free(fm->sc);
	free(fm->ub);
	free(fm->ud);
	free(fm->uf);
	free(fm->max_val);
}

void build_frequency_model_api(data* data,const int size, const int* type, const int* first, const int* second,frequency_model *fm) {
	//TODO: Consider how this may be done in parallel - stdatomic.h seems like a place to start
	//TODO: Consider non-distinct value case
	//extract the high value of each block
	for(int i = fm->block_size, j=0; i < (int)data->size; i+=fm->block_size, j++){
		fm->max_val[j] = data->array[i - 1];
	}
	fm->max_val[fm->histogram_size-1] = data->array[data->size-1];

	for(int i = 0; i < size; i++) {
		if(type[i] == 0) {
			//point queries
			simple_operation_api(fm->max_val,first[i],fm->histogram_size,fm->pq);
		} else if(type[i] == 1){
			//range queries
			range_query_api(fm->max_val,first[i],second[i],fm);
		} else if(type[i] == 2){
			//insert
			simple_operation_api(fm->max_val,first[i],fm->histogram_size,fm->in);
		} else if(type[i] == 3){
			//updates
			update_api(fm->max_val,first[i],second[i],fm);
		} else if(type[i] == 4){
			//deletes
			simple_operation_api(fm->max_val,first[i],fm->histogram_size,fm->de);
		} else {
			printf("unknown");
		}
	}
}

void initialize_and_sort_data(data *data, const int* data_in,const size_t data_size) {
	data->array = (int *) malloc(data_size*sizeof(int));
	data->size = data_size;
	memcpy(data->array,data_in,data_size*sizeof(int));
	qsort(data->array,data->size,sizeof(int),cmpfunc_int);
}

frequency_model *sorted_data_frequency_model(const int* data_in, size_t data_size, const int* type, const int* first, const int* second, size_t work_size) {
	int block_size = 8;

	data data;
	initialize_and_sort_data(&data,data_in,data_size);
	frequency_model *fm = (frequency_model *) malloc(sizeof(frequency_model));
	initialize_frequency_model(fm, data.size,block_size);

	//Workload simulator
	build_frequency_model_api(&data,work_size,type,first,second, fm);
	free(data.array);
	return fm;

}

