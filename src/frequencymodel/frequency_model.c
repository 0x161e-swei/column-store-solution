/*

 * frequency_model.cint
 *
 *  Created on: Jan 21, 2016
 *      Author: kenneth
 */
#include "frequency_model.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <float.h>
#include <math.h>

int cmpfunc (const void * a, const void * b)
{
	return ( *(int*)a - *(int*)b );
}


int ps_comparator(const void *a, const void *b, void* c) {

	const partition_struct* first = (partition_struct*)a;
	const partition_struct* second = (partition_struct*)b;
	if(first->score < second->score) {
		return 1;
	} else if(second->score < first->score) {
		return -1;
	} else {
		return 0;
	}
	return 0;
}

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
		fm->rs[fm->histogram_size-1]++;
	} else {
		//iterate the high values of the dataset, to find the bin that should be incremented for the start
		int j = 0;
		while (high_val[j] < first) {
			j++;
		}

		//do we cover more than one block?
		if(high_val[j] < last) {
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


	//const int o = work->update_old[i];
	//const int n = work->update_new[i];
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
		fm->ud[j]++;
		if(high_val[j] >= n || high_val[j-1] >= n){
			//out of range or hit the last partition - no extra cost.
			return;
		}
	} else {
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
				while(j-1 >= 0 && n < high_val[j-1]){
					fm->ub[j]++;
					j--;
				}
			} else {
				//o < n
				//go to the next block
				j++;
				while(n < high_val[j+1] && j+1 < fm->histogram_size){
					fm->uf[j]++;
					j++;
				}
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
}

static inline void prefix_sum(const int* input, int* output,const int size){
	output[0] = input[0];
	for(int i = 1; i < size; i++) {
		output[i] = input[i]+output[i-1];
	}
}

void compute_prefix_sum(const frequency_model *fm, prefix *prefix_sum_model) {
	//prefix sum for all histograms
	//TODO: Once all algorithms are in place, check if some of these are not needed and if so remove them
	prefix_sum(fm->de,prefix_sum_model->de,fm->histogram_size);
	prefix_sum(fm->in,prefix_sum_model->in,fm->histogram_size);

}

double compute_costs(forward_backward* fb, partition_struct* partition_cost, const frequency_model *fm,
		const prefix *prefix_sum, const double rr, const double rw, const double sr){
	//data movement costs per partition boundary. These are fixed and can thus be precomputed.
	//it should be noted that some cost per operation is unavoidable, independently of the partitioning.
	//That cost is not captured here.
	double static_cost = 0.0;

	for(int i = 0; i < prefix_sum->histogram_size; i++){
		partition_cost[i].partition_static_cost = 0.0;
		//accumulated forward reads
		fb->backward[i] = fm->rs[i]+fm->pq[i]+fm->de[i]+fm->ud[i];
		//accumulated forward reads
		fb->forward[i] = fm->re[i]+fm->pq[i]+fm->de[i]+fm->ud[i];
		//inserts - static work local to the block and always needed
		static_cost += fm->in[i]*(rr+rw);
#ifndef GHOST_VALUE
		if(i != 0) {
			//inserts - data movement across partitions. We never touch the block we insert into, so the cost is "shifted by one"
			partition_cost[i].partition_static_cost += prefix_sum->in[i-1]*(rr+rw);
		}
#endif
#ifndef GHOST_VALUE
		//deletes - data movement local to the block and always needed
		static_cost += fm->de[i]*rw;
		//deletes - data movement across partitions
		partition_cost[i].partition_static_cost += prefix_sum->de[i]*(rr+rw);
#else
		//deletes - data movement is local to the block with ghost values
		static_cost += fm->de[i]*(2*rw+rr);
#endif
		//update - fixed data movement
		static_cost += fm->ud[i]*(2*rw+rr);
		//update partition dependent data movement. This is local to each block/partition pair.
		if(i < prefix_sum->histogram_size-1) {
			//update backward, corresponds to the cost of the next block, rather than the previous so we offset the index by one.
			partition_cost[i].partition_static_cost += fm->ub[i+1]*(rr+rw);
		}
		partition_cost[i].partition_static_cost += fm->uf[i]*(rr+rw);

		//static cost of point queries, including deletes and updates
		static_cost += (fm->de[i]+fm->pq[i]+fm->ud[i])*rr;
		//static cost of starting a range query
		static_cost += fm->rs[i]*rr;
		//static cost of ending a range query
		static_cost += fm->re[i]*sr;
		//static cost of scanning as part of a range query
		static_cost += fm->sc[i]*sr;
	}

	return static_cost;
}

static inline void setup_partition_structure(const int block_counter,const int max_val,const int histogram_size, partition_struct *ps){

	ps[block_counter].max_val = max_val;
	ps[block_counter].max_block = block_counter;
	ps[block_counter].min_block = block_counter;
	ps[block_counter].score = 0.0;
	ps[block_counter].partition_static_cost = 0.0;
#ifdef GHOST_VALUE
	ps[block_counter].next_partitions = 0.0;
	ps[block_counter].ghost_values = 0;
	ps[block_counter].inserts = 0;

#endif

	if(block_counter+1 < histogram_size){
		ps[block_counter].next_neighbor = &ps[block_counter+1];
	} else {
		ps[block_counter].next_neighbor = NULL;
	}

	if(block_counter != 0) {
		ps[block_counter].prev_neighbor = &ps[block_counter-1];
	} else {
		ps[block_counter].prev_neighbor = NULL;
	}

}


void setup_partitions(frequency_model *fm, partition_struct *ps, int data_size) {
	for(int j=0; j < fm->histogram_size; j++){
		setup_partition_structure(j,fm->max_val[j],fm->histogram_size,ps);
#ifdef GHOST_VALUE
	//record the amount of inserts per partition as these will be used to compute the dynamic cost of each partition
		ps[j].inserts = fm->in[j];
#endif
	}

}

void build_frequency_model_api(data* data,const int size, const int* type, const int* first, const int* second,frequency_model *fm) {
	//TODO: Consider how this may be done in parallel - stdatomic.h seems like a place to start
	//TODO: Consider non-distinct value case
	//TODO: apply workload to data, record result in frequency model.
	//extract the high value of each block
	for(int i = fm->block_size, j=0; i < (int)data->size; i+=fm->block_size, j++){
		fm->max_val[j] = data->array[i];
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
	//for(int i = 0; i < fm->histogram_size; i++){
	//printf("%i\t%i\t%i\t%i\t%i\t%i\t%i\t%i\t%i\n",fm->de[i],fm->in[i],fm->pq[i],fm->re[i],fm->rs[i],fm->sc[i],fm->ub[i],fm->ud[i],fm->uf[i]);
	//}
}


static inline void compute_score(partition_struct *ps, const forward_backward *fb, const double sr) {
	if(ps->next_neighbor){
		partition_struct *next = ps->next_neighbor;
		int additional_blocks = (next->max_block-ps->min_block);
		double score = 0.0;
		for(int i = 0, index = ps->min_block; i <= additional_blocks; i++, index++) {
			//compute the cost of removing the partition boundary in terms of additional read penalty
			score += fb->backward[index]*i*sr;
			score += fb->forward[index]*(additional_blocks-i)*sr;
		}
		//the score is the difference between the data movement cost of having the partition boundary,
		//and the read penalty of not having the partition boundary
		ps->score = ps->partition_static_cost-score;
	} else {
		ps->score = -DBL_MAX;
	}
}

static inline partition_struct* insert_queue(partition_struct* head, partition_struct* el){
	partition_struct* pos = head;
	if(pos->score < el->score) {
		//el is first in line
		el->prev_queue = NULL;
		el->next_queue = pos;
		pos->prev_queue = el;
		return el;
	} else {
		while(pos->next_queue && pos->score >= el->score) {
			//printf("pos score: %f\n",pos->score);
			partition_struct* temp = pos->next_queue;
			pos = temp;
		}
		if(pos->score < el->score){
			partition_struct* temp = pos->prev_queue;
			pos = temp;
		}
		//printf("pos final: %f\t el score:%f\n",pos->score,el->score);
		//el have a larger score than pos, so we insert it immediately before pos.
		el->prev_queue = pos;
		if(pos->next_queue){
			el->next_queue = pos->next_queue;
			el->next_queue->prev_queue = el;
		} else {
			el->next_queue = NULL;
		}
		pos->next_queue = el;
	}
	return head;
}

static inline void delete_queue(partition_struct* next){
	if(next->next_queue) {
		if(next->prev_queue) {
			next->prev_queue->next_queue = next->next_queue;
		} else {
			//next is first in the queue
			next->next_queue->prev_queue = NULL;
		}
	} else if(next->prev_neighbor) {
		//next is last in the queue
		next->prev_queue->next_queue = NULL;
	}
}

partition_struct* compute_partitioning_bottom_up(const forward_backward *fb, partition_struct* partition_cost,
		const frequency_model *fm, const double sr, size_t data_size) {
	//this is not as optimal as it should be. We should keep a sorted queue of all partitions that have a positive potential impact.

	//partition_struct* tail = &partition_cost[fm->histogram_size-1];
	//generate a priority queue
	/*
	 * Algorithm outline:
	 *
	 * 1. compute priority queue score for each partition as the IOs "saved" by merging with the neighbor to the right.
	 * 2. sort array by the score
	 * 3. initialize priority queue by building pointers to the element to the right, and recording fence pointers
	 * 4. while the next score in the priority queue is positive
	 * 		a. pop the first element, p
	 * 		b. pop p's neighbor q out of order.
	 * 		c. merge p and q to p'
	 * 		d. compute score for p'
	 * 		e. use fence pointers to locate where p' belongs in the priority queue and insert it.
	 * 5. output final partitioning
	 */

	//1. compute priority queue score for each partition as the IOs "saved" by merging with the neighbor to the right.
	for(int i = 0; i < fm->histogram_size-1; i++) {
		compute_score(&partition_cost[i], fb, sr);
		partition_cost[i].part_size = fm->block_size;

	}
	partition_cost[fm->histogram_size-1].score = -DBL_MAX;
	//the size of the last partition is not necessarily a full block size.
	partition_cost[fm->histogram_size-1].part_size = data_size - fm->histogram_size-1*fm->block_size;
	//sort
	quicksort_custom(partition_cost,fm->histogram_size,sizeof(partition_struct),ps_comparator,NULL);
	for(int i = 0; i < fm->histogram_size; i++) {
		if(i != 0) {
			partition_cost[i].prev_queue = &partition_cost[i-1];
		} else {
			partition_cost[i].prev_queue = NULL;
		}
		if(i+1 < fm->histogram_size){
			partition_cost[i].next_queue = &partition_cost[i+1];
		} else {
			partition_cost[i].next_queue = NULL;
		}

	}
	partition_struct* head = &partition_cost[0];
	//4. while the next score in the priority queue is positive
	while(head->score > 0 && head->next_queue) {

		//a. pop the first element - this is head
		partition_struct *first = head;
		//b. pop p's neighbor q out of order.
		partition_struct* next = first->next_neighbor;
		//c. merge p and q to p
		first->max_block = next->max_block;
		first->next_neighbor = next->next_neighbor;
		first->partition_static_cost = next->partition_static_cost;
		first->max_val = next->max_val;
		first->part_size += next->part_size;
		//are first and next the two first in the queue list?
		if(first->next_queue != next) {
			//delete head from queue
			head = first->next_queue;
			//delete next from queue
			if(next->next_queue) {
				next->next_queue->prev_queue = next->prev_queue;
			}
			if(next->prev_queue) {
				next->prev_queue->next_queue = next->next_queue;
			}


		} else {
			//first and next are the two first in the queue, so we make nexts next the queue head
			head = next->next_queue;
		}
		head->prev_queue = NULL;

		//delete next from the neighbor list (we keep first as the new merged partition)
		if(next->next_neighbor) {
			if(next->prev_neighbor) {
				next->prev_neighbor->next_neighbor = next->next_neighbor;
				next->next_neighbor->prev_neighbor = next->prev_neighbor;
			} else {
				//next is first in the neighbor list
				next->next_neighbor->prev_neighbor = NULL;
			}
		} else if(next->prev_neighbor) {
			//next is last in the neighbor list
			next->prev_neighbor->next_neighbor = NULL;
		}
		//recompute scores for affected partitions and reinsert them in the queue.
		if(first->next_neighbor){
			//if next wasn't the last partition
			compute_score(first,fb,sr);
			//reinsert in queue.
		} else {
			first->score = -DBL_MAX;
		}

		head = insert_queue(head,first);

		if(first->prev_neighbor){
			//if first had a previous neighbor his cost just changed as well, so we delete, recompute and reinsert
			partition_struct *prev = first->prev_neighbor;
			if(head == prev) {
				head = prev->next_queue;
			}
			delete_queue(prev);
			compute_score(prev,fb,sr);
			head = insert_queue(head,prev);
		}
		/*
		int test = 0;
		partition_struct *pointer = head;
		while(pointer) {
			test++;
			printf("%f,%i,%i,%i\t",pointer->score,pointer->min_block,pointer->max_block,pointer->max_val);
			pointer = pointer->next_queue;

		}
		printf("\nsize: %i\n",test);
		 */

	}
	//done so return the partitioning
	return head;
}

#ifdef GHOST_VALUE
partition_struct* compute_partitioning_bottom_up_gv(const forward_backward *fb, partition_struct* partition_cost,
		const frequency_model *fm, const double sr, const double rr, const double rw, const int gv, const int data_size) {
	//this is not as optimal as it should be. We should keep a sorted queue of all partitions that have a positive potential impact.

	//partition_struct* tail = &partition_cost[fm->histogram_size-1];
	//generate a priority queue
	/*
	 * Algorithm outline:
	 * 1. Compute cost of the inserts per partition and accumulated, assuming no ghost values.
	 * 2. Distribute ghost values relative to the cost of inserts for each partition, and compute score for each, keeping track of the high hitter
	 * 3. Merge high highest score with neighbor
	 * 4. recompute cost(as before assuming no ghost values) for all previous partitions.
	 * 5. redistribute ghost values and compute new scores, keeping track of the highest scoring partition.
	 * While highest score is positive - GOTO: 3
	 * output partitioning and ghost value distribution.
	 *
	 */

	//double* prefix_in_back = (double *) calloc(fm->histogram_size,sizeof(double));
	//1. Compute cost of the inserts per partition and accumulated, assuming no ghost values.
	for(int i = fm->histogram_size-1; i >= 0; i--){
		if(i == fm->histogram_size-1) {
			partition_cost[fm->histogram_size-1].part_size = data_size - fm->histogram_size-1*fm->block_size;
		} else {
			partition_cost[i].part_size = fm->block_size;
		}
		partition_struct *next = &partition_cost[i];
		//amount partitions before this one
		int prev_partitions = fm->histogram_size-1-i;
		//each previous partition adds an rr and a rw to the total cost of each insert (the static part is handled elsewhere)
		double insert_cost = prev_partitions*fm->in[i]*(rr+rw);
		next->next_partitions = prev_partitions;
		if(i == fm->histogram_size-1) {
			next->prefix_in_back = insert_cost;
		} else {
			next->prefix_in_back = insert_cost+next->next_neighbor->prefix_in_back;
		}
		next->inserts = fm->in[i];
	}
	double total_insert_cost = partition_cost[0].prefix_in_back;
	//2. Distribute ghost values relative to the cost of inserts for each partition, and compute score for each, keeping track of the high hitter
	partition_struct *high_hitter = &partition_cost[0];
	for(int i = 0; i < fm->histogram_size-1; i++) {
		partition_struct *ps = &partition_cost[i];
		double fraction = (ps->next_partitions*ps->inserts*(rr+rw))/total_insert_cost;
		double ghost_assignment = (double) gv * fraction;
		ps->ghost_values = floor(ghost_assignment);
		int inserts_remaining = ps->inserts - ps->ghost_values;
		double dynamic_partition_cost = 0;
		if(inserts_remaining > 0) {
			dynamic_partition_cost = inserts_remaining*ps->next_partitions*(rr+rw);
		}

		if(ps->next_neighbor){
			partition_struct *next = ps->next_neighbor;
			int additional_blocks = (next->max_block-ps->min_block);
			double score = 0.0;
			for(int i = 0, index = ps->min_block; i <= additional_blocks; i++, index++) {
				//compute the cost of removing the partition boundary in terms of additional read penalty
				score += fb->backward[index]*i*sr;
				score += fb->forward[index]*(additional_blocks-i)*sr;
			}
			//the score is the difference between the data movement cost of having the partition boundary,
			//and the read penalty of not having the partition boundary
			ps->score = (ps->partition_static_cost+dynamic_partition_cost)-score;
		} else {
			ps->score = -DBL_MAX;
		}
		if(ps->score > high_hitter->score) {
			high_hitter = ps;
		}
	}

	partition_struct* head = &partition_cost[0];
	while(high_hitter->score > 0) {
		//3. Merge highest score with neighbor
		partition_struct *first = high_hitter;
		partition_struct *next = high_hitter->next_neighbor;
		first->max_block = next->max_block;
		first->next_neighbor = next->next_neighbor;
		first->partition_static_cost = next->partition_static_cost;
		first->max_val = next->max_val;
		first->inserts += next->inserts;
		first->part_size += next->part_size;
		//delete next from the neighbor list (we keep first as the new merged partition)
		if(next->next_neighbor) {
			if(next->prev_neighbor) {
				next->prev_neighbor->next_neighbor = next->next_neighbor;
				next->next_neighbor->prev_neighbor = next->prev_neighbor;
			} else {
				//next is first in the neighbor list
				next->next_neighbor->prev_neighbor = NULL;
			}
		} else if(next->prev_neighbor) {
			//next is last in the neighbor list
			next->prev_neighbor->next_neighbor = NULL;
		}


		//Recompute first->prefix_in_back where needed
		partition_struct *pointer = first;
		while(pointer) {
			//the merge means one less next partition for each previous partition as well as the newly merged one.
			pointer->next_partitions -= 1;
			//each previous partition adds an rr and a rw to the total cost of each insert (the static part is handled elsewhere)
			double insert_cost = pointer->next_partitions*pointer->inserts*(rr+rw);

			if(pointer->next_neighbor) {
				next->prefix_in_back = insert_cost+next->next_neighbor->prefix_in_back;
			} else {
				next->prefix_in_back = insert_cost;
			}
			if(!pointer->prev_neighbor){
				//last so we record the total score
				total_insert_cost = pointer->prefix_in_back;
			}
			pointer = pointer->prev_neighbor;

		}
		//Recompute all scores
		pointer = head;
		high_hitter = head;
		while(pointer) {
			partition_struct *ps = pointer;
			double fraction = (ps->next_partitions*ps->inserts*(rr+rw))/total_insert_cost;
			double ghost_assignment = (double) gv * fraction;
			ps->ghost_values = floor(ghost_assignment);
			int inserts_remaining = ps->inserts - ps->ghost_values;
			double dynamic_partition_cost = 0;
			if(inserts_remaining > 0) {
				dynamic_partition_cost = inserts_remaining*ps->next_partitions*(rr+rw);
			}

			if(ps->next_neighbor){
				partition_struct *next = ps->next_neighbor;
				int additional_blocks = (next->max_block-ps->min_block);
				double score = 0.0;
				for(int i = 0, index = ps->min_block; i <= additional_blocks; i++, index++) {
					//compute the cost of removing the partition boundary in terms of additional read penalty
					score += fb->backward[index]*i*sr;
					score += fb->forward[index]*(additional_blocks-i)*sr;
				}
				//the score is the difference between the data movement cost of having the partition boundary,
				//and the read penalty of not having the partition boundary
				ps->score = (ps->partition_static_cost+dynamic_partition_cost)-score;
			} else {
				ps->score = -DBL_MAX;
			}
			if(ps->score > high_hitter->score) {
				high_hitter = ps;
			}
			pointer = pointer->next_neighbor;
		}
	}


	return head;
}
#endif

frequency_model *sorted_data_frequency_model(const int* data_in, size_t data_size, const int* type, const int* first, const int* second, size_t work_size) {
	int block_size = 8;
	//double seq_write = 2;
	data data;
	data.array = (int *) malloc(data_size*sizeof(int));
	data.size = data_size;
	memcpy(data.array,data_in,data_size*sizeof(int));
	qsort(data.array,data.size,sizeof(int),cmpfunc);
	frequency_model *fm = malloc(sizeof(frequency_model));

	initialize_frequency_model(fm, data.size,block_size);

	//Workload simulator
	build_frequency_model_api(&data,work_size,type,first,second, fm);
	free(data.array);
	return fm;

}

void partition_data(frequency_model* fm,const int algo, Partition_inst *out, size_t data_size) {

	//-d /home/kenneth/test/data -l /home/kenneth/test/workload -a td -t 8 -b 2 -r 1 -R 2 -w 2 -W 4 -o /home/kenneth/test/result

	//-d /home/kenneth/test/data -l /home/kenneth/test/workload -a td -t 8 -b 2 -r 1 -R 2 -w 2 -W 4 -o /home/kenneth/test/result
	double rand_read = 20;
	double rand_write = 4;
	double seq_read = 10;
	prefix prefix_sum;
	prefix_sum.de = (int *) calloc(fm->histogram_size , sizeof(int));
	prefix_sum.in = (int *) calloc(fm->histogram_size , sizeof(int));
	prefix_sum.histogram_size = fm->histogram_size;
	//initialize_frequency_model(&prefix_sum, data.size,block_size);
	//Build prefix sum
	compute_prefix_sum(fm,&prefix_sum);
	//TODO: Compute the cost of data movement per partition boundary - use the prefix sum.
	forward_backward fb;
	fb.backward = (int*) calloc(fm->histogram_size,sizeof(int));
	fb.forward = (int*) calloc(fm->histogram_size,sizeof(int));
	partition_struct *partition_cost = (partition_struct *) malloc(fm->histogram_size*sizeof(partition_struct));
	setup_partitions(fm,partition_cost,data_size);
	compute_costs(&fb, partition_cost, fm, &prefix_sum, rand_read, rand_write, seq_read);
	//TODO: Build Algorithm in three versions, top down, bottom up and brute force
	partition_struct* bottom_up_cost;
	if(algo == 0) {
		bottom_up_cost = compute_partitioning_bottom_up(&fb, partition_cost, fm, seq_read, data_size);
	} else {
		printf("unknown algorithm number: %i\n",algo);
		printf("Defaulting to buttom up!\n");
		bottom_up_cost = compute_partitioning_bottom_up(&fb, partition_cost, fm, seq_read, data_size);
	}

	int part_size = 0;
	partition_struct *pointer = bottom_up_cost;
	while(pointer) {
		part_size++;
		//printf("%f,%i,%i,%i\t",pointer->score,pointer->min_block,pointer->max_block,pointer->max_val);
		pointer = pointer->next_queue;
	}
	out->pivots = (int *) malloc(part_size*sizeof(int));
	out->p_count = part_size;
	out->part_sizes = (int *) malloc(part_size*sizeof(int));
	pointer = bottom_up_cost;
	part_size = 0;
	while(pointer) {
		out->pivots[part_size] = pointer->max_val;
		out->part_sizes[part_size] = pointer->part_size;
		part_size++;
		pointer = pointer->next_queue;
	}

	qsort(out->pivots,out->p_count,sizeof(int),cmpfunc);
	free(fb.backward);
	free(fb.forward);
	free(partition_cost);
	free(prefix_sum.de);
	free(prefix_sum.in);
	//free_frequency_model(&fm);
}

#ifdef GHOST_VALUE
void partition_data_gv(frequency_model *fm, const int data_size, const int algo, const int ghost_values, Partition_inst *out) {

	double rand_read = 20;
	double rand_write = 4;
	double seq_read = 10;

	partition_struct *partition_cost = (partition_struct *) malloc(fm->histogram_size*sizeof(partition_struct));

	prefix prefix_sum;
	prefix_sum.de = (int *) calloc(fm->histogram_size , sizeof(int));
	prefix_sum.in = (int *) calloc(fm->histogram_size , sizeof(int));
	prefix_sum.histogram_size = fm->histogram_size;
	//initialize_frequency_model(&prefix_sum, data.size,block_size);
	//Build prefix sum
	compute_prefix_sum(fm,&prefix_sum);
	//TODO: Compute the cost of data movement per partition boundary - use the prefix sum.
	forward_backward fb;
	fb.backward = (int*) calloc(fm->histogram_size,sizeof(int));
	fb.forward = (int*) calloc(fm->histogram_size,sizeof(int));
	compute_costs(&fb, partition_cost, fm, &prefix_sum, rand_read, rand_write, seq_read);
	//TODO: Build Algorithm in three versions, top down, bottom up and brute force
	partition_struct* bottom_up_cost = compute_partitioning_bottom_up_gv(&fb, partition_cost, fm,seq_read,rand_read,rand_write,ghost_values,data_size);
	int part_size = 0;
	partition_struct *pointer = bottom_up_cost;
	while(pointer) {
		part_size++;
		//printf("%f,%i,%i,%i\t",pointer->score,pointer->min_block,pointer->max_block,pointer->max_val);
		pointer = pointer->next_queue;
	}
	out->pivots = (int *) malloc(part_size*sizeof(int));
	out->p_count = part_size;
	out->part_sizes = (int *) malloc(part_size*sizeof(int));
	out->ghost_count = (int *) malloc(part_size*sizeof(int));
	pointer = bottom_up_cost;
	part_size = 0;
	while(pointer) {
		out->pivots[part_size] = pointer->max_val;
		out->pivots[part_size] = pointer->part_size;
		out->ghost_count[part_size] = pointer->ghost_values;
		part_size++;
		pointer = pointer->next_queue;
	}

	qsort(out->pivots,out->p_count,sizeof(int),cmpfunc);
	free(fb.backward);
	free(fb.forward);
	free(partition_cost);
	free(prefix_sum.de);
	free(prefix_sum.in);
}
#endif
