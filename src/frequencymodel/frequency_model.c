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

static inline void setup_partition_structure(const int block_counter, const int data_counter,const int histogram_size, int* high_val, data* data, partition_struct *ps){

	high_val[block_counter] = data->array[data_counter];
	//initialize the partition representation

	ps[block_counter].max_val = data->array[data_counter];
	ps[block_counter].max_block = block_counter;
	ps[block_counter].min_block = block_counter;
	ps[block_counter].score = 0.0;
	ps[block_counter].partition_static_cost = 0.0;
#ifdef GHOST_VALUE
	ps[block_counter].partition_dynamic_cost = 0.0;
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


void build_frequency_model_api(data* data,const int size, const int* type, const int* first, const int* second,frequency_model *fm, partition_struct *ps) {
	//TODO: Consider how this may be done in parallel - stdatomic.h seems like a place to start
	//TODO: Consider non-distinct value case
	//TODO: apply workload to data, record result in frequency model.
	//extract the high value of each block
	int *high_val = (int *) calloc(fm->histogram_size,sizeof(int));
	for(int i = fm->block_size, j=0; i < (int)data->size; i+=fm->block_size, j++){
		setup_partition_structure(j,i,fm->histogram_size,high_val,data,ps);
	}
	if(data->size % fm->block_size != 0){
		//catch the last block in case data->size % fm->block_size != 0
		setup_partition_structure(fm->histogram_size-1,data->size-1,fm->histogram_size,high_val,data,ps);
	}

	for(int i = 0; i < size; i++) {
		if(type[i] == 0) {
			//point queries
			simple_operation_api(high_val,first[i],fm->histogram_size,fm->pq);
		} else if(type[i] == 1){
			//range queries
			range_query_api(high_val,first[i],second[i],fm);
		} else if(type[i] == 2){
			//insert
			simple_operation_api(high_val,first[i],fm->histogram_size,fm->in);
		} else if(type[i] == 3){
			//updates
			update_api(high_val,first[i],second[i],fm);
		} else if(type[i] == 4){
			//deletes
			simple_operation_api(high_val,first[i],fm->histogram_size,fm->de);
		} else {
			printf("unknown");
		}
	}
#ifdef GHOST_VALUE
	//record the amount of inserts per partition as these will be used to compute the dynamic cost of each partition
	for(int i = 0; i < fm->histogram_size; i++){
		ps[i].inserts = fm->in[i];
	}
#endif
	//for(int i = 0; i < fm->histogram_size; i++){
	//printf("%i\t%i\t%i\t%i\t%i\t%i\t%i\t%i\t%i\n",fm->de[i],fm->in[i],fm->pq[i],fm->re[i],fm->rs[i],fm->sc[i],fm->ub[i],fm->ud[i],fm->uf[i]);
	//}
	free(high_val);

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

static inline void compute_score_gv(partition_struct *ps, const forward_backward *fb, const double sr, int* prefix, int index) {
	if(index == 0) {
		//prefix[index] =
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
			next->next_queue->prev_queue = next->prev_queue;
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
		const frequency_model *fm, const double sr) {
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

	}
	partition_cost[fm->histogram_size-1].score = -DBL_MAX;
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


partition_struct* compute_partitioning_bottom_up_gv(const forward_backward *fb, partition_struct* partition_cost,
		const frequency_model *fm, const double sr, const int gv) {
	//this is not as optimal as it should be. We should keep a sorted queue of all partitions that have a positive potential impact.

	//partition_struct* tail = &partition_cost[fm->histogram_size-1];
	//generate a priority queue
	/*
	 * Algorithm outline:
	 *
	 *
	 *
	 * 1. scan backwards and compute dynamic cost of the partition, as well as the scores
	 * 		a. track the one with the best score.
	 * 		b. create accumulated cost of inserts in a "backwards" prefix sum array
	 * 2. Merge the high hitter with its neighbor
	 * 3. Scan back to recompute individual scores for previous inserts (remember to update prefix sum).
	 * 4. Scan forwards to recompute scores for all partitions taking the new total score into account.
	 * 	a. again track the high hitter
	 * 5. repeat from 2 until the high-hitter have a negative score.
	 *
	 */

	int* prefix_in_back = (int *) calloc(fm->histogram_size,sizeof(int));
	//1. compute priority queue score for each partition as the IOs "saved" by merging with the neighbor to the right.

	for(int i = 0; i < fm->histogram_size-1; i++) {
		compute_score_gv(&partition_cost[i], fb, sr, prefix_in_back, i);
	}


	partition_cost[fm->histogram_size-1].score = -DBL_MAX;
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



void partition_data(const int* data_in, size_t size,const int* type, const int* first, const int* second, size_t work_size, const int algo, Partition_inst *out) {

	//-d /home/kenneth/test/data -l /home/kenneth/test/workload -a td -t 8 -b 2 -r 1 -R 2 -w 2 -W 4 -o /home/kenneth/test/result

	//-d /home/kenneth/test/data -l /home/kenneth/test/workload -a td -t 8 -b 2 -r 1 -R 2 -w 2 -W 4 -o /home/kenneth/test/result

	int block_size = 8;
	double rand_read = 20;
	double rand_write = 4;
	double seq_read = 10;
	//double seq_write = 2;
	data data;
	data.array = (int *) malloc(size*sizeof(int));
	data.size = size;
	memcpy(data.array,data_in,size*sizeof(int));
	qsort(data.array,data.size,sizeof(int),cmpfunc);
	frequency_model fm;

	initialize_frequency_model(&fm, data.size,block_size);
	partition_struct *partition_cost = (partition_struct *) malloc(fm.histogram_size*sizeof(partition_struct));
	//Workload simulator
	build_frequency_model_api(&data,work_size,type,first,second, &fm, partition_cost);

	prefix prefix_sum;
	prefix_sum.de = (int *) calloc(fm.histogram_size , sizeof(int));
	prefix_sum.in = (int *) calloc(fm.histogram_size , sizeof(int));
	prefix_sum.histogram_size = fm.histogram_size;
	//initialize_frequency_model(&prefix_sum, data.size,block_size);
	//Build prefix sum
	compute_prefix_sum(&fm,&prefix_sum);
	//TODO: Compute the cost of data movement per partition boundary - use the prefix sum.
	forward_backward fb;
	fb.backward = (int*) calloc(fm.histogram_size,sizeof(int));
	fb.forward = (int*) calloc(fm.histogram_size,sizeof(int));
	compute_costs(&fb, partition_cost, &fm, &prefix_sum, rand_read, rand_write, seq_read);
	//TODO: Build Algorithm in three versions, top down, bottom up and brute force
	partition_struct* bottom_up_cost;
	if(algo == 0) {
		bottom_up_cost = compute_partitioning_bottom_up(&fb, partition_cost, &fm, seq_read);
	} else {
		printf("unknown algorithm number: %i\n",algo);
		printf("Defaulting to buttom up!\n");
		bottom_up_cost = compute_partitioning_bottom_up(&fb, partition_cost, &fm, seq_read);
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
	pointer = bottom_up_cost;
	part_size = 0;
	while(pointer) {
		out->pivots[part_size] = pointer->max_val;
		part_size++;
		pointer = pointer->next_queue;
	}

	qsort(out->pivots,out->p_count,sizeof(int),cmpfunc);
	free(data.array);
	free(fb.backward);
	free(fb.forward);
	free(partition_cost);
	free(prefix_sum.de);
	free(prefix_sum.in);
	free_frequency_model(&fm);
}

void partition_data_gv(const int* data_in, size_t size,const int* type, const int* first, const int* second, size_t work_size, const int algo, const int ghost_values, Partition_inst *out) {

	//-d /home/kenneth/test/data -l /home/kenneth/test/workload -a td -t 8 -b 2 -r 1 -R 2 -w 2 -W 4 -o /home/kenneth/test/result

	//-d /home/kenneth/test/data -l /home/kenneth/test/workload -a td -t 8 -b 2 -r 1 -R 2 -w 2 -W 4 -o /home/kenneth/test/result

	int block_size = 8;
	double rand_read = 20;
	double rand_write = 4;
	double seq_read = 10;
	//double seq_write = 2;
	data data;
	data.array = (int *) malloc(size*sizeof(int));
	data.size = size;
	memcpy(data.array,data_in,size*sizeof(int));
	qsort(data.array,data.size,sizeof(int),cmpfunc);
	frequency_model fm;

	initialize_frequency_model(&fm, data.size,block_size);
	partition_struct *partition_cost = (partition_struct *) malloc(fm.histogram_size*sizeof(partition_struct));
	//Workload simulator
	build_frequency_model_api(&data,work_size,type,first,second, &fm, partition_cost);

	prefix prefix_sum;
	prefix_sum.de = (int *) calloc(fm.histogram_size , sizeof(int));
	prefix_sum.in = (int *) calloc(fm.histogram_size , sizeof(int));
	prefix_sum.histogram_size = fm.histogram_size;
	//initialize_frequency_model(&prefix_sum, data.size,block_size);
	//Build prefix sum
	compute_prefix_sum(&fm,&prefix_sum);
	//TODO: Compute the cost of data movement per partition boundary - use the prefix sum.
	forward_backward fb;
	fb.backward = (int*) calloc(fm.histogram_size,sizeof(int));
	fb.forward = (int*) calloc(fm.histogram_size,sizeof(int));
	compute_costs(&fb, partition_cost, &fm, &prefix_sum, rand_read, rand_write, seq_read);
	//TODO: Build Algorithm in three versions, top down, bottom up and brute force
	partition_struct* bottom_up_cost = compute_partitioning_bottom_up(&fb, partition_cost, &fm, seq_read);


	int part_size = 0;
	partition_struct *pointer = bottom_up_cost;
	while(pointer) {
		part_size++;
		//printf("%f,%i,%i,%i\t",pointer->score,pointer->min_block,pointer->max_block,pointer->max_val);
		pointer = pointer->next_queue;
	}
	out->pivots = (int *) malloc(part_size*sizeof(int));
	out->p_count = part_size;
	pointer = bottom_up_cost;
	part_size = 0;
	while(pointer) {
		out->pivots[part_size] = pointer->max_val;
		part_size++;
		pointer = pointer->next_queue;
	}

	qsort(out->pivots,out->p_count,sizeof(int),cmpfunc);
	free(data.array);
	free(fb.backward);
	free(fb.forward);
	free(partition_cost);
	free(prefix_sum.de);
	free(prefix_sum.in);
	free_frequency_model(&fm);
}


