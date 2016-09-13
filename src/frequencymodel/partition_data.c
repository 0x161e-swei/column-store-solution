/*
 * partition_data.c
 *
 *  Created on: May 9, 2016
 *      Author: kenneth
 */
#include "partition_data.h"
#include "customsort/custom_sort.h"
#include "customsort/custom_comparators.h"
#include "customsort/custom_swappers.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

void setup_partitions(frequency_model *fm, partition_struct *ps) {
	for(int j=0; j < fm->histogram_size; j++){
		setup_partition_structure(j,fm->max_val[j],fm->histogram_size,ps);
#ifdef GHOST_VALUE
		//record the amount of inserts per partition as these will be used to compute the dynamic cost of each partition
		ps[j].inserts = fm->in[j];
#endif
	}

}

void initialize_fb(forward_backward *fb, size_t size){
	fb->backward = (int*) calloc(size,sizeof(int));
	fb->forward = (int*) calloc(size,sizeof(int));
}
double compute_costs(forward_backward* fb, partition_struct* partition_cost, const frequency_model *fm,
		const prefix *prefix_sum, const double rr, const double rw, const double sr) {
	initialize_fb(fb,fm->histogram_size);
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
	partition_cost[fm->histogram_size-1].part_size = data_size - (fm->histogram_size - 1) * fm->block_size;
	//sort
	quicksort_custom(partition_cost,fm->histogram_size,sizeof(partition_struct),cmpfunc_ps,partition_struct_swap);
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

void free_prefix_sum(prefix *prefix_sum) {
	free(prefix_sum->de);
	free(prefix_sum->in);
}



void free_fb(forward_backward *fb) {
	free(fb->backward);
	free(fb->forward);
}

void partition_data(frequency_model* fm,const int algo, Partition_inst *out, size_t data_size) {

	double rand_read = 20;
	double rand_write = 4;
	double seq_read = 10;
	prefix prefix_sum;
	compute_prefix_sum(fm,&prefix_sum);

	partition_struct *partition_cost = (partition_struct *) malloc(fm->histogram_size*sizeof(partition_struct));
	setup_partitions(fm,partition_cost);
	forward_backward fb;
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

	pointer = bottom_up_cost;
	partition_inst_aos* sort_structure = (partition_inst_aos *) malloc(part_size*sizeof(partition_inst_aos));
	part_size = 0;
	while(pointer) {
		sort_structure[part_size].pivot = pointer->max_val;
		sort_structure[part_size].part_size = pointer->part_size;
		part_size++;
		pointer = pointer->next_queue;
	}
	//free stuff to minimize peak memory usage
	free_fb(&fb);
	free(partition_cost);
	free_prefix_sum(&prefix_sum);

	quicksort_custom(sort_structure,part_size,sizeof(partition_inst_aos),cmpfunc_aof,aof_swap);

	out->pivots = (int *) malloc(part_size*sizeof(int));
	out->p_count = part_size;
	out->part_sizes = (int *) malloc(part_size*sizeof(int));
	for(int i = 0; i < part_size; i++) {
		out->pivots[i] = sort_structure[i].pivot;
		out->part_sizes[i] = sort_structure[i].part_size;
	}
	free(sort_structure);
}

#ifdef GHOST_VALUE

void partition_data_gv(frequency_model *fm, const int data_size, const int algo, const int ghost_values, Partition_inst *out) {

	double rand_read = 20;
	double rand_write = 4;
	double seq_read = 10;

	partition_struct *partition_cost = (partition_struct *) malloc(fm->histogram_size*sizeof(partition_struct));

	prefix prefix_sum;
	//Build prefix sum
	compute_prefix_sum(fm,&prefix_sum);
	//TODO: Compute the cost of data movement per partition boundary - use the prefix sum.
	forward_backward fb;
	compute_costs(&fb, partition_cost, fm, &prefix_sum, rand_read, rand_write, seq_read);
	//TODO: Build Algorithm in three versions, top down, bottom up and brute force
	partition_struct* bottom_up_cost;
	if(algo ==0) {
		bottom_up_cost = compute_partitioning_bottom_up_gv(&fb, partition_cost, fm,seq_read,rand_read,rand_write,ghost_values,data_size);
	} else {
		printf("Warning: Unknown algo, running bottom up\n");
		bottom_up_cost = compute_partitioning_bottom_up_gv(&fb, partition_cost, fm,seq_read,rand_read,rand_write,ghost_values,data_size);
	}


	int part_size = 0;
	partition_struct *pointer = bottom_up_cost;
	while(pointer) {
		part_size++;
		//printf("%f,%i,%i,%i\t",pointer->score,pointer->min_block,pointer->max_block,pointer->max_val);
		pointer = pointer->next_queue;
	}

	pointer = bottom_up_cost;

	partition_inst_aos* sort_structure = (partition_inst_aos *) malloc(part_size*sizeof(partition_inst_aos));
	part_size = 0;
	while(pointer) {
		sort_structure[part_size].pivot = pointer->max_val;
		sort_structure[part_size].part_size = pointer->part_size;
		sort_structure[part_size].ghost_count = pointer->ghost_values;
		part_size++;
		pointer = pointer->next_queue;
	}
	//free stuff to minimize peak memory usage
	free_fb(&fb);
	free(partition_cost);
	free_prefix_sum(&prefix_sum);

	quicksort_custom(sort_structure,part_size,sizeof(partition_inst_aos),cmpfunc_aof,aof_swap);
	out->pivots = (int *) malloc(part_size*sizeof(int));
	out->p_count = part_size;
	out->part_sizes = (int *) malloc(part_size*sizeof(int));
	out->ghost_count = (int *) malloc(part_size*sizeof(int));
	for(int i = 0; i < part_size; i++) {
		out->pivots[i] = sort_structure[i].pivot;
		out->part_sizes[i] = sort_structure[i].part_size;
		out->ghost_count[i] = sort_structure[i].ghost_count;
	}
	free(sort_structure);

	out->pivots = (int *) malloc(part_size*sizeof(int));
	out->p_count = part_size;
	out->part_sizes = (int *) malloc(part_size*sizeof(int));
	//TODO: Align with partition data, and seperate identical parts!
}

#endif



