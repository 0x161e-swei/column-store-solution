/*
 * custom_swappers.c
 *
 *  Created on: May 9, 2016
 *      Author: kenneth
 */

#include "custom_swappers.h"
#include "../data_structures.h"
#include <string.h>
void aof_swap(char* a,char* b) {
	partition_inst_aos __temp;
	memcpy(&__temp, ((partition_inst_aos *) a), sizeof(partition_inst_aos));
	memcpy(((partition_inst_aos *) a), ((partition_inst_aos *) b), sizeof(partition_inst_aos));
	memcpy(((partition_inst_aos *) b), & __temp, sizeof(partition_inst_aos));
}

/* Byte-wise swap two items of size SIZE. */
void partition_struct_swap(char* a,char* b) {
	//pointer_swap(a,b);
	partition_struct __temp;
	partition_struct *__a = ((partition_struct *) a);
	partition_struct *__b = ((partition_struct *) b);
	if(__a->next_neighbor == __b || __b->next_neighbor == __a || __b->prev_neighbor == __a || __a->prev_neighbor == __b) {
		if(__a->next_neighbor == __b) {
			//a before b
			partition_struct *__a_prev = __a->prev_neighbor;
			partition_struct *__b_next = __b->next_neighbor;
			__temp = * ((partition_struct *) a);
			*((partition_struct *) a) = *((partition_struct *) b);
			*((partition_struct *) b) = __temp;
			//now __b points to a and vise versa
			__a = ((partition_struct *) b);
			__b = ((partition_struct *) a);
			__b->prev_neighbor = __a;
			__a->next_neighbor = __b;
			if(__a_prev){
				__a_prev->next_neighbor = __a;
			}
			if(__b_next){
				__b_next->prev_neighbor = __b;
			}
		} else {
			//b before a
			partition_struct *__b_prev = __b->prev_neighbor;
			partition_struct *__a_next = __a->next_neighbor;
			__temp = * ((partition_struct *) a);
			*((partition_struct *) a) = *((partition_struct *) b);
			*((partition_struct *) b) = __temp;
			//now __b points to a and vise versa
			__a = ((partition_struct *) b);
			__b = ((partition_struct *) a);
			__b->next_neighbor = __a;
			__a->prev_neighbor = __b;
			if(__b_prev) {
				__b_prev->next_neighbor = __b;
			}
			if(__a_next) {
				__a_next->prev_neighbor = __a;
			}
		}
	}
	else {

		if(__a->next_neighbor){
			__a->next_neighbor->prev_neighbor = ((partition_struct *) b);
		}
		if(__a->prev_neighbor){
			__a->prev_neighbor->next_neighbor = ((partition_struct *) b);
		}
		if(__b->next_neighbor){
			__b->next_neighbor->prev_neighbor = ((partition_struct *) a);
		}
		if(__b->prev_neighbor) {
			__b->prev_neighbor->next_neighbor = ((partition_struct *) a);
		}

		__temp = * ((partition_struct *) a);
		*((partition_struct *) a) = *((partition_struct *) b);
		*((partition_struct *) b) = __temp;
	}
}


