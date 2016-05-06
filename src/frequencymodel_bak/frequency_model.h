/*
 * frequency_model.h
 *
 *  Created on: Jan 21, 2016
 *      Author: kenneth
 */

#ifndef FREQUENCY_MODEL_H_
#define FREQUENCY_MODEL_H_
#include "data_structures.h"
#include "customsort/custom_sort.h"
//#define GHOST_VALUE
frequency_model *sorted_data_frequency_model(const int* data_in, size_t data_size, const int* type, const int* first, const int* second, size_t work_size);
void partition_data_gv(frequency_model *fm, const int data_size, const int algo, const int ghost_values, Partition_inst *out);
void partition_data(frequency_model* fm,const int algo, Partition_inst *out, size_t data_size);
void free_frequency_model(frequency_model *fm);



#endif /* FREQUENCY_MODEL_H_ */
