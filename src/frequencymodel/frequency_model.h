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

void partition_data(const int* data_in, size_t size,const int* type, const int* first, const int* second, size_t work_size, Partition_inst *out);



#endif /* FREQUENCY_MODEL_H_ */
