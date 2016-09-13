/*
 * frequency_model.h
 *
 *  Created on: Jan 21, 2016
 *      Author: kenneth
 */

#ifndef FREQUENCY_MODEL_H_
#define FREQUENCY_MODEL_H_
#include "data_structures.h"
//#define GHOST_VALUE
#if __cplusplus
extern "C" {
#endif

frequency_model *sorted_data_frequency_model(const int* data_in, size_t data_size, const int* type, const int* first, const int* second, size_t work_size);
void free_frequency_model(frequency_model *fm);

#if __cplusplus
//we expose more to c++ to allow unit testing.
void simple_operation_api(const int* high_val, const int workload,
		const int histogram_size, int* histogram);
void range_query_api(const int* high_val,const int first, const int last,frequency_model *fm);
void update_api(const int* high_val, const int o, const int n,frequency_model *fm);
void initialize_frequency_model(frequency_model *fm, const int data_size, const int block_size);
void initialize_and_sort_data(data *data, const int* data_in, size_t data_size);
void build_frequency_model_api(data* data,const int size, const int* type, const int* first, const int* second,frequency_model *fm);
}
#endif


#endif /* FREQUENCY_MODEL_H_ */
