/*
 * custom_swappers.h
 *
 *  Created on: May 9, 2016
 *      Author: kenneth
 */

#ifndef FREQUENCYMODEL_CUSTOMSORT_CUSTOM_SWAPPERS_H_
#define FREQUENCYMODEL_CUSTOMSORT_CUSTOM_SWAPPERS_H_

#if __cplusplus
extern "C" {
#endif


void aof_swap(char* a,char* b);
void partition_struct_swap(char* a,char* b);
#if __cplusplus
}
#endif
#endif /* FREQUENCYMODEL_CUSTOMSORT_CUSTOM_SWAPPERS_H_ */
