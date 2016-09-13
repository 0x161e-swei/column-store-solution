/*
 * custom_comparators.h
 *
 *  Created on: May 9, 2016
 *      Author: kenneth
 */

#ifndef FREQUENCYMODEL_CUSTOMSORT_CUSTOM_COMPARATORS_H_
#define FREQUENCYMODEL_CUSTOMSORT_CUSTOM_COMPARATORS_H_
#if __cplusplus
extern "C" {
#endif


int cmpfunc_int (const void * a, const void * b);
int cmpfunc_aof (const void * a, const void * b);
int cmpfunc_ps(const void *a, const void *b);
#if __cplusplus
}
#endif
#endif /* FREQUENCYMODEL_CUSTOMSORT_CUSTOM_COMPARATORS_H_ */
