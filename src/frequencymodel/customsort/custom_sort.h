/*
 * custom_sort.h
 *
 *  Created on: Feb 9, 2016
 *      Author: kenneth
 */

#ifndef UTIL_CUSTOM_SORT_H_
#define UTIL_CUSTOM_SORT_H_

#include <alloca.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include "../data_structures.h"
#if __cplusplus
extern "C" {
#endif


typedef int (*__compar_custom) (const void *, const void *);
typedef void (*__custom_swap) (char* ,char* );
void quicksort_custom (void *const pbase, size_t total_elems, size_t size, __compar_custom cmp, __custom_swap SWAP);
#if __cplusplus
}
#endif

#endif /* UTIL_CUSTOM_SORT_H_ */
