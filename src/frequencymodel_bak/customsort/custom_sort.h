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

typedef int (*__compar_d_fn_t) (const void *, const void *, void *);
void quicksort_custom (void *const pbase, size_t total_elems, size_t size, __compar_d_fn_t cmp, void *arg);


#endif /* UTIL_CUSTOM_SORT_H_ */
