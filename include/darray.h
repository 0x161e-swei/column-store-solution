#ifndef _DARRAY_H_
#define _DARRAY_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>


#define darray_log_err(M) \
	fprintf(stderr, "[ERROR] (%s:%d) " M "\n", __FILE__, __LINE__)

#define darray_log_info(M) \
	fprintf(stderr, "[INFO] (%s:%d) " M "\n", __FILE__, __LINE__)

#define check(A, M) \
	do{ \
		if(!(A)) { \
			darray_log_err(M); \
			goto error; \
		} \
	}while(0)


#define darray_push(data, val) \
	do { \
		if ((data)->length + 1 > (data)->capacity) { \
			_darray_expand(data); \
		} \
		(data)->content[(data)->length++] = val; \
	} while(0)

#define darray_vec_push(data, src, n) \
	do { \
		while ((data)->length + n > (data)->capacity) { \
			_darray_expand(data); \
		} \
		_darray_vec_push(data, src, n); \
	} while(0)


#define darray_pop(data) \
	(data)->content[--(data)->length]


typedef struct _darray_int {
	const size_t element_size;
	unsigned int capacity;
	unsigned int length;
	int *content;
} DArray_INT;

DArray_INT *darray_create(unsigned int reserve_size);

bool _darray_reserve(DArray_INT *arr, unsigned int n);

bool _darray_expand(DArray_INT *arr);

bool darray_destory(DArray_INT *arr);

void _darray_vec_push(DArray_INT *arr, const void *src, unsigned int n);

#endif /* _DARRAY_H_ */
