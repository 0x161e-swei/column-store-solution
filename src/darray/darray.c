#include "darray.h"


DArray_INT *darray_create(size_t reserve_size) {
	static DArray_INT generic_int_array = {	.element_size = sizeof(int), 
										.capacity = 0, .length = 0, 
										.content = NULL,
										};

	DArray_INT *arr = malloc(sizeof(DArray_INT));
	check(NULL != arr, "Out of memory");

	memcpy(arr, &generic_int_array, sizeof(DArray_INT));

	_darray_reserve(arr, reserve_size);

	return arr;

	error:
	return NULL;
}

bool _darray_reserve(DArray_INT *arr, size_t n) {
	check(n > arr->capacity, "reserved size smaller than capacity");
	size_t size = 1024;
	while (size < n) size <<= 1;
	int *tmp_ptr = realloc(arr->content, size * arr->element_size);
	check(NULL != tmp_ptr, "Out of memory");
	memset(tmp_ptr, 0, size * arr->element_size);
	arr->content = tmp_ptr;
	arr->capacity = size;
	return true;

	error: 
	return false;
}

bool _darray_expand(DArray_INT *arr)
{
	// if (arr->length + 1 > arr->capacity) {
		int *ptr = NULL;
		size_t n = (arr->capacity) << 1;
		ptr = realloc(arr->content, n * arr->element_size);
		check(NULL != ptr, "Out of memory");
		// printf("expand from %zu to %zu \n", arr->length, n);
		arr->content = ptr;
		arr->capacity = n;
	// }
	return true;

	error:
	return false;
}

bool darray_destory(DArray_INT *arr) {
	check(NULL != arr && NULL != arr->content, "Attemp to free invaild mem!");
	free(arr->content);
	free(arr);
	// memset(arr, 0, sizeof(DArray));
	return true;

	error:
	return false;
}

void _darray_vec_push(DArray_INT *arr, const void *src, size_t n) {
	// size_t i = arr->length, j = 0;
	// for (; i < arr->length + n * arr->element_size; i++, j++) {
	// 	*((char *)arr->content + i) = *((char *)src + j);
	// }
	memcpy(&arr->content[arr->length], 
		src, 
		n * arr->element_size);
	arr->length += n;
}