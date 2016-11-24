#include "custom_qsort.h"

void _c_qsort(int *arr, int len) {
	int sortingStack_left[QUEUE_LENGTH];
	int sortingStack_right[QUEUE_LENGTH];
	sortingStack_left[0] = 0;
	sortingStack_right[0] = len;
	int stack_index = 0;
	while(stack_index >= 0) {
		int l = sortingStack_left[stack_index];
		int r = sortingStack_right[stack_index] - 1;
		if (l < r) {
			// when there are only a few data to sort, usually they fit in one cache line
			if (r - l < QSORT_SWITCH_POINT) {
				//insertion sort
				for (int i = l; i < r; i++) {
					int min = arr[i];
					int idc = i;
					for (int j = i + 1; j <= r; j++) {
						if (arr[j] < min) {
							min = arr[j];
							idc = j;
						}
					}
					arr[idc] = arr[i];
					arr[i] = min;
				}
					
				stack_index--;
				continue;
			}

			int piv = arr[l];	
			while (l < r) {
				while (arr[r] >= piv && l < r) r--;
				if (l < r) {
					// swap data
					arr[l++] = arr[r];
				}
				while (arr[l] <= piv && l < r) l++;
				if (l < r) {
					// swap data
					arr[r--] = arr[l];
				}
			}
			arr[l] = piv;
			// Putting two part of the array into a stack
			// sorting on the larger part first
			if ((l - sortingStack_left[stack_index]) < (sortingStack_right[stack_index] - l - 1)) {
				sortingStack_left[stack_index + 1] = l + 1;
				sortingStack_right[stack_index + 1] = sortingStack_right[stack_index];
				sortingStack_right[stack_index++] = l;
				// by increasing stack_index, we go to the top of stack
			}
			else {
				sortingStack_left[stack_index + 1] = sortingStack_left[stack_index];
				sortingStack_right[stack_index + 1] = l;
				sortingStack_left[stack_index++] = l + 1;
				// by increasing stack_index, we go to the top of stack
			}
		}
		else {
			stack_index--;
		}
	}
}