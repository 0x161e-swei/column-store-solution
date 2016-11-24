#ifndef DUMMY_PARTITION_H_
#define DUMMY_PARTITION_H_

#include <stdlib.h>

typedef struct _partition_inst{
	// pivot count
	int p_count;
	int *part_sizes;
	int *pivots;
	#ifdef GHOST_VALUE
	int *ghost_count;
	#endif
} Partition_inst;

inline void *sorted_data_frequency_model(const int* data_in __attribute__((unused)),
					size_t data_size __attribute__((unused)),
					const int* type __attribute__((unused)),
					const int* first __attribute__((unused)),
					const int* second __attribute__((unused)),
					size_t work_size __attribute__((unused)))
{return NULL;}

inline void free_frequency_model(void *fm __attribute__((unused))) {}



#ifdef GHOST_VALUE
void partition_data_gv();
#endif

inline void partition_data(void* fm __attribute__((unused)),
			const int algo __attribute__((unused)),
			Partition_inst *out __attribute__((unused)),
			size_t data_size __attribute__((unused)))
{}
// void partition_data(frequency_model* fm, const int algo, Partition_inst *out, size_t data_size);

#endif /* DUMMY_PARTITION_H_ */
