#ifndef ___TREESEARCH_
#define ___TREESEARCH_ 
#include <time.h>

#define INTERVAL 30000
#define TESTNUMBER 2000 

typedef unsigned int uint;
void clock_timediff(struct timespec start, struct timespec end);

#ifdef DDEBUG
#define d(...) fprintf(stderr, __VA_ARGS__)
#else
#define d(...)
#endif

#endif /* ___TREESEARCH_ */

