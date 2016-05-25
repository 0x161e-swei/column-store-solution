#include "utils.h"
#include <stdlib.h>

// #define LOG
// #define LOG_ERR
// #define LOG_INFO

struct timespec program_total;
struct timespec pq_total;
struct timespec rq_total;
struct timespec in_total;
struct timespec de_total;
struct timespec up_total;

#define BILLION 1000000000
struct timespec clock_timeadd(struct timespec t1, struct timespec t2)
{
    long sec = t2.tv_sec + t1.tv_sec;
    long nsec = t2.tv_nsec + t1.tv_nsec;
    if (nsec >= BILLION) {
        nsec -= BILLION;
        sec++;
    }
    return (struct timespec){ .tv_sec = sec, .tv_nsec = nsec };
}

struct timespec clock_timediff(struct timespec start, struct timespec end)
{
	struct timespec temp;
	if ((end.tv_nsec-start.tv_nsec)<0) {
		temp.tv_sec = end.tv_sec-start.tv_sec-1;
		temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec-start.tv_sec;
		temp.tv_nsec = end.tv_nsec-start.tv_nsec;
	}
	return temp;
	// printf("%ld.%ld\n", temp.tv_sec, temp.tv_nsec);
}

void cs165_log(FILE* out, const char *format, ...) {
#ifdef LOG
	va_list v;
	va_start(v, format);
	// fprintf(out, "[LOG] (%s:%d) ", __FILE__, __LINE__);
	vfprintf(out, format, v);
	va_end(v);
#else
	(void) out;
	(void) format;
#endif
}

// void log_err(const char *format, ...) {
// #ifdef LOG_ERR
//     va_list v;
//     va_start(v, format);
//     fprintf(stderr, ANSI_COLOR_RED);
//     // fprintf(stderr, "[ERROR] (%s:%d) ", __FILE__, __LINE__);
//     vfprintf(stderr, format, v);
//     fprintf(stderr, ANSI_COLOR_RESET);
//     va_end(v);
// #else
//     (void) format;
// #endif
// }

// void log_info(const char *format, ...) {
// #ifdef LOG_INFO
//     va_list v;
//     va_start(v, format);
//     fprintf(stdout, ANSI_COLOR_GREEN);
//     // fprintf(stdout, "[INFO] (%s:%d) ", __FILE__, __LINE__);
//     vfprintf(stdout, format, v);
//     fprintf(stdout, ANSI_COLOR_RESET);
//     fflush(stdout);
//     va_end(v);
// #else
//     (void) format;
// #endif
// }


void collect_file_info(const char* filename, unsigned int *lineCount, unsigned int *fieldCount) {
	FILE* fp = fopen(filename, "r");

	if (NULL != fp) {
		size_t len = 0;
		char *line = NULL;
		unsigned int fields = 1;
		ssize_t read;
		read = getline(&line, &len, fp);
		if (0 != read) {
			for (unsigned int i = 0; i < strlen(line); i++) {
				fields += (line[i] == ',');
			}
			if (line) free(line);
		}

		unsigned int lines = 0;
		while (EOF != (fscanf(fp, "%*[^\n]"), fscanf(fp, "%*c")))
			++lines;
		fclose(fp);
		*lineCount = lines;
		*fieldCount = fields;
	}
	else {
		*lineCount = -1;
		*fieldCount = -1;
	}
	log_info("file lines: %u\n", *lineCount);
	return;
}

#define NUMBEROFSLOTS 100000
#define SAMPLE_INTERVAL 10000
void doSomething(int *op_type, int *num1, int *num2, unsigned int lineCount) {
	int *pq_cost = malloc(sizeof(int) * lineCount);
	int *pq = malloc(sizeof(int) * NUMBEROFSLOTS);
	
	int *rq_beg = malloc(sizeof(int) * lineCount);
	int *rq_end = malloc(sizeof(int) * lineCount);
	int *rq = malloc(sizeof(int) * NUMBEROFSLOTS);

	int *in_cost = malloc(sizeof(int) * lineCount);
	int *in = malloc(sizeof(int) * NUMBEROFSLOTS);

	int *up_beg = malloc(sizeof(int) * lineCount);
	int *up_end = malloc(sizeof(int) * lineCount);
	int *up = malloc(sizeof(int) * NUMBEROFSLOTS);
	
	int *de_cost = malloc(sizeof(int) * lineCount);
	int *de = malloc(sizeof(int) * NUMBEROFSLOTS);

	int pq_count = 0;	
	int rq_count = 0;
	int up_count = 0;
	int in_count = 0;
	int de_count = 0;
	
	memset(pq_cost, 0, lineCount * sizeof(int));
	memset(pq, 0, NUMBEROFSLOTS * sizeof(int));

	memset(rq_beg, 0, lineCount * sizeof(int));
	memset(rq_end, 0, lineCount * sizeof(int));
	memset(rq, 0, NUMBEROFSLOTS * sizeof(int));

	memset(in_cost, 0, lineCount * sizeof(int));
	memset(in, 0, NUMBEROFSLOTS * sizeof(int));

	memset(up_beg, 0, lineCount * sizeof(int));
	memset(up_end, 0, lineCount * sizeof(int));
	memset(up, 0, NUMBEROFSLOTS * sizeof(int));

	memset(de_cost, 0, lineCount * sizeof(int));
	memset(de, 0, NUMBEROFSLOTS * sizeof(int));

	printf("inside function memset done\n");
	for (uint i = 0; i < lineCount; i++) {
		switch (op_type[i]) {
			// point query operation
			case 0: {
				pq_cost[pq_count] = num1[i];
				int k = num1[i] / SAMPLE_INTERVAL;
				if (k >= NUMBEROFSLOTS) {
					k = NUMBEROFSLOTS - 1;
					pq[k]++;
				}
				else {
					pq[k]++;
				}
				pq_count++;
				break;
			}
			// range query operation
			case 1: {
				int k = num1[i] / SAMPLE_INTERVAL;
				int j = num2[i] / SAMPLE_INTERVAL;
				if (k >= NUMBEROFSLOTS || j >= NUMBEROFSLOTS) {
					k = NUMBEROFSLOTS - 1;
					rq[k]++;
				}
				else {
					rq_beg[rq_count] = num1[i];
					rq_end[rq_count] = num2[i];
					for (int x = k; x <= j; x++)
						rq[x]++;
				}
				rq_count++;
				break;
			}
			// insert operation
			case 2: {
				in_cost[in_count] = num1[i];
				int k = num1[i] / SAMPLE_INTERVAL;
				if (k >= NUMBEROFSLOTS) {
					k = NUMBEROFSLOTS - 1;
					in[k]++;
				}
				else {
					in[k]++;
				}
				in_count++;
				break;
			}
			// update operation
			case 3: {
				int k = num1[i] / SAMPLE_INTERVAL;
				int j = num2[i] / SAMPLE_INTERVAL;
				if (k >= NUMBEROFSLOTS || j >= NUMBEROFSLOTS) {
					k = NUMBEROFSLOTS - 1;
					up[k]++;
				}
				else {
					up_beg[up_count] = num1[i];
					up_end[up_count] = num2[i];
					for (int x = k; x <= j; x++)
						up[x]++;
				}
				up_count++;
				break;
			}
			// delete operation
			case 4: {
				de_cost[de_count] = num1[i];
				int k = num1[i] / SAMPLE_INTERVAL;
				if (k >= NUMBEROFSLOTS) {
					k = NUMBEROFSLOTS - 1;
					de[k]++;
				}
				else {
					de[k]++;
				}
				de_count++;
				break;
			}
			default: {
				break;
			}
		}	
	}
	printf("sort done\n");
	int len = NUMBEROFSLOTS;
	FILE *f = fopen("data/pq", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(pq, sizeof(int), len, f);
	}
	free(pq);
	fclose(f);
	f = fopen("data/pq_cost", "w+");
	if (NULL != f){
		fwrite(&pq_count, sizeof(pq_count), 1, f);
		fwrite(pq_cost, sizeof(int), pq_count, f);
	}
	free(pq_cost);
	fclose(f);

	f = fopen("data/rq_cost_beg", "w+");
	if (NULL != f){
		fwrite(&rq_count, sizeof(rq_count), 1, f);
		fwrite(rq_beg, sizeof(int), rq_count, f);
	}
	free(rq_beg);
	fclose(f);
	f = fopen("data/rq_cost_end", "w+");
	if (NULL != f){
		fwrite(&rq_count, sizeof(rq_count), 1, f);
		fwrite(rq_end, sizeof(int), rq_count, f);
	}
	free(rq_end);
	fclose(f);
	f = fopen("data/rq", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(rq, sizeof(int), len, f);
	}
	fclose(f);
	free(rq);

	f = fopen("data/in_cost", "w+");
        if (NULL != f){
                fwrite(&in_count, sizeof(in_count), 1, f);
                fwrite(in_cost, sizeof(int), in_count, f);
        }
        free(in_cost);
        fclose(f);
	f = fopen("data/in", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(in, sizeof(int), len, f);
	}
	fclose(f);
	free(in);
	f = fopen("data/up_cost_beg", "w+");
	if (NULL != f){
		fwrite(&up_count, sizeof(up_count), 1, f);
		fwrite(up_beg, sizeof(int), up_count, f);
	}
	free(up_beg);
	fclose(f);
	f = fopen("data/up_cost_end", "w+");
	if (NULL != f){
		fwrite(&up_count, sizeof(up_count), 1, f);
		fwrite(up_end, sizeof(int), up_count, f);
	}
	free(up_end);
	fclose(f);
	f = fopen("data/up", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(up, sizeof(int), len, f);
	}
	fclose(f);
	free(up);

	f = fopen("data/de_cost", "w+");
        if (NULL != f){
                fwrite(&de_count, sizeof(de_count), 1, f);
                fwrite(de_cost, sizeof(int), de_count, f);
        }
        free(de_cost);
        fclose(f);
	f = fopen("data/de", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(de, sizeof(int), len, f);
	}
	fclose(f);
	free(de);
}
