#include "utils.h"
#include <stdlib.h>

// #define LOG
// #define LOG_ERR
// #define LOG_INFO



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
	return;
}


void doSomething(int *op_type, int *num1, int *num2, unsigned int lineCount) {
	int *pq = malloc(sizeof(int) * 100000000);
	int *rq = malloc(sizeof(int) * 100000000);
	int *in = malloc(sizeof(int) * 100000000);
	int *up = malloc(sizeof(int) * 100000000);
	int *de = malloc(sizeof(int) * 100000000);
	memset(pq, 0, 100000000);
	memset(rq, 0, 100000000);
	memset(in, 0, 100000000);
	memset(up, 0, 100000000);
	memset(de, 0, 100000000);
	printf("inside function memset done\n");
	for (uint i = 0; i < lineCount; i++) {
		switch (op_type[i]) {
			case 0: {
				int k = num1[i] / 100;
				if (k >= 100000000) {
					k = 100000000 - 1;
					pq[k]++;
				}
				else {
					pq[k]++;	
				}
				break;
			}
			case 1: {
				int k = num1[i] / 100;
				int j = num2[i] / 100;
				if (k >= 100000000 || j >= 100000000) {
					k = 100000000 - 1;
					rq[k]++;
				}
				else {
					for (int x = k; x <= j; x++)
						rq[x]++;	
				}
				break;
			}
			case 2: {
				int k = num1[i] / 100;
				if (k >= 100000000) {
					k = 100000000 - 1;
					in[k]++;
				}
				else {
					in[k]++;	
				}
				break;
			}
			case 3: {
				int k = num1[i] / 100;
				if (k >= 100000000) {
					k = 100000000 - 1;
					up[k]++;
				}
				else {
					up[k]++;	
				}
				k = num2[i] / 100;
				if (k >= 100000000) {
					k = 100000000 - 1;
					up[k]++;
				}
				else {
					up[k]++;	
				}
				break;
			}
			case 4: {
				int k = num1[i] / 100;
				if (k >= 100000000) {
					k = 100000000 - 1;
					de[k]++;
				}
				else {
					de[k]++;	
				}
				break;
			}
			default: {
				break;
			}
		}	
	}
	printf("sort done\n");
	int len = lineCount;
	FILE *f = fopen("data/pq", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(pq, sizeof(int), len, f);
	}
	free(pq);
	fclose(f);
	f = fopen("data/rq", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(rq, sizeof(int), len, f);
	}
	fclose(f);
	free(rq);
	f = fopen("data/in", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(in, sizeof(int), len, f);
	}
	fclose(f);
	free(in);
	f = fopen("data/up", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(up, sizeof(int), len, f);
	}
	fclose(f);
	free(up);
	f = fopen("data/de", "w+");
	if (NULL != f){
		fwrite(&len, sizeof(len), 1, f);
		fwrite(de, sizeof(int), len, f);
	}
	fclose(f);
	free(de);
}