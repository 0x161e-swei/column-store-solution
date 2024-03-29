// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#define _GNU_SOURCE
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include "config.h"
#define ANSI_COLOR_RED		"\x1b[31m"
#define ANSI_COLOR_GREEN	"\x1b[32m"
#define ANSI_COLOR_BLUE		"\x1b[34m"
#define ANSI_COLOR_RESET	"\x1b[0m"


extern struct timespec program_total;
extern struct timespec pq_total;
extern struct timespec rq_total;
extern struct timespec in_total;
extern struct timespec de_total;
extern struct timespec up_total;

struct timespec clock_timeadd(struct timespec t1, struct timespec t2);
struct timespec clock_timediff(struct timespec start, struct timespec end);

// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
// void log_err(const char *format, ...);
#ifdef LOG_ERR
#define log_err(...) do {															\
	fprintf(stderr, ANSI_COLOR_RED);												\
	fprintf(stderr, "[ERROR] (%s:%d): %s(): ", __FILE__, __LINE__, __func__);	\
	fprintf(stderr, ##__VA_ARGS__);													\
	fprintf(stderr, ANSI_COLOR_RESET);												\
	fflush(stderr);																	\
} while (0)
#else
#define log_err(...) do{} while(0)
#endif


#ifdef DEBUG
#define debug(...) do{																\
	fprintf(stdout, ANSI_COLOR_BLUE);												\
	fprintf(stdout, "[DEBUG] (%s:%d): %s(): ", __FILE__, __LINE__,  __func__);	\
	fprintf(stdout, ##__VA_ARGS__);													\
	fprintf(stdout, ANSI_COLOR_RESET);												\
	fflush(stdout);																	\
}while(0)
#else
#define debug(...)
#endif 
// log_info(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
// void log_info(const char *format, ...);

#ifdef LOG_INFO
#define log_info(...) do {															\
	fprintf(stdout, ANSI_COLOR_GREEN);												\
	fprintf(stdout, "[INFO] (%s:%d): %s(): ", __FILE__, __LINE__, __func__);	\
	fprintf(stdout, ##__VA_ARGS__);													\
	fprintf(stdout, ANSI_COLOR_RESET);												\
	fflush(stdout);																	\
} while (0)
#else
#define log_info(...) do{} while(0)
#endif

void collect_file_info(const char* filename, unsigned int *lineCount, unsigned int *fieldCount);

void doSomething(int *op_type, int *num1, int *num2, unsigned int lineCount);
#endif /* __UTILS_H__ */
