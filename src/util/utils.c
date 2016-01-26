#define _GNU_SOURCE
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


void collect_file_info(const char* filename, size_t *lineCount, size_t *fieldCount) {
    FILE* fp = fopen(filename, "r");

    if (NULL != fp) {
        size_t len = 0;
        char *line = NULL;
        size_t fields = 1;
        ssize_t read;
        read = getline(&line, &len, fp);
        if (0 != read )  {
            for (uint i = 0; i < strlen(line); i++) {
                fields += (line[i] == ',');
            }
            if (line) free(line);    
        }
        
        size_t lines = 0;
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
