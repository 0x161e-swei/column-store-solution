#ifndef __FILEPARSER_H
#define __FILEPARSER_H
#include <string.h>
#include "cs165_api.h"

status parse_field(const char *fields, uint lineCount, uint fieldCount, Column **cols);
status parse_dataset_csv(const char *filename);

#endif /* __FILEPARSER_H */