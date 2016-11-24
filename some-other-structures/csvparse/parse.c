#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


void collect_file_info(const char* filename, size_t *lineCount, size_t *fieldCount) {
	FILE* fp = fopen(filename, "r");

	if (NULL != fp) {
		size_t len = 0;
		char *line = NULL;
		size_t fields = 1;
		ssize_t read;
		read = getline(&line, &len, fp);
		if (0 != read) {
			for (unsigned int i = 0; i < strlen(line); i++) {
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

void parse_dataset_csv(const char *filename, size_t lineCount, size_t fieldCount) {
	FILE* fp = fopen(filename, "r");
	if (NULL != fp) {
		char **cols;
		int **nums;
		cols = malloc(sizeof(char *) * fieldCount);
		nums = malloc(sizeof(int *) * fieldCount);

		size_t len = 0;
		char *line = NULL;
		ssize_t read;
		// handle the first line with Column names
		read = getline(&line, &len, fp);
		if (0 != read) {
			char *str = line;
			size_t i = 0;
			for (; i < fieldCount - 1; i++) {
				// allocating space for data in column i
				nums[i] = malloc(sizeof(int) * lineCount);
				char *first_comma = strchr(str, ',');
				int t = (int)(first_comma - str);
				cols[i] = malloc(sizeof(char) * (t + 1));
				strncpy(cols[i], str, t);
				cols[i][t] = '\0';
				str = first_comma + 1;
			}
			// allocating space for data in column i
			nums[i] = malloc(sizeof(int) * lineCount);
			char *n = strchr(str, '\n');
			int t = (int)(n - str);
			cols[i] = malloc(sizeof(char) * (t + 1));
			strncpy(cols[i], str, t);
			cols[i][t] = '\0';
			if (line) free(line);
		}

		for (size_t i = 0; i < lineCount; i++) {
			char *line = NULL;
			read = getline(&line, &len, fp);
			if (0 != read) {
				char *str = line;
				char *next = NULL;
				size_t j = 0;
				for (; j < fieldCount; j++) {
					int n = strtol(str, &next, 10);
					nums[j][i] = n;
					str = next + 1;
				}
				if (line) {
					free(line);
				}
			}
		}
		fclose(fp);
		for (size_t i = 0; i < fieldCount; i++) {
			free(nums[i]);
			free(cols[i]);
		}
		free(nums);
		free(cols);
	}
}

int main() {
	char *filename = "dataset";
	size_t lineCount;
	size_t fieldCount;
	printf("max int %d and min integer%d \n", MAX_INT, MIN_INT);
//	collect_file_info(filename, &lineCount, &fieldCount);
//	parse_dataset_csv(filename, lineCount, fieldCount);
} 
