#include "fileparser.h"
#include "db.h"
#include "table.h"
#include "column.h"
#include "uthash.h"

char *readfile(const char *filename, size_t *len) {
	FILE *fp = fopen(filename, "r"); 
 
	// Get file length
	int rc = fseek(fp, 0, SEEK_END);
	if (rc < 0) {
		log_err("fseek(END)");
		return NULL;
	}
	long l = ftell(fp);
	if (l < 0) {
		log_err("ftell()");
		return NULL;
	}
	*len = l;
 
	// Set file stream to the beginning
	rc = fseek(fp, 0, SEEK_SET);
	if (rc < 0) {
		log_err("fseek(SET)");
		return NULL;
	}
 
	/* Read the whole file
		WARNING: maybe it would be better to 
		do it block by block
	 */
	char *contents = malloc(*len);
	if (contents == NULL) {
		log_err("malloc");
		return NULL;
	}
	size_t read = 0;
	while (read < *len) {
		size_t r = fread(contents + read, 1, *len - read, fp);
		if (r == 0) {
			if (ferror(fp)) {
				log_err("error reading input\n");
				free(contents);
				fclose(fp);
				return NULL;
			} else if (feof(fp)) {
				log_err("EOF encountered after %zu bytes (expected %zu)\n",
					   read, *len);
				*len = read;
				break;
			}
		}
		read += r;
	}
	fclose(fp);
	return contents;
}

void csv_process_fields(void *s,  size_t len __attribute__((unused)), void *data)
{
	parse_csv_info *info = (parse_csv_info *)data;
	if (NULL != info) {
		if (info->error) return;
		if (1 == info->first_row) {		 // First row case
			
			// Figure out the table name
			unsigned int i = 0, flag = 0;
			char *name = (char *)s;
			while ('\0' != name[i]) {
				if ('.' == name[i]) {
					flag++;
					if (2 == flag) {		// Find the second '.'
						break;
					}
				}
				i++;
			}
			// strncpy the name, +1 for index-diff and '\0'
			char *tmp_name = malloc(sizeof(char) * (i + 1));
			strncpy(tmp_name, name, i);	 // Only copy i bytes to skip second dot
			tmp_name[i] = '\0';

			// Find the table according to the name
			Table* tmp_tbl;
			HASH_FIND_STR(database->tables, tmp_name, tmp_tbl);
			if (NULL != tmp_tbl) {
				// Update the length of table according to the file loaded
				tmp_tbl->length = info->line_count;
			}
			else{
				log_err("Invalid table name\n");
				info->error = 1;
				return;
			}

			// Find the column according to the name
			Column *tmp_col;
			HASH_FIND_STR(col_hash_list, (char *)s, tmp_col);
			if (NULL != tmp_col) {
				// TODO: better implementation of dynamic array  
				// tmp_col->data = malloc(sizeof(int) * info->line_count);
				tmp_col->data = darray_create(info->line_count);

				info->cols = realloc((info->cols), (info->cur_feild + 1) * sizeof(Column *));
				if (NULL == info->cols) {
					log_err("Mem alloc failed in loading\n");
					info->error = 1;
					return;
				}
				info->cols[info->cur_feild] = tmp_col;
			}
			else {
				log_err("Invail column name\n");
				info->error = 1;	
				return;
			}
		}
		else {							  // General case
			((info->cols[info->cur_feild])->data)->content[info->cur_row] = atoi((char *)s);
			printf("catch data %d\n", atoi((char *)s));
		}
		info->cur_feild++;
	}
}


void csv_process_row(int delim __attribute__((unused)), void *data) 
{
	parse_csv_info *info = (parse_csv_info *)data;
	if (NULL != info) {
		if (info->error) return;
		if (1 == info->first_row) {
			info->first_row = 0;
			info->field_count = info->cur_feild;
			// info->cols = malloc(sizeof(Column *) * info->field_count);
			// for (size_t i = 0; i < info->field_count; i++) {
			//	 info->cols[i] = malloc(sizeof(int) * info->line_count);
			// }
			info->cur_row = 0;	
		}
		else {
			info->cur_row++;	
		}
		info->cur_feild = 0;
	}
}

status load_data4file(const char* filename, size_t line_count) {
	size_t length = 0;
	char *contents = readfile(filename, &length);

	if (NULL == contents) {
		status ret;
		ret.code = ERROR;
		return ret;
	}

	struct csv_parser p;
	int rc = csv_init(&p, CSV_APPEND_NULL);
	if (0 != rc) {
		free(contents);
		log_err("failed to initialize CSV parser\n");
		status ret;
		ret.code = ERROR;
		return ret;
	}

	parse_csv_info parser_info;
	memset((void *)&parser_info, 0, sizeof(parse_csv_info));
	parser_info.line_count = line_count;
	parser_info.first_row = 1;
	size_t byte_processed = csv_parse(&p, (void *)contents, length, csv_process_fields, csv_process_row, &parser_info);
	rc = csv_fini(&p, csv_process_fields, csv_process_row, &parser_info);
	free(contents);

	if (0 != rc || byte_processed != length) {
		log_err("error parsing file %s\n", csv_strerror(csv_error(&p)));
		status ret;
		ret.code = ERROR;
		return ret;		
	}


	for (size_t i = 0; i < parser_info.field_count; i++) {
		for (size_t j = 0; j < parser_info.line_count; j++) {
			// printf("%d\n", parser_info.cols[i][j]);
			printf("%d ", ((parser_info.cols[i])->data)->content[j]);
		}
		printf("\n");
	}

   	status ret;
   	ret.code = CMD_DONE;
   	return ret;
}

// int main(int argc, char const *argv[])
// {
//	 char str[] = "filename";
//	 size_t t = count_file_lines(str);
//	 load_data4file(str, t - 1);

//	 // status s = load_data4file(filename, t - 1);
	
//	 // if (CMD_DONE != s.code) {
//	 //	 log_err("something wrong\n");
//	 // }
//	 return 0;
// }