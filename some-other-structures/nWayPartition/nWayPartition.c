#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <random>
#include <fstream>
#include <time.h>
#define ARR_LEN 200000000
#define PIVOT_NUM 32 


int A[ARR_LEN];
int pos[ARR_LEN];
int *idc;

void doPartition(int p_count, int pivots[]) {
	idc = (int *) malloc(sizeof(int) * p_count);
	for (int i = 0; i < p_count / 2 - 1; i++) 
		idc[i] = 0;
	for (int i = p_count / 2; i < p_count; i++) 
		idc[i] = ARR_LEN - 1;

	// #ifdef ONEPASS 
	// pos[0] = 0;
	// pos[ARR_LEN - 1] = ARR_LEN - 1;
	// #endif
	#ifdef PRE
	for (int i = 0; i < ARR_LEN; i++) {
		pos[i] = i;
	}
	#endif

	while (idc[p_count / 2 - 1] <= idc[p_count / 2]) {
		while (A[idc[p_count / 2 - 1]] <= pivots[p_count / 2 - 1] && idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			#ifdef ONEPASS
			// Init the position when the head pointers first hit the data
			pos[idc[p_count / 2 - 1]] = idc[p_count / 2 - 1];
			#endif
			int tmp_idc = idc[p_count / 2 - 1];
			int tmp_val = A[tmp_idc];
			int tmp_pos = pos[tmp_idc];
			for (int i = p_count / 2 - 2; i >= 0; i--) {
				if (tmp_val <= pivots[i]) {
					A[tmp_idc] = A[idc[i]];
					pos[tmp_idc] = pos[idc[i]];
					tmp_idc = idc[i];
					idc[i]++;
				}
				else {					
					break;
				}
			}
			A[tmp_idc] = tmp_val;
			pos[tmp_idc] = tmp_pos;

			// move the left head forward
			idc[p_count / 2 - 1]++;
		}
		
		while (A[idc[p_count / 2]] > pivots[p_count / 2 - 1] && idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			#ifdef ONEPASS
			// Init the position when the head pointers first hit the data
			pos[idc[p_count / 2]] = idc[p_count / 2];
			#endif
			int tmp_idc = idc[p_count / 2];
			int tmp_val = A[tmp_idc];
			int tmp_pos = pos[tmp_idc];
			for (int i = p_count / 2; i < p_count - 1; i++) {
				if (tmp_val > pivots[i]) {
					A[tmp_idc] = A[idc[i + 1]];
					pos[tmp_idc] = pos[idc[i + 1]];
					tmp_idc = idc[i + 1];
					idc[i + 1]--;
				}
				else {
					break;
				}
			}
			A[tmp_idc] = tmp_val;
			pos[tmp_idc] = tmp_pos;

			// move the right head backward
			idc[p_count / 2]--;
		}
		
		if (idc[p_count / 2 - 1] <= idc[p_count / 2]) {
			#ifdef ONEPASS
			// Init the position when the head pointers first hit the data
			pos[idc[p_count / 2 - 1]] = idc[p_count / 2 - 1];
			pos[idc[p_count / 2]] = idc[p_count / 2];
			#endif
			int tmp_l_idc = idc[p_count / 2];
			int r_val = A[tmp_l_idc];
			int r_pos = pos[tmp_l_idc];
			int tmp_r_idc = idc[p_count / 2 - 1];
			int l_val = A[tmp_r_idc];
			int l_pos = pos[tmp_r_idc];
			for (int i = p_count / 2; i < p_count - 1; i++) {
				if (l_val > pivots[i]) {
					A[tmp_l_idc] = A[idc[i + 1]];
					pos[tmp_l_idc] = pos[idc[i + 1]];
					tmp_l_idc = idc[i + 1];
					idc[i + 1]--;
				}
				else {
					break;
				}
			}
			for (int i = p_count / 2 - 2; i >= 0; i--) {
				if (r_val <= pivots[i]) {
					A[tmp_r_idc] = A[idc[i]];
					pos[tmp_r_idc] = pos[idc[i]];
					tmp_r_idc = idc[i];
					idc[i]++;
				}
				else {
					break;
				}
			}
			
			A[tmp_l_idc] = l_val;
			pos[tmp_l_idc] = l_pos;
			A[tmp_r_idc] = r_val;
			pos[tmp_r_idc] = r_pos;

			// move the head pointers
			idc[p_count / 2 - 1]++;
			idc[p_count / 2]--;
		}
	}
}


int main(int argc, char const *argv[])
{
	int pivots[PIVOT_NUM] = {
								2000,	4000,	6000,	8000,	10000,	12000,	14000,	16000,
								18000,	20000,	22000,	24000,	26000,	28000,	30000,	32000,
								34000,	36000,	37000,	38000,	40000,	42000,	44000,	46000,
								48000,	50000,	52000,	54000,	56000,	59000,	62000,	65535
							};

	
	// int pivots[PIVOT_NUM] = {
	// 							2000,	4000,	6000,	8500,	10000,	11000,	12000,	14000,
	// 							16000,	18000,	20000,	22000,	24000,	27000,	29000,	32800,
	// 							34000,	36000,	38000,	40210,	43118,	44996,	46555,	48999,
	// 							49987,	51000,	52500,	55001,	58250,	59000,	63444,	65535
	// 						};

	// srand(time(NULL));
	// initialization of data
	std::random_device generator;
    std::normal_distribution<double> distribution(32767, 5300);
	
	double sum = 0;
	// for (int j = 0; j < 100; j ++){
		for (int i = 0; i < ARR_LEN; i++) {
			double number = distribution(generator);
			A[i] = (int)(number) % 65535;
			// A[i] =  rand() % 65535 + 1;			
			// A[i] =  250;
		}
		// A[ARR_LEN - 1] = 2;
		// // show init data
		// for (int i = 0; i < ARR_LEN; i++) {
		// 	if ((i & 0x7) == 0x0) 
		// 		printf("\n");
		// 	printf("%d\t", A[i]);
		// }
		// printf("\n");
		// clock_t begin = clock();
		doPartition(PIVOT_NUM, pivots);
		// clock_t end = clock();
		// sum += (double)(end - begin) / CLOCKS_PER_SEC;
	// }

	int count = 0, i = 0;
	
	FILE *fout = fopen("size.json", "w+");
	fprintf(fout, "[%d, ", idc[0]);
	for (i = 1; i < PIVOT_NUM / 2; i++) {
		fprintf(fout, "%d, ", idc[i] - idc[i - 1]);
	}
	for (i = PIVOT_NUM / 2 + 1; i < PIVOT_NUM; i++) {
		fprintf(fout, "%d, ", idc[i] - idc[i - 1]);
	}
	fprintf(fout, "%d]\n", ARR_LEN - idc[i - 1]);


	char prefix[] = "partition";
	i = 0;  count = 0;

	char buffer[5];
	while ((count < PIVOT_NUM / 2) && (i < ARR_LEN)) {	
		sprintf(buffer, "%d", count);
		char *filename;
		filename = (char *) malloc(sizeof(char) * (strlen(prefix) + strlen(buffer) + 1));
		strncpy(filename, prefix, strlen(prefix) + 1);
		strncat(filename, buffer, strlen(buffer) + 1);

		FILE *fp = fopen(filename, "w+");
		fprintf(fp, "[");
		while (i < idc[count] - 1) {
			fprintf(fp, "%d,", A[i++]);
		}
		if (i == (idc[count] - 1)) 
			fprintf(fp, "%d]\n", A[i++]);
		else 
			fprintf(fp, "]\n");
		count++;
		fclose(fp);
		free(filename);
	}

	count = PIVOT_NUM / 2 + 1;
	while ((count < PIVOT_NUM) && (i < ARR_LEN)) {
		sprintf(buffer, "%d", count - 1);
		char *filename;
		filename = (char *) malloc(sizeof(char) * (strlen(prefix) + strlen(buffer) + 1));
		strncpy(filename, prefix, strlen(prefix) + 1);
		strncat(filename, buffer, strlen(buffer) + 1);

		FILE *fp = fopen(filename, "w+");
		fprintf(fp, "[");
		while (i < idc[count]) {
			fprintf(fp, "%d,", A[i++]);
		}
		if (i == idc[count]) 
			fprintf(fp, "%d]\n", A[i++]);
		else 
			fprintf(fp, "]\n");
		count++;
		fclose(fp);
		free(filename);

	}

	sprintf(buffer, "%d", count - 1);
	char *filename;
	filename = (char *) malloc(sizeof(char) * (strlen(prefix) + strlen(buffer) + 1));
	strncpy(filename, prefix, strlen(prefix) + 1);
	strncat(filename, buffer, strlen(buffer) + 1);

	FILE *fp = fopen(filename, "w+");
	fprintf(fp, "[");
	while  (i < ARR_LEN - 1) {
		fprintf(fp, "%d,", A[i++]);
	}
	if (i == (ARR_LEN - 1)) fprintf(fp, "%d]\n", A[i]);
	else fprintf(fp, "]\n");
	fclose(fp);
	free(filename);

	// while ((count < PIVOT_NUM / 2) && (i < ARR_LEN)) {
	// 	// fprintf(fp, "\t[");
	// 	while (i < idc[count] - 1) {
	// 		fprintf(fp, "%d,", A[i++]);
	// 	}
	// 	fprintf(fp, "%d],\n\t[", A[i++]);
	// 	count++;
	// }


	// while ((count < PIVOT_NUM) && (i < ARR_LEN)) {
	// 	// fprintf(fp, "\t[");
	// 	while (i < idc[count]) {
	// 		fprintf(fp, "%d,", A[i++]);
	// 	}
	// 	fprintf(fp, "%d],\n\t[", A[i++]);
	// 	count++;
	// }
	// // fprintf(fp, "\t[");
	// while  (i < ARR_LEN - 1) {
	// 	fprintf(fp, "%d,", A[i++]);
	// }
	// fprintf(fp, "%d]\n]\n", A[i]);
	


	// while(i < ARR_LEN && count < PIVOT_NUM) {
	// 	printf(fp, "[");
	// 	while (A[i] <= pivots[count]) {
	// 		printf(fp, "%d,", A[i]);
	// 		i++;
	// 	}
	// 	count++;
	// 	printf(fp, "],\n");
	// 	// printf("%d\n", count);
	// }
	
	

	// // show result
	// for (int i =0; i < ARR_LEN; i++) {
	// 	if ((i & 0x7) == 0x0) 
	// 		printf("\n");
	// 	printf("%d\t", pos[i]);
	// }
	// printf("\n");

	// printf(fout, "%lf\n", sum);
	return 0;
}
