#include "k_ary_tree.h"
#include "treesearch.h"
#include <math.h>
#include <stdio.h>

uint logBaseK(int K, int x) {
	uint res = 0;
	while (0 < x) {
		res++;
		x = x / K - 1;
	}
	// uint res = floor(log(x) / log(K));
	return res;
}

BTree *bTreeCreate(int *arr, uint len) {
	uint height = logBaseK(FANOUT_K, len - 1) - 1;
	BTree *root = malloc(sizeof(BTree));
	root->height = height;
	uint *level = malloc(sizeof(uint) * (height + 2));
	uint l = (len - 1) / FANOUT_K;
	uint space = 0;
	// printf("height here %d  %d\n", height,  l);
	level[height + 1] = 0;
	while (0 != height) {
		space += l;
		level[height] = l;
		l = (l - 1) / FANOUT_K;
		height--;
	}
	level[height] = 0;

	// printf("space %d\n", space);
	int *nodes = malloc(sizeof(int) * space);
	
	int power = 0;
	height = root->height;
	uint beg = space - 1;
	while(0 != height) {
		power++;
		beg -= level[height];
		uint step = (uint) pow(FANOUT_K, power);
		for (uint i = 1; i <= level[height]; i++) {
			nodes[beg + i] = arr[step * i - 1];
		}
		height--;
		// nodes[beg + i] = arr[len - 1];
	}
	uint i = 1;
	for (; i <= root->height; i++) {
		level[i] += level[i - 1];
	}
	level[i] = len - 1;
	root->levelNodeCount = level;
	root->internalNode = nodes;
	root->leaves = arr;
	root->arr_len = len;
	return root;
}

uint bt_sequential_search_node(BTree *root, int key, uint lo, uint hi, uint height) {
	uint idc;
	uint offset = 0;
	uint base = 0;
	d("search height %u with lo %u and hi %u\n", height, lo, hi);

	// go down through the tree
	while(height <= root->height) {
		// offset = (height == root->height)? 0: root->levelNodeCount[height];
		// base = root->levelNodeCount[height - 1];
		for (idc = lo; idc <= hi; idc++) {
			if (key < root->internalNode[idc]) {
				// lo = (idc - base) * FANOUT_K + offset;
				// hi = lo + FANOUT_K - 1;
				break;
			}
			else if (key == root->internalNode[idc]) {
				idc = idc - root->levelNodeCount[height - 1] + 1;
				for (uint i = height; i <= root->height; i++) {
					idc *= FANOUT_K;
				}
				return idc - 1;
			}
		}
		offset = (height == root->height)? 0: root->levelNodeCount[height];
		base = root->levelNodeCount[height - 1];

		if (idc != (hi + 1)) {
			lo = (idc - base) * FANOUT_K + offset;
		 	hi = lo + FANOUT_K - 1;
		}
		else {
			lo = (idc - base - 1) * FANOUT_K + offset;
			hi = root->levelNodeCount[height + 1];
		}
		height++;
	}

	// search the leaves
	for (idc = lo; idc <= hi; idc++) {
		if (key <= root->leaves[idc]) {
			return idc;
		}
	}
	return idc - 1;
}

uint bt_binary_search_node(BTree *root, int key, uint lo, uint hi, uint height){
	uint mid, old_hi = hi;
	int *array_to_search = NULL;
	if (height <= root->height) {
		array_to_search = root->internalNode;
	}
	else {
		// at leaves
		array_to_search = root->leaves;
		while (lo < hi) {
			mid = (lo + hi) / 2;
			if (key > array_to_search[mid]) {
				lo = mid + 1;
			}
			else if (key < array_to_search[mid]){
				hi = mid;
			}
			else {
				return mid;
			}
		}
		return lo;
	}

	// printf("height %u before binary search %u and %u\n", height, lo, hi);
	while (lo < hi) {
		mid = (lo + hi) / 2;
		if (key > array_to_search[mid]) {
			lo = mid + 1;
		}
		else if (key < array_to_search[mid]){
			hi = mid;
		}
		else {
			mid = mid - root->levelNodeCount[height - 1] + 1;
			for (uint i = height; i <= root->height; i++) {
				mid *= FANOUT_K;
			}
			return mid - 1;
		}
	}

	// printf("after a binary search lo %u and hi %u\n", lo, hi);
	uint offset = (height == root->height)? 0: root->levelNodeCount[height];
	uint base = root->levelNodeCount[height - 1];
	
	if (hi != old_hi || key <= array_to_search[hi]) {
		height++;
		// printf("go search internal node\n");
		return bt_binary_search_node(root, key, (lo - base) * FANOUT_K + offset, (lo - base) * FANOUT_K + offset + FANOUT_K - 1, height);
	}
	else {
		// search the last node
		uint end;
		if (height == root->height) {
			// we are at the last level of index
			end = root->arr_len - 1;
		}
		else {
			end = root->levelNodeCount[height + 1] - root->levelNodeCount[height];
		}
		height++;
		// printf("go search last node with end %d\n", end);
		return bt_binary_search_node(root, key, (lo - base) * FANOUT_K + offset, end, height);
	}
}
