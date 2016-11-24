#include <stdlib.h>
#include "treesearch.h"
#define FANOUT_K 16

typedef	struct _btree
{
	uint height;
	// prefix sum of the number of nodes in the upper k levels
	uint *levelNodeCount;
	int *internalNode;
	int *leaves;
	uint arr_len;
} BTree;


uint logBaseK(int K, int x);
BTree *bTreeCreate(int *arr, uint len);
uint bt_binary_search_node(BTree *root, int key, uint lo, uint hi, uint height);
uint bt_sequential_search_node(BTree *root, int key, uint lo, uint hi, uint height);

inline uint bt_node_search(BTree *root, int key) {
	#if FANOUT_K >= 85
	return bt_binary_search_node(root, key, 0, root->levelNodeCount[1] - 1, 1);
	#else
	return bt_sequential_search_node(root, key, 0, root->levelNodeCount[1] - 1,  1);
	#endif
}
