/* binary search is based on the fact the pivots array is sorted
 * Kenneth's algorithm initially does not provide a sorted array of pivots
 */
unsigned int binary_search_pivots(int *pivots, unsigned int len, int key){
        unsigned int beg = 0, end = len - 1, mid;
        while (beg < end) {
                mid = (beg + end) / 2;
                if (key > pivots[mid]) {
                        beg = mid + 1;
                }
                else if (key < pivots[mid]){
                        end = mid;
                }
                else {
                        return mid;
                }
        }
        return beg;
}

