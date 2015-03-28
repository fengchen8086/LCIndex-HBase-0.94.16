package org.apache.hadoop.hbase.index.regionserver;

import java.util.Comparator;

public class QuickSort {

  // threshold of using insertion sort or quick sort algorithm
  private static final int THRESHOLD = 10;

  /**
   * Sorting method which combines quick sort algorithm with insertion sort algorithm to make
   * sorting more efficient.
   * @param array the array to be sorted
   * @param comparator used to compare elements of the array
   */
  public static <T> void sort(T[] array, Comparator<? super T> comparator) {
    quickSort(array, 0, array.length - 1, comparator);
  }

  /**
   * Get the median of the low, middle and high.
   * @param array
   * @param low the lowest index of the sub-array
   * @param high the highest index of the sub-array
   * @param comparator used to compare elements of the array
   * @return T
   */
  private static <T> T median(T[] array, int low, int high, Comparator<? super T> comparator) {
    int middle = (low + high) / 2;

    if (comparator.compare(array[low], array[middle]) > 0) {
      swap(array, low, middle);
    }
    if (comparator.compare(array[low], array[high]) > 0) {
      swap(array, low, high);
    }
    if (comparator.compare(array[middle], array[high]) > 0) {
      swap(array, middle, high);
    }

    swap(array, middle, high - 1);
    return array[high - 1];
  }

  /**
   * Internal method to sort the array with quick sort algorithm and insertion sort algorithm.
   * @param array the array to be sorted
   * @param low the lowest index of the sub-array
   * @param high the highest index of the sub-array
   */
  private static <T> void quickSort(T[] array, int low, int high, Comparator<? super T> comparator) {
    if (low + THRESHOLD <= high) {
      // get the pivot
      T pivot = median(array, low, high, comparator);

      // partition
      int i = low, j = high - 1;
      for (;;) {
        while (comparator.compare(array[++i], pivot) < 0)
          ;
        while (comparator.compare(array[--j], pivot) > 0)
          ;
        if (i < j) swap(array, i, j);
        else break;
      }
      swap(array, i, high - 1);

      quickSort(array, low, i - 1, comparator);
      quickSort(array, i + 1, high, comparator);

    } else {
      // if the total number is less than THRESHOLD, use insertion sort instead
      insertionSort(array, low, high, comparator);
    }
  }

  /**
   * Swap two elements in an array.
   * @param array an array of Objects.
   * @param index1 index of the first element
   * @param index2 index of the second element
   */
  private static <T> void swap(T[] array, int index1, int index2) {
    T tmp = array[index1];
    array[index1] = array[index2];
    array[index2] = tmp;
  }

  /**
   * Sorting method with insertion sort algorithm.
   * @param arr the array to be sorted
   * @param low the lowest index of the sub-array
   * @param high the highest index of the sub-array
   */
  private static <T> void
      insertionSort(T[] arr, int low, int high, Comparator<? super T> comparator) {
    int i;
    for (int j = low+1; j <= high; j++) {
      T tmp = arr[j];
      for (i = j; i > low && comparator.compare(tmp, arr[i-1]) < 0; i--) {
        arr[i] = arr[i-1];
      }
      arr[i] = tmp;
    }
  }
}