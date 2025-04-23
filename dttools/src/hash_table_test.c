/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "hash_table_test.h"
#include "hash_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#define DEFAULT_LOAD 0.75

#define DEFAULT_SIZE 127
#define TEST_SIZE 1000000
#define RANDOM_SEED 12345

static int test_failed = 0;

static void test_assert(int condition, const char *file, int line, const char *message)
{
    if (!condition) {
        fprintf(stderr, "TEST FAILED: %s at %s:%d\n", message, file, line);
        test_failed = 1;
    }
}

#define ASSERT(condition, message) test_assert(condition, __FILE__, __LINE__, message)

static char *generate_key(int i) {
    char *key = malloc(24);
    if (!key) return NULL;
    sprintf(key, "key_%d", i);
    return key;
}

static void print_hash_table_stats(struct hash_table *h, const char *stage, int items_processed)
{
    printf("  [%s - %d items] Size: %d, Buckets: %d, Load factor: %.3f\n", 
           stage, items_processed, hash_table_size(h), 
           hash_table_bucket_count(h), hash_table_load_factor(h));
}

int test_hash_table_basic()
{
    printf("Testing hash_table basic operations with %d elements...\n", TEST_SIZE);
    test_failed = 0;
    
    struct hash_table *h = hash_table_create(0, NULL);
    ASSERT(h != NULL, "hash_table_create should return a valid pointer");
    
    ASSERT(hash_table_size(h) == 0, "New hash table should have size 0");
    print_hash_table_stats(h, "Initial", 0);
    
    char **keys = malloc(TEST_SIZE * sizeof(char*));
    char **values = malloc(TEST_SIZE * sizeof(char*));
    ASSERT(keys != NULL && values != NULL, "Memory allocation should succeed");
    
    printf("  Inserting %d items...\n", TEST_SIZE);
    
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    
    for (int i = 0; i < TEST_SIZE; i++) {
        keys[i] = generate_key(i);
        values[i] = strdup(keys[i]);
        ASSERT(hash_table_insert(h, keys[i], values[i]), "Insertion should succeed");
        
        if (i == 100 || i == 1000 || i == 10000 || i == 100000 || 
            i == 250000 || i == 500000 || i == 750000 || 
            i == TEST_SIZE-1) {
            print_hash_table_stats(h, "After insertion", i+1);
        }
    }
    
    gettimeofday(&end_time, NULL);
    double insert_time = (end_time.tv_sec - start_time.tv_sec) + 
                      (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    ASSERT(hash_table_size(h) == TEST_SIZE, "Size should match number of insertions");
    printf("  Successfully inserted all %d items in %.2f seconds (%.0f items/sec)\n", 
           TEST_SIZE, insert_time, TEST_SIZE / insert_time);
    
    printf("  Verifying all items...\n");
    gettimeofday(&start_time, NULL);
    
    int verified = 0;
    for (int i = 0; i < TEST_SIZE; i++) {
        void *value = hash_table_lookup(h, keys[i]);
        if (value != NULL && strcmp(keys[i], (char*)value) == 0) {
            verified++;
        }
        
        if (i == 100 || i == 1000 || i == 10000 || i == 100000 || 
            i == 250000 || i == 500000 || i == 750000 || 
            i == TEST_SIZE-1) {
            printf("  Verified %d/%d items\n", i+1, TEST_SIZE);
        }
    }
    
    gettimeofday(&end_time, NULL);
    double lookup_time = (end_time.tv_sec - start_time.tv_sec) + 
                       (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    ASSERT(verified == TEST_SIZE, "All items should be found and have correct values");
    printf("  Successfully verified %d items in %.2f seconds (%.0f lookups/sec)\n", 
           verified, lookup_time, TEST_SIZE / lookup_time);
    
    printf("  Testing iteration over all elements...\n");
    gettimeofday(&start_time, NULL);
    
    int count = 0;
    char *key;
    void *value;
    
    hash_table_firstkey(h);
    while (hash_table_nextkey(h, &key, &value)) {
        count++;
        if (count == 100 || count == 1000 || count == 10000 || count == 100000 || 
            count == 250000 || count == 500000 || count == 750000 || 
            count == TEST_SIZE) {
            printf("  Iterated through %d/%d items\n", count, TEST_SIZE);
        }
    }
    
    gettimeofday(&end_time, NULL);
    double iterate_time = (end_time.tv_sec - start_time.tv_sec) + 
                        (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    ASSERT(count == TEST_SIZE, "Iteration should visit all items");
    printf("  Successfully iterated through all %d items in %.2f seconds (%.0f items/sec)\n", 
           count, iterate_time, TEST_SIZE / iterate_time);
    
    printf("  Cleaning up...\n");
    gettimeofday(&start_time, NULL);
    
    for (int i = 0; i < TEST_SIZE; i++) {
        hash_table_remove(h, keys[i]);
        free(keys[i]);
        free(values[i]);
        
        if (i == 100 || i == 1000 || i == 10000 || i == 100000 || 
            i == 250000 || i == 500000 || i == 750000 || 
            i == TEST_SIZE-1) {
            print_hash_table_stats(h, "After removal", TEST_SIZE - (i+1));
        }
    }
    
    gettimeofday(&end_time, NULL);
    double cleanup_time = (end_time.tv_sec - start_time.tv_sec) + 
                        (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    printf("  Cleanup completed in %.2f seconds\n", cleanup_time);
    
    free(keys);
    free(values);
    
    hash_table_delete(h);
    
    printf("Basic operations test %s.\n", test_failed ? "FAILED" : "PASSED");
    return !test_failed;
}

int test_hash_table_dynamic_resize()
{
    printf("Testing hash_table dynamic resizing behavior...\n");
    test_failed = 0;
    
    int initial_size = 10;
    struct hash_table *h = hash_table_create(initial_size, NULL);
    ASSERT(h != NULL, "hash_table_create should return a valid pointer");
    
    print_hash_table_stats(h, "Initial", 0);
    ASSERT(hash_table_bucket_count(h) == initial_size, "Initial bucket count should match specified size");
    
    int test_items = 1000;
    char **keys = malloc(test_items * sizeof(char*));
    char **values = malloc(test_items * sizeof(char*));
    ASSERT(keys != NULL && values != NULL, "Memory allocation should succeed");
    
    printf("  Inserting items to trigger expansion...\n");
    int expansion_count = 0;
    int prev_bucket_count = hash_table_bucket_count(h);
    
    for (int i = 0; i < test_items; i++) {
        keys[i] = generate_key(i);
        values[i] = strdup(keys[i]);
        ASSERT(hash_table_insert(h, keys[i], values[i]), "Insertion should succeed");
        
        int current_bucket_count = hash_table_bucket_count(h);
        if (current_bucket_count > prev_bucket_count) {
            expansion_count++;
            printf("  Expansion #%d detected: %d -> %d buckets (after %d insertions, load factor: %.3f)\n", 
                   expansion_count, prev_bucket_count, current_bucket_count, 
                   i+1, hash_table_load_factor(h));
            prev_bucket_count = current_bucket_count;
        }
        
        if (i == 10 || i == 50 || i == 100 || i == 200 || i == 500 || i == test_items-1) {
            print_hash_table_stats(h, "During insertion", i+1);
        }
    }
    
    printf("  Final state: %d items, %d buckets, load factor: %.3f\n",
           hash_table_size(h), hash_table_bucket_count(h), hash_table_load_factor(h));
    
    ASSERT(expansion_count > 0, "Hash table should have expanded at least once");
    printf("  Hash table expanded %d times during %d insertions\n", expansion_count, test_items);
    
    printf("  Removing items to trigger shrinking...\n");
    int shrink_count = 0;
    prev_bucket_count = hash_table_bucket_count(h);
    
    for (int i = 0; i < test_items; i++) {
        void *value = hash_table_remove(h, keys[i]);
        ASSERT(value == values[i], "Removed value should match original");
        
        int current_bucket_count = hash_table_bucket_count(h);
        if (current_bucket_count < prev_bucket_count) {
            shrink_count++;
            printf("  Shrinking #%d detected: %d -> %d buckets (after %d removals, load factor: %.3f)\n", 
                   shrink_count, prev_bucket_count, current_bucket_count, 
                   i+1, hash_table_load_factor(h));
            prev_bucket_count = current_bucket_count;
        }
        
        if (i == 10 || i == 50 || i == 100 || i == 200 || i == 500 || i == test_items-1) {
            print_hash_table_stats(h, "During removal", test_items - (i+1));
        }
        
        free(keys[i]);
        free(values[i]);
    }
    
    ASSERT(hash_table_size(h) == 0, "Hash table should be empty after all removals");
    printf("  Hash table shrunk %d times during %d removals\n", shrink_count, test_items);
    
    free(keys);
    free(values);
    hash_table_delete(h);
    
    printf("Dynamic resizing test %s.\n", test_failed ? "FAILED" : "PASSED");
    return !test_failed;
}

int test_hash_table_shrinking()
{
    printf("Testing hash_table dynamic shrinking specifically...\n");
    test_failed = 0;
    
    int initial_size = 2000;
    struct hash_table *h = hash_table_create(initial_size, NULL);
    ASSERT(h != NULL, "hash_table_create should return a valid pointer");
    
    hash_table_set_shrink_threshold(h, 0.2);
    
    print_hash_table_stats(h, "Initial", 0);
    ASSERT(hash_table_bucket_count(h) >= initial_size, "Initial bucket count should be at least the specified size");
    
    int test_items = 1000;
    char **keys = malloc(test_items * sizeof(char*));
    char **values = malloc(test_items * sizeof(char*));
    ASSERT(keys != NULL && values != NULL, "Memory allocation should succeed");
    
    printf("  Inserting %d items to fill the table...\n", test_items);
    for (int i = 0; i < test_items; i++) {
        keys[i] = generate_key(i);
        values[i] = strdup(keys[i]);
        ASSERT(hash_table_insert(h, keys[i], values[i]), "Insertion should succeed");
    }
    
    print_hash_table_stats(h, "After insertion", test_items);
    float initial_load = hash_table_load_factor(h);
    printf("  Initial load factor: %.3f\n", initial_load);
    
    printf("  Testing batch removal behavior...\n");
    
    const int batch_sizes[] = {100, 200, 300};
    const int batch_count = sizeof(batch_sizes) / sizeof(batch_sizes[0]);
    
    for (int b = 0; b < batch_count; b++) {
        int batch_size = batch_sizes[b];
        int start_idx = b * batch_size;
        printf("  Removing batch #%d: %d items (indices %d-%d)...\n", 
               b+1, batch_size, start_idx, start_idx + batch_size - 1);
        
        const char **batch_keys = malloc(batch_size * sizeof(char*));
        ASSERT(batch_keys != NULL, "Memory allocation for batch keys should succeed");
        
        for (int i = 0; i < batch_size; i++) {
            if (start_idx + i < test_items) {
                batch_keys[i] = keys[start_idx + i];
            }
        }
        
        int before_buckets = hash_table_bucket_count(h);
        float before_load = hash_table_load_factor(h);
        
        int removed = hash_table_remove_batch(h, batch_keys, batch_size);
        
        int after_buckets = hash_table_bucket_count(h);
        float after_load = hash_table_load_factor(h);
        
        printf("  Batch #%d results: removed %d/%d items\n", b+1, removed, batch_size);
        printf("  Before: %d buckets, load factor %.3f\n", before_buckets, before_load);
        printf("  After:  %d buckets, load factor %.3f\n", after_buckets, after_load);
        
        if (after_buckets < before_buckets) {
            printf("  Shrinking detected: %d -> %d buckets (%.1f%% reduction)\n", 
                   before_buckets, after_buckets, 
                   100.0 * (before_buckets - after_buckets) / before_buckets);
            
            ASSERT(after_load > 0.1 && after_load < 0.4, 
                   "Load factor after shrinking should be in reasonable range");
        }
        
        free(batch_keys);
        print_hash_table_stats(h, "After batch removal", hash_table_size(h));
    }
    
    printf("  Testing incremental removal behavior...\n");
    
    int remaining_count = hash_table_size(h);
    int start_idx = batch_count * batch_sizes[0];
    int shrink_events = 0;
    int prev_bucket_count = hash_table_bucket_count(h);
    
    for (int i = start_idx; i < test_items && remaining_count > 0; i++) {
        if (keys[i] == NULL) continue;
        
        void *value = hash_table_remove(h, keys[i]);
        if (value != NULL) {
            free(value);
            remaining_count--;
        }
        
        int current_bucket_count = hash_table_bucket_count(h);
        if (current_bucket_count < prev_bucket_count) {
            shrink_events++;
            float load_factor = hash_table_load_factor(h);
            printf("  Shrink event #%d: %d -> %d buckets (remaining items: %d, load factor: %.3f)\n", 
                   shrink_events, prev_bucket_count, current_bucket_count, 
                   remaining_count, load_factor);
            prev_bucket_count = current_bucket_count;
            
            print_hash_table_stats(h, "After shrink", remaining_count);
        }
        
        if ((i - start_idx) % 50 == 0 || remaining_count == 0) {
            printf("  Removed %d individual items, %d items remaining\n", 
                   i - start_idx + 1, remaining_count);
            float load = hash_table_load_factor(h);
            if (load < 0.3) {
                printf("  Current load factor: %.3f (below shrink threshold 0.2)\n", load);
            }
        }
    }
    
    printf("  Hash table had %d shrink events during incremental removal\n", shrink_events);
    printf("  Final bucket count: %d (vs initial %d)\n", 
           hash_table_bucket_count(h), initial_size);
    
    for (int i = 0; i < test_items; i++) {
        if (keys[i]) free(keys[i]);
    }
    free(keys);
    free(values);
    
    hash_table_delete(h);
    
    printf("Dynamic shrinking test %s.\n", test_failed ? "FAILED" : "PASSED");
    return !test_failed;
}

int test_hash_table_edge_cases()
{
    printf("Testing hash_table edge cases...\n");
    test_failed = 0;
    
    printf("  Test 1: Creating hash table with size 0...\n");
    struct hash_table *h1 = hash_table_create(0, NULL);
    ASSERT(h1 != NULL, "hash_table_create with size 0 should return a valid pointer");
    print_hash_table_stats(h1, "Size 0 creation", 0);
    ASSERT(hash_table_bucket_count(h1) == DEFAULT_SIZE, 
           "Hash table with initial size 0 should have default size");
    
    void *lookup = hash_table_lookup(h1, "nonexistent");
    ASSERT(lookup == NULL, "Lookup in empty table should return NULL");
    
    void *removed = hash_table_remove(h1, "nonexistent");
    ASSERT(removed == NULL, "Remove from empty table should return NULL");
    
    char *key;
    void *value;
    int count = 0;
    hash_table_firstkey(h1);
    while (hash_table_nextkey(h1, &key, &value)) {
        count++;
    }
    ASSERT(count == 0, "Empty hash table should have 0 items to iterate");
    
    hash_table_delete(h1);
    printf("  Test 1 passed: Empty hash table behaves correctly\n");
    
    printf("  Test 2: Creating hash table with size 1...\n");
    struct hash_table *h2 = hash_table_create(1, NULL);
    ASSERT(h2 != NULL, "hash_table_create with size 1 should return a valid pointer");
    print_hash_table_stats(h2, "Size 1 creation", 0);
    
    printf("  Inserting items into size-1 hash table to force expansion...\n");
    int item_count = 20;
    char **keys = malloc(item_count * sizeof(char*));
    
    for (int i = 0; i < item_count; i++) {
        keys[i] = generate_key(i);
        ASSERT(hash_table_insert(h2, keys[i], (void*)(long)i), 
               "Insertion into size-1 hash table should succeed");
        
        if (i == 0 || i == 1 || i == 2 || i == 5 || i == 10 || i == 19) {
            print_hash_table_stats(h2, "During insertion", i+1);
        }
    }
    
    for (int i = 0; i < item_count; i++) {
        void *found = hash_table_lookup(h2, keys[i]);
        ASSERT(found == (void*)(long)i, "Items in expanded table should be retrievable");
    }
    
    for (int i = 0; i < item_count; i++) {
        free(keys[i]);
    }
    free(keys);
    hash_table_delete(h2);
    printf("  Test 2 passed: Size-1 hash table expands correctly\n");
    
    printf("  Test 3: Testing hash collisions handling...\n");
    
    unsigned constant_hash(const char *key) {
        return 42;
    }
    
    struct hash_table *h3 = hash_table_create(10, constant_hash);
    ASSERT(h3 != NULL, "hash_table_create with custom hash function should return a valid pointer");
    
    int collision_count = 100;
    char **c_keys = malloc(collision_count * sizeof(char*));
    
    printf("  Inserting %d items with identical hash values...\n", collision_count);
    for (int i = 0; i < collision_count; i++) {
        c_keys[i] = generate_key(i);
        ASSERT(hash_table_insert(h3, c_keys[i], (void*)(long)i), 
               "Insertion with collision should succeed");
    }
    
    print_hash_table_stats(h3, "After collision insertions", collision_count);
    
    printf("  Verifying retrievability with collisions...\n");
    int found_count = 0;
    for (int i = 0; i < collision_count; i++) {
        void *found = hash_table_lookup(h3, c_keys[i]);
        if (found == (void*)(long)i) {
            found_count++;
        }
    }
    
    ASSERT(found_count == collision_count, 
           "All items should be retrievable despite hash collisions");
    printf("  Successfully found all %d items with identical hash values\n", found_count);
    
    printf("  Testing removal with collisions...\n");
    for (int i = 0; i < collision_count; i += 2) {
        void *removed = hash_table_remove(h3, c_keys[i]);
        ASSERT(removed == (void*)(long)i, "Removal with collision should return correct value");
    }
    
    print_hash_table_stats(h3, "After collision removals", hash_table_size(h3));
    
    found_count = 0;
    for (int i = 1; i < collision_count; i += 2) {
        void *found = hash_table_lookup(h3, c_keys[i]);
        if (found == (void*)(long)i) {
            found_count++;
        }
    }
    
    int expected_remaining = collision_count / 2 + (collision_count % 2);
    ASSERT(found_count == expected_remaining, 
           "Remaining items should be retrievable after collision removals");
    
    for (int i = 0; i < collision_count; i++) {
        free(c_keys[i]);
    }
    free(c_keys);
    hash_table_delete(h3);
    printf("  Test 3 passed: Hash table handles collisions correctly\n");
    
    printf("Edge case tests %s.\n", test_failed ? "FAILED" : "PASSED");
    return !test_failed;
}

int test_hash_table_high_load()
{
    printf("Testing hash_table under high load conditions...\n");
    test_failed = 0;
    
    int initial_size = 10000;
    struct hash_table *h = hash_table_create(initial_size, NULL);
    ASSERT(h != NULL, "hash_table_create should return a valid pointer");
    
    int items_to_insert = (int)(initial_size * DEFAULT_LOAD * 0.95);
    printf("  Inserting %d items to achieve high load factor (~0.7)...\n", items_to_insert);
    
    char **keys = malloc(items_to_insert * sizeof(char*));
    ASSERT(keys != NULL, "Memory allocation should succeed");
    
    struct timeval start_time, end_time;
    double duration;
    
    int benchmark_size = items_to_insert / 10;
    printf("  Benchmark: Inserting first %d items (low load)...\n", benchmark_size);
    
    gettimeofday(&start_time, NULL);
    for (int i = 0; i < benchmark_size; i++) {
        keys[i] = generate_key(i);
        ASSERT(hash_table_insert(h, keys[i], (void*)(long)i), "Insertion should succeed");
    }
    gettimeofday(&end_time, NULL);
    
    duration = (end_time.tv_sec - start_time.tv_sec) + 
              (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    double low_load_rate = benchmark_size / duration;
    printf("  Low load insertion rate: %.0f items/second\n", low_load_rate);
    print_hash_table_stats(h, "After low-load insertion", benchmark_size);
    
    printf("  Continuing insertion to high load...\n");
    
    gettimeofday(&start_time, NULL);
    for (int i = benchmark_size; i < items_to_insert; i++) {
        keys[i] = generate_key(i);
        ASSERT(hash_table_insert(h, keys[i], (void*)(long)i), "Insertion should succeed");
        
        if (i == items_to_insert/4 || i == items_to_insert/2 || i == (3*items_to_insert)/4) {
            print_hash_table_stats(h, "During high-load insertion", i+1);
        }
    }
    gettimeofday(&end_time, NULL);
    
    duration = (end_time.tv_sec - start_time.tv_sec) + 
              (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    double high_load_rate = (items_to_insert - benchmark_size) / duration;
    printf("  High load insertion rate: %.0f items/second\n", high_load_rate);
    
    float performance_ratio = low_load_rate / high_load_rate;
    printf("  Performance comparison: low load is %.2f times faster than high load\n", 
           performance_ratio);
    
    print_hash_table_stats(h, "Final high-load state", items_to_insert);
    
    printf("  Testing lookup performance under high load...\n");
    
    int lookup_count = 10000;
    gettimeofday(&start_time, NULL);
    
    int found = 0;
    for (int i = 0; i < lookup_count; i++) {
        int idx = rand() % items_to_insert;
        void *value = hash_table_lookup(h, keys[idx]);
        if (value == (void*)(long)idx) {
            found++;
        }
    }
    
    gettimeofday(&end_time, NULL);
    duration = (end_time.tv_sec - start_time.tv_sec) + 
              (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    ASSERT(found == lookup_count, "All lookups should succeed under high load");
    printf("  High load lookup rate: %.0f lookups/second\n", lookup_count / duration);
    
    printf("  Testing iteration performance under high load...\n");
    
    gettimeofday(&start_time, NULL);
    
    int count = 0;
    char *key;
    void *value;
    
    hash_table_firstkey(h);
    while (hash_table_nextkey(h, &key, &value)) {
        count++;
    }
    
    gettimeofday(&end_time, NULL);
    duration = (end_time.tv_sec - start_time.tv_sec) + 
              (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    
    ASSERT(count == items_to_insert, "Iteration should visit all items under high load");
    printf("  High load iteration rate: %.0f items/second\n", count / duration);
    
    for (int i = 0; i < items_to_insert; i++) {
        free(keys[i]);
    }
    free(keys);
    hash_table_delete(h);
    
    printf("High load tests %s.\n", test_failed ? "FAILED" : "PASSED");
    return !test_failed;
}

int run_hash_table_tests()
{
    int success = 1;
    
    srand(RANDOM_SEED);
    printf("\nRUNNING HASH TABLE TESTS\n");
    printf("========================\n\n");
    
    success &= test_hash_table_edge_cases();
    success &= test_hash_table_high_load();
    success &= test_hash_table_dynamic_resize();
    success &= test_hash_table_shrinking();
    success &= test_hash_table_basic();
    
    printf("\nSummary: hash_table tests %s.\n\n", success ? "PASSED" : "FAILED");
    
    return success;
}

int main(int argc, char **argv)
{
    return run_hash_table_tests() ? 0 : 1;
}
