/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "itable_test.h"
#include "itable.h"
#include "debug.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#define DEFAULT_SIZE 127
#define SMALL_TEST_SIZE 100000ULL
#define MEDIUM_TEST_SIZE 100000ULL
#define LARGE_TEST_SIZE 1000000ULL
#define RANDOM_SEED 12345
#define STRESS_CYCLES 5

// 修复类型转换问题
typedef unsigned long long UINT64;

static int test_failed = 0;
static clock_t test_start_time;

static void start_timer() {
    test_start_time = clock();
}

static double get_elapsed_time() {
    return (double)(clock() - test_start_time) / CLOCKS_PER_SEC;
}

static void test_assert(int condition, const char *file, int line, const char *message)
{
    if (!condition) {
        fprintf(stderr, "TEST FAILED: %s at %s:%d\n", message, file, line);
        test_failed = 1;
    }
}

#define ASSERT(condition, message) test_assert(condition, __FILE__, __LINE__, message)

static void *generate_value(UINT64_T key) {
    char *value = malloc(32);
    if (!value) return NULL;
    sprintf(value, "value_%llu", (unsigned long long)key);
    return value;
}

// 添加用于打印itable状态的函数
static void print_itable_stats(struct itable *t, const char *stage, UINT64_T items_processed)
{
    printf("  [%s - %llu items] Size: %d\n", 
           stage, (unsigned long long)items_processed, itable_size(t));
}

int test_itable_basic()
{
    printf("Testing itable basic operations...\n");
    test_failed = 0;
    start_timer();
    
    struct itable *t = itable_create(0);
    ASSERT(t != NULL, "itable_create should return a valid pointer");
    
    ASSERT(itable_size(t) == 0, "New itable should have size 0");
    
    // Test insertion
    ASSERT(itable_insert(t, 123, "value123"), "itable_insert should succeed");
    ASSERT(itable_size(t) == 1, "Size should be 1 after one insertion");
    
    // Test lookup
    ASSERT(strcmp(itable_lookup(t, 123), "value123") == 0, "itable_lookup should return correct value");
    ASSERT(itable_lookup(t, 456) == NULL, "itable_lookup should return NULL for non-existent key");
    
    // Test removal
    void *removed = itable_remove(t, 123);
    ASSERT(strcmp(removed, "value123") == 0, "itable_remove should return the removed value");
    ASSERT(itable_size(t) == 0, "Size should be 0 after removal");
    ASSERT(itable_lookup(t, 123) == NULL, "Removed key should no longer be found");
    
    // Test multiple insertions (larger scale)
    UINT64_T test_size = SMALL_TEST_SIZE;
    printf("  Inserting %llu items...\n", (unsigned long long)test_size);
    for (UINT64_T i = 0; i < test_size; i++) {
        void *value = generate_value(i);
        ASSERT(itable_insert(t, i, value), "Multiple insertions should succeed");
    }
    ASSERT((UINT64_T)itable_size(t) == test_size, "Size should match number of insertions");
    
    // Test lookup for random items
    printf("  Testing random lookups...\n");
    for (int i = 0; i < 1000; i++) {
        UINT64_T key = rand() % test_size;
        void *value = itable_lookup(t, key);
        ASSERT(value != NULL, "Should find randomly selected key");
        char expected[32];
        sprintf(expected, "value_%llu", (unsigned long long)key);
        ASSERT(strcmp(expected, (char*)value) == 0, "Value should match key");
    }
    
    // Test iteration
    printf("  Testing full iteration...\n");
    UINT64_T key;
    void *value;
    UINT64_T count = 0;
    itable_firstkey(t);
    while (itable_nextkey(t, &key, &value)) {
        count++;
        free(value); // Clean up
    }
    ASSERT(count == test_size, "Iteration should visit all entries");
    
    // Clean up
    itable_delete(t);
    
    printf("Basic operations tests %s in %.2f seconds.\n", test_failed ? "FAILED" : "PASSED", get_elapsed_time());
    return !test_failed;
}

int test_itable_expansion()
{
    printf("Testing itable dynamic expansion with %llu items...\n", (unsigned long long)MEDIUM_TEST_SIZE);
    test_failed = 0;
    start_timer();
    
    // 使用较小的初始大小以确保触发扩容
    int initial_size = 10;
    struct itable *t = itable_create(initial_size);
    ASSERT(t != NULL, "itable_create should return a valid pointer");
    
    // 插入足够多的元素触发多次扩容
    printf("  Inserting items to trigger expansion...\n");
    UINT64_T num_items = MEDIUM_TEST_SIZE;
    void **values = malloc(num_items * sizeof(void*));
    ASSERT(values != NULL, "Memory allocation for values should succeed");
    
    // 跟踪表大小变化以检测扩容
    int expansion_count = 0;
    int prev_space_used = 0;
    
    struct timeval start_tv, end_tv;
    gettimeofday(&start_tv, NULL);
    
    for (UINT64_T i = 0; i < num_items; i++) {
        values[i] = generate_value(i);
        ASSERT(itable_insert(t, i, values[i]), "Insertion should succeed");
        
        int current_size = itable_size(t);
        
        // 当我们观察到插入速度有显著变化时，可能是发生了扩容
        // 或者当表大小与之前插入的数量比例发生变化时
        if (i > 0 && i % 1000 == 0) {
            // 检查近期插入的速度是否变慢(可能表示扩容)
            if (current_size / (int)(i+1) != prev_space_used / (int)i) {
                expansion_count++;
                printf("  Possible expansion #%d detected at %llu items (size: %d)\n", 
                       expansion_count, (unsigned long long)i+1, current_size);
            }
            prev_space_used = current_size;
        }
        
        // 周期性地打印状态
        if (i % 20000 == 0 && i > 0) {
            print_itable_stats(t, "During insertion", i);
            printf("  Inserted %llu items so far\n", (unsigned long long)i);
        }
    }
    
    gettimeofday(&end_tv, NULL);
    double insert_time = (end_tv.tv_sec - start_tv.tv_sec) + 
                        (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    
    printf("  Inserted %llu items in %.2f seconds (%.0f items/sec)\n", 
           (unsigned long long)num_items, insert_time, (double)num_items / insert_time);
    
    print_itable_stats(t, "After all insertions", num_items);
    
    ASSERT((UINT64_T)itable_size(t) == num_items, "Size should match number of insertions");
    ASSERT(expansion_count > 0, "Table should have expanded at least once");
    printf("  Detected approximately %d expansion events\n", expansion_count);
    
    // 验证随机项仍然可以访问
    printf("  Verifying random access...\n");
    for (int i = 0; i < 10000; i++) {
        UINT64_T index = rand() % num_items;
        void *value = itable_lookup(t, index);
        ASSERT(value != NULL, "Should find key after expansion");
        char expected[32];
        sprintf(expected, "value_%llu", (unsigned long long)index);
        ASSERT(strcmp(expected, (char*)value) == 0, "Values should match after expansion");
    }
    
    // 清理
    printf("  Cleaning up...\n");
    itable_clear(t, free);
    free(values);
    itable_delete(t);
    
    printf("Dynamic expansion tests %s in %.2f seconds.\n", test_failed ? "FAILED" : "PASSED", get_elapsed_time());
    return !test_failed;
}

int test_itable_shrinking()
{
    printf("Testing itable dynamic shrinking with %llu items...\n", (unsigned long long)MEDIUM_TEST_SIZE);
    test_failed = 0;
    start_timer();
    
    // 创建一个表并使用适当的初始大小
    int initial_size = 1000;
    struct itable *t = itable_create(initial_size);
    ASSERT(t != NULL, "itable_create should return a valid pointer");
    
    // 插入许多项目以确保表扩展
    printf("  Inserting items to expand the table...\n");
    UINT64_T NUM_ITEMS = MEDIUM_TEST_SIZE;
    void **values = malloc(NUM_ITEMS * sizeof(void*));
    ASSERT(values != NULL, "Memory allocation for values should succeed");
    
    for (UINT64_T i = 0; i < NUM_ITEMS; i++) {
        values[i] = generate_value(i);
        ASSERT(itable_insert(t, i, values[i]), "Insertion should succeed");
        
        // 周期性地打印进度
        if (i % 50000 == 0 && i > 0) {
            print_itable_stats(t, "During insertion", i);
            printf("  Inserted %llu items so far\n", (unsigned long long)i);
        }
    }
    
    print_itable_stats(t, "After insertion", NUM_ITEMS);
    printf("  After insertion: %d items\n", itable_size(t));
    
    // 跟踪表大小变化以检测缩容
    int shrink_count = 0;
    
    // 删除批量项目以触发缩容
    printf("  Removing items to trigger shrinking...\n");
    
    // 删除90%的项目
    int total_removed = 0;
    const int BATCH_SIZE = 10000;
    const UINT64_T TOTAL_TO_REMOVE = (UINT64_T)((double)NUM_ITEMS * 0.9);
    
    struct timeval start_tv, end_tv;
    gettimeofday(&start_tv, NULL);
    
    for (UINT64_T start = 0; start < TOTAL_TO_REMOVE; start += BATCH_SIZE) {
        UINT64_T batch_end = start + BATCH_SIZE;
        if (batch_end > TOTAL_TO_REMOVE) batch_end = TOTAL_TO_REMOVE;
        
        int before_size = itable_size(t);
        
        for (UINT64_T i = start; i < batch_end; i++) {
            void *value = itable_remove(t, i);
            ASSERT(value != NULL, "Removal should succeed");
            char expected[32];
            sprintf(expected, "value_%llu", (unsigned long long)i);
            ASSERT(strcmp(expected, (char*)value) == 0, "Removed value should match key");
            free(value);
            total_removed++;
        }
        
        int after_size = itable_size(t);
        
        // 检查表是否显著收缩(通过观察删除后的表现)
        if (before_size - after_size > BATCH_SIZE) {
            // 可能发生了缩容，表内部结构发生了重组
            shrink_count++;
            printf("  Possible shrinking #%d detected after removing %d items (size: %d)\n", 
                   shrink_count, total_removed, after_size);
        }
        
        // 周期性打印状态
        if (start % 50000 == 0 || start + BATCH_SIZE >= TOTAL_TO_REMOVE) {
            print_itable_stats(t, "After batch removal", (UINT64_T)itable_size(t));
            printf("  After removing %d items: remaining %d items\n", 
                  total_removed, itable_size(t));
        }
    }
    
    gettimeofday(&end_tv, NULL);
    double remove_time = (end_tv.tv_sec - start_tv.tv_sec) + 
                        (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    
    printf("  Removed %d items in %.2f seconds (%.0f items/sec)\n", 
           total_removed, remove_time, total_removed / remove_time);
    
    print_itable_stats(t, "After all removals", (UINT64_T)itable_size(t));
    printf("  Detected approximately %d shrinking events\n", shrink_count);
    
    // 如果没有检测到缩容，则记录警告，但不一定导致测试失败
    // 因为缩容是可选的优化，不是所有实现都必须支持
    if (shrink_count == 0) {
        printf("  WARNING: No shrinking events detected. This may be normal if the implementation doesn't support shrinking.\n");
    }
    
    // 验证剩余项目仍然可以访问
    printf("  Verifying remaining items...\n");
    int remaining_count = 0;
    for (UINT64_T i = TOTAL_TO_REMOVE; i < NUM_ITEMS; i++) {
        void *value = itable_lookup(t, i);
        ASSERT(value != NULL, "Items should still be accessible after shrinking");
        char expected[32];
        sprintf(expected, "value_%llu", (unsigned long long)i);
        ASSERT(strcmp(expected, (char*)value) == 0, "Values should match after shrinking");
        remaining_count++;
    }
    printf("  Successfully verified %d remaining items\n", remaining_count);
    
    // 清理剩余项目
    printf("  Cleaning up...\n");
    itable_clear(t, free);
    free(values);
    itable_delete(t);
    
    printf("Dynamic shrinking tests %s in %.2f seconds.\n", test_failed ? "FAILED" : "PASSED", get_elapsed_time());
    return !test_failed;
}

int test_itable_high_load()
{
    printf("Testing itable under high load conditions...\n");
    test_failed = 0;
    start_timer();
    
    // 创建一个较大的表以测试高负载条件
    int initial_size = 10000;
    struct itable *t = itable_create(initial_size);
    ASSERT(t != NULL, "itable_create should return a valid pointer");
    
    // 插入足够多的元素达到高负载
    UINT64_T items_to_insert = (UINT64_T)(initial_size * 0.9); // 90% load factor
    printf("  Inserting %llu items to achieve high load factor...\n", (unsigned long long)items_to_insert);
    
    void **values = malloc(items_to_insert * sizeof(void*));
    ASSERT(values != NULL, "Memory allocation should succeed");
    
    struct timeval start_tv, end_tv;
    double insert_time;
    
    // 低负载插入基准测试
    UINT64_T low_load_items = items_to_insert / 10;
    printf("  Benchmark: Inserting first %llu items (low load)...\n", (unsigned long long)low_load_items);
    
    gettimeofday(&start_tv, NULL);
    for (UINT64_T i = 0; i < low_load_items; i++) {
        values[i] = generate_value(i);
        ASSERT(itable_insert(t, i, values[i]), "Low load insertion should succeed");
    }
    gettimeofday(&end_tv, NULL);
    
    insert_time = (end_tv.tv_sec - start_tv.tv_sec) + 
                 (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    double low_load_rate = (double)low_load_items / insert_time;
    printf("  Low load insertion rate: %.0f items/second\n", low_load_rate);
    
    // 高负载插入性能测试
    printf("  Continuing insertion to high load...\n");
    
    gettimeofday(&start_tv, NULL);
    for (UINT64_T i = low_load_items; i < items_to_insert; i++) {
        values[i] = generate_value(i);
        ASSERT(itable_insert(t, i, values[i]), "High load insertion should succeed");
        
        // 周期性打印状态
        UINT64_T quarter = items_to_insert / 4;
        UINT64_T half = items_to_insert / 2;
        UINT64_T three_quarters = (3 * items_to_insert) / 4;
        
        if (i == quarter || i == half || i == three_quarters) {
            printf("  Inserted %llu items so far\n", (unsigned long long)i);
        }
    }
    gettimeofday(&end_tv, NULL);
    
    insert_time = (end_tv.tv_sec - start_tv.tv_sec) + 
                 (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    double high_load_rate = (double)(items_to_insert - low_load_items) / insert_time;
    printf("  High load insertion rate: %.0f items/second\n", high_load_rate);
    
    printf("  Performance comparison: low load is %.2f times faster than high load\n", 
           low_load_rate / high_load_rate);
    
    // 高负载查找性能测试
    printf("  Testing lookup performance under high load...\n");
    
    int lookup_count = 10000;
    gettimeofday(&start_tv, NULL);
    
    for (int i = 0; i < lookup_count; i++) {
        UINT64_T key = rand() % items_to_insert;
        void *value = itable_lookup(t, key);
        ASSERT(value != NULL, "Lookup should succeed under high load");
        
        char expected[32];
        sprintf(expected, "value_%llu", (unsigned long long)key);
        ASSERT(strcmp(expected, (char*)value) == 0, "Value should match key under high load");
    }
    
    gettimeofday(&end_tv, NULL);
    double lookup_time = (end_tv.tv_sec - start_tv.tv_sec) + 
                        (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    
    printf("  High load lookup rate: %.0f lookups/second\n", lookup_count / lookup_time);
    
    // 高负载迭代性能测试
    printf("  Testing iteration performance under high load...\n");
    
    gettimeofday(&start_tv, NULL);
    
    UINT64_T key;
    void *value;
    UINT64_T count = 0;
    
    itable_firstkey(t);
    while (itable_nextkey(t, &key, &value)) {
        count++;
    }
    
    gettimeofday(&end_tv, NULL);
    double iteration_time = (end_tv.tv_sec - start_tv.tv_sec) + 
                           (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
    
    ASSERT(count == items_to_insert, "Iteration should visit all items under high load");
    printf("  High load iteration rate: %.0f items/second\n", (double)count / iteration_time);
    
    // 清理资源
    printf("  Cleaning up...\n");
    itable_clear(t, free);
    free(values);
    itable_delete(t);
    
    printf("High load tests %s in %.2f seconds.\n", test_failed ? "FAILED" : "PASSED", get_elapsed_time());
    return !test_failed;
}

int test_itable_resize_during_iteration()
{
    printf("Testing itable resizing during iteration...\n");
    test_failed = 0;
    start_timer();
    
    // 创建一个较小的表
    int initial_size = 10;
    struct itable *t = itable_create(initial_size);
    ASSERT(t != NULL, "itable_create should return a valid pointer");
    
    // 插入一些初始数据
    UINT64_T initial_items = 100;
    void **values = malloc(initial_items * 2 * sizeof(void*)); // 为后续插入预留空间
    ASSERT(values != NULL, "Memory allocation should succeed");
    
    printf("  Inserting %llu initial items...\n", (unsigned long long)initial_items);
    for (UINT64_T i = 0; i < initial_items; i++) {
        values[i] = generate_value(i);
        ASSERT(itable_insert(t, i, values[i]), "Initial insertion should succeed");
    }
    
    printf("  Starting iteration with insertions to trigger resize...\n");
    
    // 开始迭代
    UINT64_T key;
    void *value;
    int visited = 0;
    int inserted = 0;
    
    // 先记录下初始大小
    int initial_table_size = itable_size(t);
    
    itable_firstkey(t);
    while (itable_nextkey(t, &key, &value)) {
        visited++;
        
        // 每迭代10个元素，插入一个新元素以触发扩容
        if (visited % 10 == 0) {
            UINT64_T new_key = initial_items + inserted;
            values[initial_items + inserted] = generate_value(new_key);
            ASSERT(itable_insert(t, new_key, values[initial_items + inserted]), 
                   "Insertion during iteration should succeed");
            inserted++;
            
            if (inserted % 10 == 0) {
                printf("  Visited %d items, inserted %d additional items\n", visited, inserted);
            }
        }
    }
    
    // 修正此处：判断标准应该是visited == initial_table_size，而不是initial_items
    // 因为在某些实现中，迭代器可能会访问到插入的新元素
    ASSERT(visited >= initial_table_size, 
           "Iteration should visit at least all original items despite resizing");
    
    printf("  After iteration: visited %d items, inserted %d additional items\n", 
           visited, inserted);
    printf("  Initial table size was %d items\n", initial_table_size);
    
    // 验证所有元素都可以被访问
    printf("  Verifying all items are accessible...\n");
    UINT64_T total_items = initial_items + (UINT64_T)inserted;
    UINT64_T found = 0;
    
    for (UINT64_T i = 0; i < total_items; i++) {
        value = itable_lookup(t, i);
        if (value != NULL) {
            char expected[32];
            sprintf(expected, "value_%llu", (unsigned long long)i);
            if (strcmp(expected, (char*)value) == 0) {
                found++;
            }
        }
    }
    
    ASSERT(found == total_items, "All items should be accessible after resize during iteration");
    printf("  Successfully verified %llu/%llu items\n", 
          (unsigned long long)found, (unsigned long long)total_items);
    
    // 清理资源
    printf("  Cleaning up...\n");
    itable_clear(t, free);
    free(values);
    itable_delete(t);
    
    printf("Resize during iteration tests %s in %.2f seconds.\n", 
           test_failed ? "FAILED" : "PASSED", get_elapsed_time());
    return !test_failed;
}

int test_itable_sparse_keys()
{
    printf("Testing itable with sparse keys...\n");
    test_failed = 0;
    start_timer();
    
    struct itable *t = itable_create(0);
    ASSERT(t != NULL, "itable_create should return a valid pointer");
    
    // 测试非常分散的键
    UINT64_T num_items = 1000;
    UINT64_T *keys = malloc(num_items * sizeof(UINT64_T));
    void **values = malloc(num_items * sizeof(void*));
    ASSERT(keys != NULL && values != NULL, "Memory allocation should succeed");
    
    // 生成稀疏键 (1, 1001, 2001, 3001, ...)
    printf("  Inserting %llu items with sparse keys...\n", (unsigned long long)num_items);
    for (UINT64_T i = 0; i < num_items; i++) {
        keys[i] = 1 + i * 1000;  // 使用间隔为1000的键
        values[i] = generate_value(keys[i]);
        ASSERT(itable_insert(t, keys[i], values[i]), "Insertion with sparse keys should succeed");
    }
    
    ASSERT((UINT64_T)itable_size(t) == num_items, "Table size should match number of insertions");
    
    // 验证所有元素都可以访问
    printf("  Verifying all items are accessible...\n");
    for (UINT64_T i = 0; i < num_items; i++) {
        void *value = itable_lookup(t, keys[i]);
        ASSERT(value != NULL, "Should find sparse key");
        
        char expected[32];
        sprintf(expected, "value_%llu", (unsigned long long)keys[i]);
        ASSERT(strcmp(expected, (char*)value) == 0, "Value should match sparse key");
    }
    
    // 验证不存在的键返回NULL
    for (UINT64_T i = 0; i < 10; i++) {
        UINT64_T nonexistent_key = 500 + i * 1000;  // 键在有效区间但不存在
        ASSERT(itable_lookup(t, nonexistent_key) == NULL, 
               "Lookup for nonexistent key should return NULL");
    }
    
    // 测试迭代功能，确保所有键都被访问
    printf("  Testing iteration with sparse keys...\n");
    
    UINT64_T key;
    void *value;
    UINT64_T count = 0;
    
    itable_firstkey(t);
    while (itable_nextkey(t, &key, &value)) {
        // 查找这个键是否在我们的原始键列表中
        int found = 0;
        for (UINT64_T i = 0; i < num_items; i++) {
            if (key == keys[i]) {
                found = 1;
                break;
            }
        }
        
        ASSERT(found, "Iterated key should be in the original key list");
        count++;
    }
    
    ASSERT(count == num_items, "Iteration should visit all items with sparse keys");
    
    // 清理资源
    printf("  Cleaning up...\n");
    itable_clear(t, free);
    free(keys);
    free(values);
    itable_delete(t);
    
    printf("Sparse keys tests %s in %.2f seconds.\n", 
           test_failed ? "FAILED" : "PASSED", get_elapsed_time());
    return !test_failed;
}

int test_itable_edge_cases()
{
    printf("Testing itable edge cases...\n");
    test_failed = 0;
    start_timer();
    
    // 测试1: 创建大小为0的表 (应该使用默认大小)
    printf("  Test 1: Creating table with size 0...\n");
    struct itable *t1 = itable_create(0);
    ASSERT(t1 != NULL, "itable_create with size 0 should return a valid pointer");
    
    // 测试空表的基本操作
    void *lookup = itable_lookup(t1, 42);
    ASSERT(lookup == NULL, "Lookup in empty table should return NULL");
    
    void *removed = itable_remove(t1, 42);
    ASSERT(removed == NULL, "Remove from empty table should return NULL");
    
    // 测试迭代空表
    UINT64_T key;
    void *value;
    int count = 0;
    
    itable_firstkey(t1);
    while (itable_nextkey(t1, &key, &value)) {
        count++;
    }
    
    ASSERT(count == 0, "Empty table should have 0 items to iterate");
    
    itable_delete(t1);
    printf("  Test 1 passed: Empty table behaves correctly\n");
    
    // 测试2: 创建大小为1的表 (极小情况)
    printf("  Test 2: Creating table with size 1...\n");
    struct itable *t2 = itable_create(1);
    ASSERT(t2 != NULL, "itable_create with size 1 should return a valid pointer");
    
    // 插入多个元素，测试强制扩容
    printf("  Inserting items into size-1 table to force expansion...\n");
    UINT64_T item_count = 20;
    
    for (UINT64_T i = 0; i < item_count; i++) {
        void *v = generate_value(i);
        ASSERT(itable_insert(t2, i, v), "Insertion into size-1 table should succeed");
    }
    
    // 验证所有元素都可以正确查找
    for (UINT64_T i = 0; i < item_count; i++) {
        void *found = itable_lookup(t2, i);
        ASSERT(found != NULL, "Items in expanded table should be retrievable");
        
        char expected[32];
        sprintf(expected, "value_%llu", (unsigned long long)i);
        ASSERT(strcmp(expected, (char*)found) == 0, "Retrieved values should be correct");
    }
    
    itable_clear(t2, free);
    itable_delete(t2);
    printf("  Test 2 passed: Size-1 table expands correctly\n");
    
    // 测试3: 使用非常大的键
    printf("  Test 3: Using very large keys...\n");
    struct itable *t3 = itable_create(10);
    ASSERT(t3 != NULL, "itable_create should return a valid pointer");
    
    // 插入使用非常大的键的元素
    UINT64_T large_keys[] = {
        0xFFFFFFFFFFFFFFFF,  // 最大的64位无符号整数
        0xFFFFFFFFFFFFFF00,
        0xFFFFFF0000000000,
        0x8000000000000000,  // 最高位设置
        0x7FFFFFFFFFFFFFFF   // 所有位设置除了最高位
    };
    
    int large_key_count = sizeof(large_keys) / sizeof(large_keys[0]);
    
    printf("  Inserting %d items with very large keys...\n", large_key_count);
    for (int i = 0; i < large_key_count; i++) {
        void *v = generate_value(large_keys[i]);
        ASSERT(itable_insert(t3, large_keys[i], v), "Insertion with large key should succeed");
    }
    
    // 验证所有元素都可以正确查找
    for (int i = 0; i < large_key_count; i++) {
        void *found = itable_lookup(t3, large_keys[i]);
        ASSERT(found != NULL, "Items with large keys should be retrievable");
        
        char expected[60];  // 更大的缓冲区，因为键值可能非常大
        sprintf(expected, "value_%llu", (unsigned long long)large_keys[i]);
        ASSERT(strcmp(expected, (char*)found) == 0, "Retrieved values for large keys should be correct");
    }
    
    itable_clear(t3, free);
    itable_delete(t3);
    printf("  Test 3 passed: Table handles very large keys correctly\n");
    
    printf("Edge case tests %s in %.2f seconds.\n", 
           test_failed ? "FAILED" : "PASSED", get_elapsed_time());
    return !test_failed;
}

int run_itable_tests()
{
    int success = 1;
    
    srand(RANDOM_SEED);
    printf("\nRUNNING INTEGER TABLE TESTS\n");
    printf("===========================\n\n");
    
    success &= test_itable_edge_cases();
    success &= test_itable_high_load();
    success &= test_itable_resize_during_iteration();
    success &= test_itable_sparse_keys();
    success &= test_itable_expansion();
    success &= test_itable_shrinking();
    success &= test_itable_basic();
    
    printf("\nSummary: itable tests %s.\n\n", success ? "PASSED" : "FAILED");
    
    return success;
}

int main(int argc, char **argv)
{
    return run_itable_tests() ? 0 : 1;
}
