/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HASH_TABLE_TEST_H
#define HASH_TABLE_TEST_H

/* 测试哈希表的基本功能 */
int test_hash_table_basic();

/* 测试哈希表的动态扩容功能 */
int test_hash_table_expansion();

/* 测试哈希表的动态缩容功能 */
int test_hash_table_shrinking();

/* 测试哈希表在高负载下的性能和行为 */
int test_hash_table_high_load();

/* 测试哈希表在迭代过程中的动态扩容和缩容 */
int test_hash_table_resize_during_iteration();

/* 测试哈希表的批量删除功能 */
int test_hash_table_batch_remove();

/* 测试哈希表在极端情况下的行为 */
int test_hash_table_edge_cases();

/* 在哈希表上运行所有测试 */
int run_hash_table_tests();

#endif