/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef ITABLE_TEST_H
#define ITABLE_TEST_H

/* 测试整数表的基本功能 */
int test_itable_basic();

/* 测试整数表的动态扩容功能 */
int test_itable_expansion();

/* 测试整数表的动态缩容功能 */
int test_itable_shrinking();

/* 测试整数表在高负载下的性能和行为 */
int test_itable_high_load();

/* 测试整数表在迭代过程中的动态扩容和缩容 */
int test_itable_resize_during_iteration();

/* 测试非连续整数键值的情况 */
int test_itable_sparse_keys();

/* 测试整数表在极端情况下的行为 */
int test_itable_edge_cases();

/* 在整数表上运行所有测试 */
int run_itable_tests();

#endif
