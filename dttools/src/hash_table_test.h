/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HASH_TABLE_TEST_H
#define HASH_TABLE_TEST_H

int test_hash_table_basic();

int test_hash_table_expansion();

int test_hash_table_shrinking();

int test_hash_table_high_load();

int test_hash_table_resize_during_iteration();

int test_hash_table_batch_remove();

int test_hash_table_edge_cases();

int run_hash_table_tests();

#endif