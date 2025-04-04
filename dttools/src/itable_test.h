/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef ITABLE_TEST_H
#define ITABLE_TEST_H

int test_itable_basic();

int test_itable_expansion();

int test_itable_shrinking();

int test_itable_high_load();

int test_itable_resize_during_iteration();

int test_itable_sparse_keys();

int test_itable_edge_cases();

int run_itable_tests();

#endif
