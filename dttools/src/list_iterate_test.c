/*
Copyright (C) 2026- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* Checks LIST_ITERATE and SKIP_LIST_ITERATE, which iterate with a private cursor that the compiler deletes when the
 * loop's scope ends: nested walks of one list, break, continue, return and goto out of a walk, items removed and added
 * during a walk, empty lists, and that no walk leaves a reference to its list behind, so the list can be destroyed
 * right after any of them. Run under valgrind to also check that every cursor is freed. */

#include "list.h"
#include "skip_list.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static int failures = 0;

#define CHECK(name, condition) \
	do { \
		if (condition) { \
			printf("ok   %s\n", name); \
		} else { \
			printf("FAIL %s\n", name); \
			failures++; \
		} \
	} while (0)

static struct list *make_list(int n)
{
	struct list *l = list_create();
	for (intptr_t i = 1; i <= n; i++) {
		list_push_tail(l, (void *)i);
	}
	return l;
}

/* A list can be destroyed only when no cursor refers to it, so this checks that walks released their cursors. */
static int destroy_list(struct list *l)
{
	while (list_pop_head(l)) {
	}
	return list_destroy(l);
}

static intptr_t find_in_walk(struct list *l, intptr_t wanted)
{
	void *item;
	LIST_ITERATE(l, item)
	{
		if ((intptr_t)item == wanted) {
			return (intptr_t)item;
		}
	}
	return 0;
}

static int sum_until_goto(struct list *l, intptr_t stop)
{
	int sum = 0;
	void *item;
	LIST_ITERATE(l, item)
	{
		if ((intptr_t)item == stop) {
			goto done;
		}
		sum += (intptr_t)item;
	}
done:
	return sum;
}

static void check_lists(void)
{
	void *a, *b, *item;

	struct list *l = make_list(10);
	int pairs = 0;
	LIST_ITERATE(l, a)
	{
		LIST_ITERATE(l, b)
		{
			pairs++;
		}
	}
	CHECK("nested walks of one list visit every pair", pairs == 100);

	int count = 0;
	int ordered = 1;
	intptr_t previous = 0;
	LIST_ITERATE(l, item)
	{
		ordered = ordered && (intptr_t)item == previous + 1;
		previous = (intptr_t)item;
		count++;
	}
	CHECK("a walk visits items in order", count == 10 && ordered);

	count = 0;
	LIST_ITERATE(l, item)
	{
		if ((intptr_t)item == 4) {
			break;
		}
		count++;
	}
	int after_break = 0;
	LIST_ITERATE(l, item)
	{
		after_break++;
	}
	CHECK("break ends a walk, and the next walk starts from the head", count == 3 && after_break == 10);

	count = 0;
	LIST_ITERATE(l, item)
	{
		if ((intptr_t)item % 2) {
			continue;
		}
		count++;
	}
	CHECK("continue skips to the next item", count == 5);

	CHECK("return from inside a walk", find_in_walk(l, 7) == 7 && find_in_walk(l, 11) == 0);
	CHECK("goto out of a walk", sum_until_goto(l, 5) == 1 + 2 + 3 + 4);

	/* An inner walk that breaks must not disturb the outer one, which the shared iterator of the list did. */
	count = 0;
	LIST_ITERATE(l, a)
	{
		LIST_ITERATE(l, b)
		{
			if (b == a) {
				break;
			}
		}
		count++;
	}
	CHECK("an inner walk that breaks leaves the outer walk in place", count == 10);

	CHECK("walks leave no reference to the list", destroy_list(l));

	l = make_list(10);
	count = 0;
	LIST_ITERATE(l, item)
	{
		if ((intptr_t)item % 2 == 0) {
			list_remove(l, item);
		}
		count++;
	}
	CHECK("removing the current item continues with the next", count == 10 && list_size(l) == 5);
	int odd = 1;
	LIST_ITERATE(l, item)
	{
		odd = odd && (intptr_t)item % 2;
	}
	CHECK("removed items are gone", odd);

	count = 0;
	LIST_ITERATE(l, item)
	{
		if (count == 0) {
			list_pop_tail(l);
		}
		count++;
	}
	CHECK("removing a later item skips it", count == 4 && list_size(l) == 4);

	count = 0;
	LIST_ITERATE(l, item)
	{
		if ((intptr_t)item < 100) {
			list_push_tail(l, (void *)((intptr_t)item + 100));
		}
		count++;
	}
	CHECK("items appended during a walk are visited", count == 8 && list_size(l) == 8);
	CHECK("walks with removals leave no reference to the list", destroy_list(l));

	l = list_create();
	count = 0;
	LIST_ITERATE(l, item)
	{
		count++;
	}
	CHECK("an empty list has no items", count == 0);
	CHECK("an empty walk leaves no reference to the list", destroy_list(l));

	/* The macro declares its cursor under the name cursor, so a variable of that name outside the loop is hidden
	 * inside its body. */
	l = make_list(3);
	int cursor = 42;
	int seen = 0;
	LIST_ITERATE(l, item)
	{
		seen = 1;
	}
	CHECK("a variable named cursor outside the walk is unchanged", cursor == 42 && seen);
	destroy_list(l);
}

static void check_skip_lists(void)
{
	void *a, *b, *item;
	struct skip_list *s = skip_list_create(1, 0.5);
	for (intptr_t i = 1; i <= 10; i++) {
		double priority = -(double)i;
		skip_list_insert_arr(s, (void *)i, &priority);
	}

	int pairs = 0;
	SKIP_LIST_ITERATE(s, a)
	{
		SKIP_LIST_ITERATE(s, b)
		{
			pairs++;
		}
	}
	CHECK("nested walks of one skip list visit every pair", pairs == 100);

	int count = 0;
	SKIP_LIST_ITERATE(s, item)
	{
		if (count == 3) {
			break;
		}
		count++;
	}
	int again = 0;
	SKIP_LIST_ITERATE(s, item)
	{
		again++;
	}
	CHECK("break ends a skip list walk, and the next walk starts from the head", count == 3 && again == 10);

	struct skip_list_cursor *cur = skip_list_cursor_create(s);
	skip_list_seek(cur, 0);
	count = 0;
	SKIP_LIST_ITERATE_CURSOR(cur, item)
	{
		if ((intptr_t)item % 2 == 0) {
			skip_list_remove_here(cur);
		}
		count++;
	}
	skip_list_cursor_delete(cur);
	CHECK("a walk from a cursor may remove the current item", count == 10 && skip_list_size(s) == 5);

	count = 0;
	SKIP_LIST_ITERATE(s, item)
	{
		count++;
	}
	CHECK("removed skip list items are gone", count == 5);

	while (skip_list_pop_head(s)) {
	}
	CHECK("skip list walks leave no cursor behind", skip_list_delete(s));
}

int main(void)
{
	check_lists();
	check_skip_lists();
	printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
	return failures ? 1 : 0;
}
