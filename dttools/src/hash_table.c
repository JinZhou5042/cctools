/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "hash_table.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_SIZE 127
#define DEFAULT_MAX_LOAD 0.75
#define DEFAULT_MIN_LOAD 0.125
#define REMOVED_KEEP_MIN 1024 /* removed array capacity that is always kept across compactions */
#define MIGRATE_INSERT 16      /* old buckets that each insert moves while a resize is in progress */
#define MIGRATE_COMPACT 16384  /* old buckets that each compaction moves while a resize is in progress */
#define DEFAULT_FUNC hash_string

/* One key and its value. The key is stored inline, so an entry is one allocation and a lookup that matches reads one
 * object. A removed entry leaves its bucket at once, so lookups walk only live entries, but it stays allocated in the
 * table's removed array, still marked deleted and with its next pointer intact, until the table frees removed entries
 * at a compaction, a resize, or a clear. An iterator that stands on it, or a key pointer that the caller obtained from
 * it, therefore stays valid exactly as long as when removed entries stayed in their buckets. */
struct entry {
	struct entry *next; /* Next entry of the bucket, or of the bucket it left when it was removed. */
	void *value;
	unsigned hash;
	int deleted;
	char key[];
};

/* A resize moves the entries a few buckets at a time, so no single operation costs O(entries): while it is in
 * progress, old_buckets holds the buckets not moved yet, every insert moves MIGRATE_INSERT of them and every compaction
 * MIGRATE_COMPACT, and lookups and removals look in both arrays. Iterations walk the buckets and then the old buckets,
 * as one sequence of bucket_count + old_count buckets, and only inserts, compactions, and clears move entries, each of
 * which ends any iteration in progress. */
struct hash_table {
	hash_func_t hash_func;
	int bucket_count;
	int size;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;
	int iteration_index;
	int need_compact;	/* entries removed since the last compaction, which the removed array holds */
	struct entry **removed; /* removed entries, freed at the next compaction, resize, or clear */
	int removed_capacity;	/* length of the removed array */
	struct entry **old_buckets; /* buckets that a resize in progress has not moved yet, or NULL */
	int old_count;		    /* length of old_buckets, or 0 */
	int old_next;		    /* index of the first old bucket not moved yet */
	int shrinking;		    /* set while compactions halve the buckets of a table whose load is low */
};

/* The bucket at index i of the sequence that iterations walk: the buckets, then the old buckets of a resize. */
static struct entry *bucket_at(struct hash_table *h, int i)
{
	return i < h->bucket_count ? h->buckets[i] : h->old_buckets[i - h->bucket_count];
}

static int bucket_total(struct hash_table *h)
{
	return h->bucket_count + h->old_count;
}

/* Stand the iterator on the first entry of the first bucket at index i or later of the sequence that iterations walk,
 * or on none. Bucket heads are live entries, because a removed entry leaves its bucket at once. */
static void seek_bucket(struct hash_table *h, int i)
{
	h->ientry = 0;
	for (; i < h->bucket_count; i++) {
		if (h->buckets[i]) {
			h->ientry = h->buckets[i];
			h->ibucket = i;
			return;
		}
	}
	for (; i < h->bucket_count + h->old_count; i++) {
		if (h->old_buckets[i - h->bucket_count]) {
			h->ientry = h->old_buckets[i - h->bucket_count];
			h->ibucket = i;
			return;
		}
	}
	h->ibucket = i;
}

struct hash_table *hash_table_create(int bucket_count, hash_func_t func)
{
	struct hash_table *h;

	h = (struct hash_table *)malloc(sizeof(struct hash_table));
	if (!h)
		return 0;

	if (bucket_count < 1)
		bucket_count = DEFAULT_SIZE;
	if (!func)
		func = DEFAULT_FUNC;

	h->size = 0;
	h->ientry = 0;
	h->iteration_index = 0;
	h->need_compact = 0;
	h->removed = 0;
	h->removed_capacity = 0;
	h->old_buckets = 0;
	h->old_count = 0;
	h->old_next = 0;
	h->shrinking = 0;
	h->hash_func = func;
	h->bucket_count = bucket_count;
	h->buckets = (struct entry **)calloc(bucket_count, sizeof(struct entry *));
	if (!h->buckets) {
		free(h);
		return 0;
	}

	return h;
}

/* Keep a removed entry until the next compaction, resize, or clear. */
static void hold_removed(struct hash_table *h, struct entry *e)
{
	if (h->need_compact == h->removed_capacity) {
		h->removed_capacity = h->removed_capacity ? 2 * h->removed_capacity : 8;
		h->removed = xxrealloc(h->removed, sizeof(*h->removed) * h->removed_capacity);
	}
	h->removed[h->need_compact++] = e;
}

/* Free the entries removed since the last compaction. The removed array keeps its capacity for the next removals,
 * unless it holds more than REMOVED_KEEP_MIN entries and eight times the live entries plus buckets, so it never holds
 * more than a constant times the most removals between two compactions, and a table that is filled and emptied again
 * and again does not reallocate it each time. The removed entries stay in an array rather than in a list linked
 * through them: freeing walks the array in removal order without a cache miss per entry, and a list link would move
 * the inline key of a hash_table entry and slow its lookups. */
static void free_removed(struct hash_table *h)
{
	if (h->need_compact) {
		/* The iterator may stand on a removed entry. A pop resumes from it, so it starts again instead. */
		h->ientry = 0;
	}
	for (int i = 0; i < h->need_compact; i++) {
		free(h->removed[i]);
	}
	h->need_compact = 0;
	if (h->removed_capacity > REMOVED_KEEP_MIN && h->removed_capacity > 8 * (h->size + h->bucket_count)) {
		free(h->removed);
		h->removed = 0;
		h->removed_capacity = 0;
	}
}

void hash_table_clear(struct hash_table *h, void (*delete_func)(void *))
{
	struct entry *e, *f;
	int i;

	for (i = 0; i < bucket_total(h); i++) {
		e = bucket_at(h, i);
		while (e) {
			if (delete_func) {
				delete_func(e->value);
			}
			f = e->next;
			free(e);
			e = f;
		}
	}

	for (i = 0; i < h->bucket_count; i++) {
		h->buckets[i] = 0;
	}

	free(h->old_buckets);
	h->old_buckets = 0;
	h->old_count = 0;
	h->old_next = 0;
	h->shrinking = 0;

	free_removed(h);
	h->size = 0;
	h->iteration_index++;
}

void hash_table_delete(struct hash_table *h)
{
	hash_table_clear(h, 0);
	free(h->removed);
	free(h->buckets);
	free(h);
}

char **hash_table_keys_array(struct hash_table *h)
{
	char **keys = (char **)malloc(sizeof(char *) * (h->size + 1));
	int ikey = 0;

	struct entry *e, *f;
	int i;

	for (i = 0; i < bucket_total(h); i++) {
		e = bucket_at(h, i);
		while (e) {
			keys[ikey] = strdup(e->key);
			ikey++;
			f = e->next;
			e = f;
		}
	}

	keys[h->size] = NULL;

	return keys;
}

void hash_table_free_keys_array(char **keys)
{
	int i = 0;
	while (keys[i]) {
		free(keys[i]);
		i++;
	}

	free(keys);
}

/* Return the live entry of key, whose hash and bucket index are given, in the buckets or, while a resize is in
 * progress, in the old buckets, where a bucket already moved is empty. */
static struct entry *find_entry(struct hash_table *h, const char *key, unsigned hash, unsigned index)
{
	struct entry *e;
	for (e = h->buckets[index]; e; e = e->next) {
		if (hash == e->hash && !strcmp(key, e->key)) {
			return e;
		}
	}
	if (h->old_buckets) {
		for (e = h->old_buckets[hash % h->old_count]; e; e = e->next) {
			if (hash == e->hash && !strcmp(key, e->key)) {
				return e;
			}
		}
	}
	return 0;
}

void *hash_table_lookup(struct hash_table *h, const char *key)
{
	unsigned hash = h->hash_func(key);
	struct entry *e = find_entry(h, key, hash, hash % h->bucket_count);
	return e ? e->value : 0;
}

int hash_table_size(struct hash_table *h)
{
	return h->size;
}

double hash_table_load(struct hash_table *h)
{
	return ((double)h->size) / h->bucket_count;
}

/* Move the entries of up to count old buckets of a resize in progress into the buckets, in the order of the old
 * buckets, and end the resize once every old bucket was moved. Moving entries ends any iteration in progress, and a pop
 * starts again, because the iterator's bucket index no longer describes where its entry is. */
static void hash_table_migrate(struct hash_table *h, int count)
{
	if (!h->old_buckets) {
		return;
	}

	struct entry *e, *f;
	for (; count > 0 && h->old_next < h->old_count; count--, h->old_next++) {
		e = h->old_buckets[h->old_next];
		h->old_buckets[h->old_next] = 0;
		while (e) {
			f = e->next;
			unsigned index = e->hash % h->bucket_count;
			e->next = h->buckets[index];
			h->buckets[index] = e;
			e = f;
		}
	}

	if (h->old_next == h->old_count) {
		free(h->old_buckets);
		h->old_buckets = 0;
		h->old_count = 0;
		h->old_next = 0;
	}

	h->ientry = 0;
	h->iteration_index++;
}

/* Start moving every entry to a new array of new_count buckets, and free the removed entries. Inserts and compactions
 * then move the old buckets a few at a time (hash_table_migrate()), so a resize costs O(1) per operation instead of
 * O(entries) in one. The new array comes zeroed from calloc(), which maps a large one without touching its pages. */
static int hash_table_resize(struct hash_table *h, int new_count)
{
	if (h->old_buckets) {
		return 0;
	}

	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	free_removed(h);
	h->old_buckets = h->buckets;
	h->old_count = h->bucket_count;
	h->old_next = 0;
	h->buckets = new_buckets;
	h->bucket_count = new_count;

	h->ientry = 0;
	h->iteration_index++;

	return 1;
}

static int hash_table_double_buckets(struct hash_table *h)
{
	return hash_table_resize(h, (2 * (h->bucket_count + 1)) - 1);
}

/* Halve the buckets of a table whose load fell below DEFAULT_MIN_LOAD, as (n+1)/2-1 undoes the doubling to 2n+1,
 * unless the load would then exceed half of DEFAULT_MAX_LOAD, so the next inserts do not double it again at once. One
 * halving at a time keeps the old buckets of the resize at most twice the new ones, so the inserts before the next
 * doubling, at least a quarter of the old buckets, move them all. Return whether a resize started, which also frees
 * the removed entries. */
static int hash_table_reduce_buckets(struct hash_table *h)
{
	if (((float)h->size / h->bucket_count) >= DEFAULT_MIN_LOAD) {
		return 0;
	}

	int half = ((h->bucket_count + 1) / 2) - 1;
	if (half < DEFAULT_SIZE || ((float)h->size / half) > DEFAULT_MAX_LOAD / 2) {
		return 0;
	}

	return hash_table_resize(h, half);
}

/* Free the entries removed since the last compaction, move up to MIGRATE_COMPACT old buckets of a resize in
 * progress, and start halving the buckets when the load is low. A table whose removals started a shrink keeps
 * halving at later compactions until its load is no longer low. The cost is O(removed entries), amortized O(1) per
 * removal however often it is called, plus O(MIGRATE_COMPACT). A compaction that frees or moves entries ends any
 * iteration in progress, as an insert does: an iteration continued afterwards aborts instead of reading a freed
 * entry. */
int hash_table_compact(struct hash_table *h)
{
	int freed = h->need_compact;
	if (!freed && !h->old_buckets && !h->shrinking) {
		return 0;
	}

	if (!h->old_buckets) {
		h->shrinking = hash_table_reduce_buckets(h);
	}
	hash_table_migrate(h, MIGRATE_COMPACT);
	free_removed(h);
	h->iteration_index++;

	return freed;
}

int hash_table_insert(struct hash_table *h, const char *key, const void *value)
{
	struct entry *e;
	unsigned hash, index;

	/* A table above its load while a resize is in progress doubles once the resize ended, which takes fewer inserts
	 * than it took to reach that load. */
	if (h->old_buckets) {
		hash_table_migrate(h, MIGRATE_INSERT);
	} else if (((float)h->size / h->bucket_count) > DEFAULT_MAX_LOAD && hash_table_double_buckets(h)) {
		hash_table_migrate(h, MIGRATE_INSERT);
	}

	hash = h->hash_func(key);
	index = hash % h->bucket_count;
	e = find_entry(h, key, hash, index);
	if (e) {
		if (e->value != value) {
			notice(D_DEBUG, "key %s already exists in hash table with different value, ignoring new value.", key);
		}
		return 0;
	}

	/* A table that is never iterated is never compacted, so compact once removed entries outnumber the rest. A new
	 * entry ends any iteration in progress, so no iterator is left on a freed entry. */
	if (h->need_compact > h->size + h->bucket_count) {
		hash_table_compact(h);
		index = hash % h->bucket_count;
	}

	size_t length = strlen(key) + 1;
	e = (struct entry *)xxmalloc(sizeof(struct entry) + length);

	memcpy(e->key, key, length);
	e->value = (void *)value;
	e->hash = hash;
	e->deleted = 0;
	e->next = h->buckets[index];
	h->buckets[index] = e;

	h->size++;
	h->iteration_index++;

	return 1;
}

/* A removed entry leaves its bucket at once and is freed later, see struct entry. */
void *hash_table_remove(struct hash_table *h, const char *key)
{
	struct entry *e, **link;
	unsigned hash, index;

	hash = h->hash_func(key);
	index = hash % h->bucket_count;
	link = &h->buckets[index];

	for (int old = 0; old < 2; old++) {
		while ((e = *link)) {
			if (hash == e->hash && !strcmp(key, e->key)) {
				*link = e->next;
				e->deleted = 1;
				hold_removed(h, e);

				h->size--;
				return e->value;
			}

			link = &e->next;
		}
		if (!h->old_buckets) {
			break;
		}
		link = &h->old_buckets[hash % h->old_count];
	}

	return 0;
}

static void hash_table_reset_to_first(struct hash_table *h)
{
	seek_bucket(h, 0);
}

int hash_table_fromkey(struct hash_table *h, const char *key)
{
	if (key) {
		h->iteration_index++;

		unsigned hash = h->hash_func(key);
		h->ibucket = hash % h->bucket_count;
		h->ientry = h->buckets[h->ibucket];

		while (h->ientry) {
			if (hash == h->ientry->hash && !strcmp(key, h->ientry->key) && !h->ientry->deleted) {
				return h->iteration_index;
			}
			h->ientry = h->ientry->next;
		}

		if (h->old_buckets) {
			h->ibucket = h->bucket_count + hash % h->old_count;
			for (h->ientry = bucket_at(h, h->ibucket); h->ientry; h->ientry = h->ientry->next) {
				if (hash == h->ientry->hash && !strcmp(key, h->ientry->key) && !h->ientry->deleted) {
					return h->iteration_index;
				}
			}
		}
	}

	hash_table_reset_to_first(h);
	return h->iteration_index;
}

void *hash_table_pop(struct hash_table *t)
{
	char *key;
	void *value;

	if (hash_table_nextkey(t, t->iteration_index, &key, (void **)&value)) {
		t->iteration_index++;
		return hash_table_remove(t, key);
	}

	hash_table_firstkey(t);

	if (hash_table_nextkey(t, t->iteration_index, &key, (void **)&value)) {
		return hash_table_remove(t, key);
	}

	return NULL;
}

int hash_table_firstkey(struct hash_table *h)
{
	hash_table_compact(h);
	h->iteration_index++;
	hash_table_reset_to_first(h);
	return h->iteration_index;
}

int hash_table_nextkey(struct hash_table *h, int iteration, char **key, void **value)
{
	if (!h) {
		return 0;
	}

	if (iteration != h->iteration_index) {
		fatal("cctools bug: the hash table iteration has not been reset since last modification");
	}

	if (h->ientry) {
		*key = h->ientry->key;
		*value = h->ientry->value;

		h->ientry = h->ientry->next;
		while (h->ientry && h->ientry->deleted) {
			h->ientry = h->ientry->next;
		}
		if (!h->ientry) {
			seek_bucket(h, h->ibucket + 1);
		}
		return 1;
	} else {
		return 0;
	}
}

int hash_table_randomkey(struct hash_table *h, int *offset_bookkeep)
{
	h->iteration_index++;

	h->ientry = 0;
	if (h->bucket_count < 1) {
		return h->iteration_index;
	}

	int ibucket_start = random() % bucket_total(h);

	for (h->ibucket = ibucket_start; h->ibucket < bucket_total(h); h->ibucket++) {
		h->ientry = bucket_at(h, h->ibucket);
		if (h->ientry && !h->ientry->deleted) {
			*offset_bookkeep = h->ibucket;
			return h->iteration_index;
		}
	}

	for (h->ibucket = 0; h->ibucket < ibucket_start; h->ibucket++) {
		h->ientry = bucket_at(h, h->ibucket);
		if (h->ientry && !h->ientry->deleted) {
			*offset_bookkeep = h->ibucket;
			return h->iteration_index;
		}
	}

	return h->iteration_index;
}

int hash_table_nextkey_with_offset(struct hash_table *h, int iteration, int offset_bookkeep, char **key, void **value)
{
	if (iteration != h->iteration_index) {
		fatal("cctools bug: the hash table iteration has not been reset since last modification");
	}

	if (h->bucket_count < 1) {
		return 0;
	}

	offset_bookkeep = offset_bookkeep % bucket_total(h);

	if (h->ientry) {
		*key = h->ientry->key;
		*value = h->ientry->value;

		h->ientry = h->ientry->next;
		while (h->ientry && h->ientry->deleted) {
			h->ientry = h->ientry->next;
		}
		if (!h->ientry) {
			h->ibucket = (h->ibucket + 1) % bucket_total(h);
			for (; h->ibucket != offset_bookkeep; h->ibucket = (h->ibucket + 1) % bucket_total(h)) {
				h->ientry = bucket_at(h, h->ibucket);
				if (h->ientry && !h->ientry->deleted) {
					break;
				}
			}
		}
		return 1;
	}

	return 0;
}

void hash_table_foreach_ro(struct hash_table *h, void (*func)(const char *key, void *value, void *arg), void *arg)
{
	struct entry *e;
	int i;

	int prev_iteration = h->iteration_index;

	for (i = 0; i < bucket_total(h); i++) {
		e = bucket_at(h, i);
		while (e) {
			if (!e->deleted) {
				func(e->key, e->value, arg);
			}
			e = e->next;

			if (h->iteration_index != prev_iteration) {
				fatal("cctools bug: the hash table iteration has been modified since last call to hash_table_foreach_ro");
			}
		}
	}
}

typedef unsigned long int ub4; /* unsigned 4-byte quantities */
typedef unsigned char ub1;     /* unsigned 1-byte quantities */

#define hashsize(n) ((ub4)1 << (n))
#define hashmask(n) (hashsize(n) - 1)

/*
--------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.
For every delta with one or two bits set, and the deltas of all three
  high bits or all three low bits, whether the original value of a,b,c
  is almost all zero or is uniformly distributed,
* If mix() is run forward or backward, at least 32 bits in a,b,c
  have at least 1/4 probability of changing.
* If mix() is run forward, every bit of c will change between 1/3 and
  2/3 of the time.  (Well, 22/100 and 78/100 for some 2-bit deltas.)
mix() was built out of 36 single-cycle latency instructions in a
  structure that could supported 2x parallelism, like so:
	  a -= b;
	  a -= c; x = (c>>13);
	  b -= c; a ^= x;
	  b -= a; x = (a<<8);
	  c -= a; b ^= x;
	  c -= b; x = (b>>13);
	  ...
  Unfortunately, superscalar Pentiums and Sparcs can't take advantage
  of that parallelism.  They've also turned some of those single-cycle
  latency instructions into multi-cycle latency instructions.  Still,
  this is the fastest good hash I could find.  There were about 2^^68
  to choose from.  I only looked at a billion or so.
--------------------------------------------------------------------
*/

#define mix(a, b, c) \
	{ \
		a -= b; \
		a -= c; \
		a ^= (c >> 13); \
		b -= c; \
		b -= a; \
		b ^= (a << 8); \
		c -= a; \
		c -= b; \
		c ^= (b >> 13); \
		a -= b; \
		a -= c; \
		a ^= (c >> 12); \
		b -= c; \
		b -= a; \
		b ^= (a << 16); \
		c -= a; \
		c -= b; \
		c ^= (b >> 5); \
		a -= b; \
		a -= c; \
		a ^= (c >> 3); \
		b -= c; \
		b -= a; \
		b ^= (a << 10); \
		c -= a; \
		c -= b; \
		c ^= (b >> 15); \
	}

/*
--------------------------------------------------------------------
hash() -- hash a variable-length key into a 32-bit value
  k       : the key (the unaligned variable-length array of bytes)
  len     : the length of the key, counting by bytes
  initval : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Every 1-bit and 2-bit delta achieves avalanche.
About 6*len+35 instructions. The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements. If you are hashing n strings (ub1 **)k, do it like
this: for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h); By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.  You may
use this code any way you wish, private, educational, or commercial.  It's free. See
http://burtleburtle.net/bob/hash/evahash.html Use for hash table lookup, or anything where one collision in 2^^32 is
acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/

/* Ignoring implicit-fallthrough warnings, as fallthrough is what we want in the following switch-case */
/* *k the key */
/* length the length of the key */
/* initval the previous hash, or an arbitrary value */
static ub4 jenkins_hash(register const ub1 *k, register ub4 length, register ub4 initval)
{
	register ub4 a, b, c, len; /* Set up the internal state */
	len = length;
	a = b = 0x9e3779b9;			   /* the golden ratio; an arbitrary value */
	c = initval; /* the previous hash value */ /*---------------------------------------- handle most of the key */
	while (len >= 12) {
		a += (k[0] + ((ub4)k[1] << 8) + ((ub4)k[2] << 16) + ((ub4)k[3] << 24));
		b += (k[4] + ((ub4)k[5] << 8) + ((ub4)k[6] << 16) + ((ub4)k[7] << 24));
		c += (k[8] + ((ub4)k[9] << 8) + ((ub4)k[10] << 16) + ((ub4)k[11] << 24));
		mix(a, b, c);
		k += 12;
		len -= 12;
	}
	/*------------------------------------- handle the last 11 bytes */
	c += length;
	switch (len) { /* all the case statements fall through */
	case 11:
		c += ((ub4)k[10] << 24);
		/* falls through */
	case 10:
		c += ((ub4)k[9] << 16);
		/* falls through */
	case 9:
		/* the first byte of c is reserved for the length */
		c += ((ub4)k[8] << 8);
		/* falls through */
	case 8:
		b += ((ub4)k[7] << 24);
		/* falls through */
	case 7:
		b += ((ub4)k[6] << 16);
		/* falls through */
	case 6:
		b += ((ub4)k[5] << 8);
		/* falls through */
	case 5:
		b += k[4];
		/* falls through */
	case 4:
		a += ((ub4)k[3] << 24);
		/* falls through */
	case 3:
		a += ((ub4)k[2] << 16);
		/* falls through */
	case 2:
		a += ((ub4)k[1] << 8);
		/* falls through */
	case 1:
		a += k[0];
		/* falls through */
	case 0:
	default:
		/* case 0: nothing left to add */
		break;
	}
	mix(a, b, c);
	/*-------------------------------------------- report the result */
	return c;
}

unsigned hash_string(const char *s)
{
	return jenkins_hash((const ub1 *)s, strlen(s), 0);
}

/* vim: set noexpandtab tabstop=8: */
