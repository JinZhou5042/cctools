/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "itable.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_SIZE 127
#define DEFAULT_LOAD 0.75

struct entry {
	UINT64_T key;
	void *value;
	struct entry *next;
};

struct itable {
	int size;
	int bucket_count;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;
};

static int itable_halve_buckets(struct itable *h)
{
    // 防止空指针引用和无效参数
    if (!h || h->bucket_count <= DEFAULT_SIZE) {
        return 1;
    }
    
    // 计算新的桶数量（减半）
    int new_bucket_count = h->bucket_count / 2;
    if (new_bucket_count < DEFAULT_SIZE) new_bucket_count = DEFAULT_SIZE;
    
    // 记录当前迭代状态
    int was_iterating = (h->ientry != NULL);
    UINT64_T current_key = 0;
    
    if (was_iterating && h->ientry) {
        // 保存当前key
        current_key = h->ientry->key;
    }
    
    // 创建一个新的哈希表
    struct itable *hn = itable_create(new_bucket_count);
    if (!hn) {
        return 0;
    }
    
    // 移动所有键值对到新哈希表
    UINT64_T key;
    void *value;
    int insert_failed = 0;
    
    itable_firstkey(h);
    while (itable_nextkey(h, &key, &value)) {
        if (!itable_insert(hn, key, value)) {
            insert_failed = 1;
        }
    }
    
    // 如果任何插入失败，中止缩容操作
    if (insert_failed) {
        itable_delete(hn);
        return 0;
    }
    
    // 确保size一致性
    if (h->size != hn->size) {
        itable_delete(hn);
        return 0;
    }
    
    // 保存新哈希表的桶和大小信息
    struct entry **old_buckets = h->buckets;
    int old_bucket_count = h->bucket_count;
    
    // 更新原哈希表
    h->buckets = hn->buckets;
    h->bucket_count = hn->bucket_count;
    h->size = hn->size;  // 显式设置大小，确保一致性
    
    // 防止双重释放
    hn->buckets = NULL;
    
    // 恢复迭代状态
    if (was_iterating && current_key) {
        // 尝试直接使用哈希函数快速定位
        UINT64_T index = current_key % h->bucket_count;
        h->ibucket = index;
        h->ientry = h->buckets[index];
        
        // 在桶中查找确切的条目
        while (h->ientry && h->ientry->key != current_key) {
            h->ientry = h->ientry->next;
        }
        
        // 如果直接查找失败，回退到完整遍历
        if (!h->ientry) {
            itable_firstkey(h);
            UINT64_T next_key;
            void *next_value;
            
            while (itable_nextkey(h, &next_key, &next_value)) {
                if (next_key == current_key) {
                    // 找到当前位置
                    break;
                }
            }
        }
    } else {
        // 重置迭代状态
        h->ientry = NULL;
        h->ibucket = 0;
    }
    
    // 释放临时资源
    free(hn);
    
    // 释放旧桶中的所有条目
    struct entry *e, *f;
    int i;
    for (i = 0; i < old_bucket_count; i++) {
        e = old_buckets[i];
        while (e) {
            f = e->next;
            free(e);
            e = f;
        }
    }
    free(old_buckets);
    
    
    return 1;
}

struct itable *itable_create(int bucket_count)
{
	struct itable *h;

	h = (struct itable *)malloc(sizeof(struct itable));
	if (!h)
		return 0;

	if (bucket_count == 0)
		bucket_count = DEFAULT_SIZE;

	h->bucket_count = bucket_count;
	h->buckets = (struct entry **)calloc(bucket_count, sizeof(struct entry *));
	if (!h->buckets) {
		free(h);
		return 0;
	}

	h->size = 0;

	return h;
}

void itable_clear(struct itable *h, void (*delete_func)(void *))
{
	struct entry *e, *f;
	int i;

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			if (delete_func)
				delete_func(e->value);
			f = e->next;
			free(e);
			e = f;
		}
	}

	for (i = 0; i < h->bucket_count; i++) {
		h->buckets[i] = 0;
	}
}

void itable_delete(struct itable *h)
{
	itable_clear(h, 0);
	free(h->buckets);
	free(h);
}

int itable_size(struct itable *h)
{
	return h->size;
}

void *itable_lookup(struct itable *h, UINT64_T key)
{
	struct entry *e;
	UINT64_T index;

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			return e->value;
		}
		e = e->next;
	}

	return 0;
}

static int itable_double_buckets(struct itable *h)
{
	struct itable *hn = itable_create(2 * h->bucket_count);

	if (!hn)
		return 0;

	/* Move pairs to new hash */
	uint64_t key;
	void *value;
	itable_firstkey(h);
	while (itable_nextkey(h, &key, &value))
		if (!itable_insert(hn, key, value)) {
			itable_delete(hn);
			return 0;
		}

	/* Delete all old pairs */
	struct entry *e, *f;
	int i;
	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			f = e->next;
			free(e);
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets = hn->buckets;
	h->bucket_count = hn->bucket_count;
	h->size = hn->size;

	/* Delete reference to new, so old is safe */
	free(hn);

	return 1;
}

int itable_insert(struct itable *h, UINT64_T key, const void *value)
{
	struct entry *e;
	UINT64_T index;

	if (((float)h->size / h->bucket_count) > DEFAULT_LOAD)
		itable_double_buckets(h);

	index = key % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (key == e->key) {
			e->value = (void *)value;
			return 1;
		}
		e = e->next;
	}

	e = (struct entry *)malloc(sizeof(struct entry));
	if (!e)
		return 0;

	e->key = key;
	e->value = (void *)value;
	e->next = h->buckets[index];
	h->buckets[index] = e;
	h->size++;

	return 1;
}

void *itable_remove(struct itable *h, UINT64_T key)
{
    if (!h) return 0;
    
    struct entry *e, *f;
    void *value;
    UINT64_T index;

    index = key % h->bucket_count;
    e = h->buckets[index];
    f = 0;

    while (e) {
        if (key == e->key) {
            if (f) {
                f->next = e->next;
            } else {
                h->buckets[index] = e->next;
            }
            value = e->value;
            free(e);
            h->size--;
            
            // 检查是否需要缩容
            if (h->bucket_count > DEFAULT_SIZE && 
                ((float)h->size / h->bucket_count) < 0.25) { // 使用0.25作为缩容阈值
                
                // 添加防抖动机制，避免频繁的扩缩容操作
                if ((float)h->size / h->bucket_count < 0.2) {
                    float expected_load = (float)h->size / (h->bucket_count / 2);
                    
                    // 如果缩容后负载因子会接近扩容阈值，则不缩容
                    if (expected_load < DEFAULT_LOAD * 0.9) {
                        int result = itable_halve_buckets(h);
                        if (!result) {
                        }
                    }
                }
            }
            
            return value;
        }
        f = e;
        e = e->next;
    }

    return 0;
}

void *itable_pop(struct itable *t)
{
	UINT64_T key;
	void *value;

	itable_firstkey(t);
	if (itable_nextkey(t, &key, (void **)&value)) {
		return itable_remove(t, key);
	} else {
		return 0;
	}
}

void itable_firstkey(struct itable *h)
{
	h->ientry = 0;
	for (h->ibucket = 0; h->ibucket < h->bucket_count; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry)
			break;
	}
}

int itable_nextkey(struct itable *h, UINT64_T *key, void **value)
{
	if (h->ientry) {
		*key = h->ientry->key;
		if (value)
			*value = h->ientry->value;

		h->ientry = h->ientry->next;
		if (!h->ientry) {
			h->ibucket++;
			for (; h->ibucket < h->bucket_count; h->ibucket++) {
				h->ientry = h->buckets[h->ibucket];
				if (h->ientry)
					break;
			}
		}

		return 1;
	} else {
		return 0;
	}
}

/* vim: set noexpandtab tabstop=8: */
