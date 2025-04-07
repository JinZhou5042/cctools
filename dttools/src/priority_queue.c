/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "priority_queue.h"
#include "hash_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>

#define DEFAULT_CAPACITY 127

struct element {
	void *data;
	double priority; // In this implementation, elements with bigger priorities are considered to be privileged.
};

struct priority_queue {
	int size;
	int capacity;
	struct element **elements;

	/* The following three cursors are used to iterate over the elements in the numerical order they are stored in the array, which is
	   different from the order of priorities.  Each of them has different concerns when traverse the queue Though the typical priority-based
	   traversal is done by the repeated invocation of priority_queue_peek_top and priority_queue_pop APIs, rather than using any cursors. */
	int base_cursor;   // Used in PRIORITY_QUEUE_BASE_ITERATE. It iterates from the first position and never be reset automatically.
	int static_cursor; // Used in PRIORITY_QUEUE_STATIC_ITERATE. It iterates from the last position and never be reset automatically.
	int rotate_cursor; // Used in PRIORITY_QUEUE_ROTATE_ITERATE. It iterates from the last position and can be reset when certain events happen.

	struct hash_table *data_to_idx;

	pq_key_generator_t key_generator;
	pq_key_comparator_t key_comparator;
};

static char *ptr_to_key(const void *ptr) {
    char *key = malloc(32);
    if (key && ptr) {
        snprintf(key, 32, "%p", ptr);
    }
    return key;
}

/****** Static Methods ******/

#define SAFE_PTR_TO_KEY(ptr, key_var) \
	char *key_var = ptr_to_key(ptr); \
	if (!key_var) { \
		fprintf(stderr, "Fatal error: Memory allocation failed for key.\n"); \
		return -1; \
	}

#define FREE_KEY(key_var) \
	free(key_var)


static void swap_elements(struct priority_queue *pq, int i, int j)
{
    int *i_idx = malloc(sizeof(int));
    int *j_idx = malloc(sizeof(int));

    if (!i_idx || !j_idx) {
        if (i_idx)
            free(i_idx);
        if (j_idx)
            free(j_idx);
        fprintf(stderr, "Fatal error: Memory allocation failed in swap_elements.\n");
        return;
    }

    *i_idx = j;
    *j_idx = i;

    // 使用自定义键生成器
    char *key_i = pq->key_generator(pq->elements[i]->data);
    char *key_j = pq->key_generator(pq->elements[j]->data);

    if (!key_i || !key_j) {
        free(i_idx);
        free(j_idx);
        if (key_i)
            free(key_i);
        if (key_j)
            free(key_j);
        fprintf(stderr, "Fatal error: Memory allocation failed for keys.\n");
        return;
    }

    void *old_i_val = hash_table_remove(pq->data_to_idx, key_i);
    void *old_j_val = hash_table_remove(pq->data_to_idx, key_j);

    if (old_i_val)
        free(old_i_val);
    if (old_j_val)
        free(old_j_val);

    if (!hash_table_insert(pq->data_to_idx, key_i, j_idx)) {
        free(i_idx);
        free(j_idx);
        free(key_i);
        free(key_j);
        fprintf(stderr, "Fatal error: Failed to insert into hash table.\n");
        return;
    }

    if (!hash_table_insert(pq->data_to_idx, key_j, i_idx)) {
        hash_table_remove(pq->data_to_idx, key_i);
        free(i_idx);
        free(j_idx);
        free(key_i);
        free(key_j);
        fprintf(stderr, "Fatal error: Failed to insert into hash table.\n");
        return;
    }

    free(key_i);
    free(key_j);

    struct element *temp = pq->elements[i];
    pq->elements[i] = pq->elements[j];
    pq->elements[j] = temp;
}

static int swim(struct priority_queue *pq, int k)
{
	if (!pq) {
		return 1;
	}

	while (k > 0 && pq->elements[(k - 1) / 2]->priority <= pq->elements[k]->priority) {
		swap_elements(pq, k, (k - 1) / 2);
		k = (k - 1) / 2;
	}

	return k;
}

static int sink(struct priority_queue *pq, int k)
{
	if (!pq) {
		return -1;
	}

	while (2 * k + 1 < pq->size) {
		int j = 2 * k + 1;
		if (j + 1 < pq->size && pq->elements[j]->priority <= pq->elements[j + 1]->priority) {
			j++;
		}
		if (pq->elements[k]->priority >= pq->elements[j]->priority) {
			break;
		}
		swap_elements(pq, k, j);
		k = j;
	}

	return k;
}

static int priority_queue_double_capacity(struct priority_queue *pq)
{
	if (!pq) {
		return 0;
	}

	int new_capacity = pq->capacity * 2;
	struct element **new_elements = (struct element **)malloc(sizeof(struct element *) * new_capacity);
	if (!new_elements) {
		return 0;
	}

	memcpy(new_elements, pq->elements, sizeof(struct element *) * pq->size);

	free(pq->elements);
	pq->elements = new_elements;
	pq->capacity = new_capacity;

	return 1;
}

/****** External Methods ******/

/* 实现自定义键生成器的创建函数 */
struct priority_queue *priority_queue_create_with_custom_key(
    int init_capacity, 
    pq_key_generator_t key_generator, 
    pq_key_comparator_t key_comparator)
{
    struct priority_queue *pq = (struct priority_queue *)malloc(sizeof(struct priority_queue));
    if (!pq) {
        return NULL;
    }
    
    if (init_capacity < 1) {
        init_capacity = DEFAULT_CAPACITY;
    }
    
    pq->elements = (struct element **)calloc(init_capacity, sizeof(struct element *));
    if (!pq->elements) {
        free(pq);
        fprintf(stderr, "Fatal error: Memory allocation failed.\n");
        exit(EXIT_FAILURE);
        return NULL;
    }
    
    pq->data_to_idx = hash_table_create(0, 0);
    if (!pq->data_to_idx) {
        free(pq->elements);
        free(pq);
        fprintf(stderr, "Fatal error: Memory allocation failed.\n");
        exit(EXIT_FAILURE);
        return NULL;
    }
    
    pq->capacity = init_capacity;
    pq->size = 0;
    
    pq->static_cursor = 0;
    pq->base_cursor = 0;
    pq->rotate_cursor = 0;
    
    /* 设置自定义键函数，如果未提供则使用默认 */
    pq->key_generator = key_generator ? key_generator : ptr_to_key;
    pq->key_comparator = key_comparator ? key_comparator : NULL;
    
    return pq;
}

/* 为了向后兼容，原始创建函数调用新函数 */
struct priority_queue *priority_queue_create(int init_capacity)
{
    return priority_queue_create_with_custom_key(init_capacity, NULL, NULL);
}

int priority_queue_size(struct priority_queue *pq)
{
	if (!pq) {
		return -1;
	}

	return pq->size;
}

/* 实现push_or_update辅助函数 */
int priority_queue_push_or_update(struct priority_queue *pq, void *data, double priority)
{
    if (!pq) {
        return -1;
    }
    
    int idx = priority_queue_find_idx(pq, data);
    if (idx != -1) {
        /* 元素已存在，更新其优先级 */
        return priority_queue_update_priority(pq, data, priority);
    } else {
        /* 元素不存在，添加它 */
        return priority_queue_push(pq, data, priority);
    }
}

/* 修改push使用自定义键生成器 */
int priority_queue_push(struct priority_queue *pq, void *data, double priority)
{
    if (!pq || !data) {
        return -1;
    }
    
    if (pq->size >= pq->capacity) {
        if (!priority_queue_double_capacity(pq)) {
            return -1;
        }
    }
    
    /* 检查数据是否已在队列中 */
    if (priority_queue_find_idx(pq, data) != -1) {
        return -1;
    }
    
    struct element *e = (struct element *)malloc(sizeof(struct element));
    if (!e) {
        return -1;
    }
    e->data = data;
    e->priority = priority;
    
    pq->elements[pq->size++] = e;
    
    int idx = pq->size - 1;
    int *idx_ptr = malloc(sizeof(int));
    if (!idx_ptr) {
        pq->size--;
        free(e);
        return -1;
    }
    *idx_ptr = idx;
    
    /* 使用自定义键生成函数 */
    char *key = pq->key_generator(data);
    if (!key) {
        pq->size--;
        free(e);
        free(idx_ptr);
        return -1;
    }
    
    hash_table_insert(pq->data_to_idx, key, idx_ptr);
    free(key);
    
    int new_idx = swim(pq, pq->size - 1);
    
    if (new_idx <= pq->rotate_cursor) {
        priority_queue_rotate_reset(pq);
    }
    
    return new_idx;
}

/* 同样需要修改pop, remove等操作以使用自定义键生成器... */
void *priority_queue_pop(struct priority_queue *pq)
{
    if (!pq || pq->size == 0) {
        return NULL;
    }

    struct element *e = pq->elements[0];
    void *data = e->data;

    /* 使用自定义键生成器 */
    char *key = pq->key_generator(data);
    if (key) {
        hash_table_remove(pq->data_to_idx, key);
        free(key);
    }

    if (pq->size > 1) {
        void *last_data = pq->elements[pq->size - 1]->data;
        char *last_key = pq->key_generator(last_data);

        if (last_key) {
            hash_table_remove(pq->data_to_idx, last_key);

            int *idx_ptr = malloc(sizeof(int));
            if (idx_ptr) {
                *idx_ptr = 0;
                hash_table_insert(pq->data_to_idx, last_key, idx_ptr);
            }

            free(last_key);
        }
    }

    pq->elements[0] = pq->elements[--pq->size];
    pq->elements[pq->size] = NULL;

    sink(pq, 0);
    free(e);

    return data;
}

void *priority_queue_peek_top(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return NULL;
	}

	return pq->elements[0]->data;
}

double priority_queue_get_priority(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 0 || idx > pq->size - 1) {
		return NAN;
	}

	return pq->elements[idx]->priority;
}

void *priority_queue_peek_at(struct priority_queue *pq, int idx)
{
	if (!pq || pq->size < 1 || idx < 0 || idx > pq->size - 1) {
		return NULL;
	}

	return pq->elements[idx]->data;
}

char *priority_queue_get_key(struct priority_queue *pq, const void *data) {
	if (!pq || !data) {
		return NULL;
	}

	char *key = pq->key_generator(data);
	if (!key) {
		return NULL;
	}

	return key;
}


int priority_queue_update_priority(struct priority_queue *pq, void *data, double new_priority)
{
    if (!pq || !data) {
        return -1;  // Invalid input
    }
    
    // 使用键生成器获取键
    char *key = pq->key_generator ? pq->key_generator(data) : NULL;
    if (!key) {
        // 没有键生成器或键生成失败，回退到使用指针作为键
        char pointer_key[64];
        snprintf(pointer_key, sizeof(pointer_key), "%p", data);
        key = strdup(pointer_key);
        if (!key) {
            return -1;  // 内存分配失败
        }
    }
    
    // 从哈希表中查找索引
    int *idx_ptr = hash_table_lookup(pq->data_to_idx, key);
    int idx = idx_ptr ? *idx_ptr : -1;
    free(key);  // 释放键
    
    // 如果未找到，添加为新元素
    if (idx == -1) {
        return priority_queue_push(pq, data, new_priority);
    }
    
    // 重要：验证索引是否有效且元素不为NULL
    if (idx < 0 || idx >= pq->size || !pq->elements[idx]) {
        return priority_queue_push(pq, data, new_priority);
    }
    
    // 索引有效，保存旧优先级
    double old_priority = pq->elements[idx]->priority;
    
    // 如果优先级没有变化，无需更新
    if (old_priority == new_priority) {
        return idx;
    }
    
    // 更新优先级
    pq->elements[idx]->priority = new_priority;
    
    // 根据优先级变化调整堆
    if (new_priority > old_priority) {
        // 优先级增加，向上调整
        return swim(pq, idx);
    } else {
        // 优先级减少，向下调整
        return sink(pq, idx);
    }
}

/* 修改find_idx使用自定义键生成器 */
int priority_queue_find_idx(struct priority_queue *pq, const void *data)
{
    if (!pq || !data) {
        return -1;
    }
    
    /* 使用自定义键生成函数 */
    char *key = pq->key_generator(data);
    if (!key) {
        return -1;
    }
    
    int *idx_ptr = hash_table_lookup(pq->data_to_idx, key);
    free(key);
    
    return idx_ptr ? *idx_ptr : -1;
}

int priority_queue_static_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return -1;
	}

	int static_idx = pq->static_cursor;
	pq->static_cursor++;

	if (pq->static_cursor > pq->size - 1) {
		pq->static_cursor = 0;
	}

	return static_idx;
}

void priority_queue_base_reset(struct priority_queue *pq)
{
	if (!pq) {
		return;
	}

	pq->base_cursor = 0;
}

/*
Advance the base cursor and return it, should be used only in PRIORITY_QUEUE_BASE_ITERATE
*/

int priority_queue_base_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return -1;
	}

	int base_idx = pq->base_cursor;
	pq->base_cursor++;

	if (pq->base_cursor > pq->size - 1) {
		priority_queue_base_reset(pq);
	}

	return base_idx;
}

void priority_queue_rotate_reset(struct priority_queue *pq)
{
	if (!pq) {
		return;
	}

	pq->rotate_cursor = 0;
}

int priority_queue_rotate_next(struct priority_queue *pq)
{
	if (!pq || pq->size == 0) {
		return -1;
	}

	int rotate_idx = pq->rotate_cursor;
	pq->rotate_cursor++;

	if (pq->rotate_cursor > pq->size - 1) {
		priority_queue_rotate_reset(pq);
	}

	return rotate_idx;
}

int priority_queue_remove_by_key(struct priority_queue *pq, const char *key) {
    if (!pq || !key) {
        return 0;
    }

    int *idx_ptr = hash_table_lookup(pq->data_to_idx, key);
    if (!idx_ptr) {
        return 0;
    }

    int idx = *idx_ptr;
    free(idx_ptr);

    return priority_queue_remove(pq, idx);
}

int priority_queue_find_idx_by_key(struct priority_queue *pq, const char *key) {
    if (!pq || !key) {
        return -1;
    }

    int *idx_ptr = hash_table_lookup(pq->data_to_idx, key);
    
    return idx_ptr ? *idx_ptr : -1;
}

int priority_queue_remove(struct priority_queue *pq, int idx)
{
    if (!pq || idx < 0 || idx >= pq->size) {
        return 0;
    }

    struct element *to_delete = pq->elements[idx];
    struct element *last_elem = pq->elements[pq->size - 1];

    // 保存优先级信息，避免后面use-after-free
    double old_priority = to_delete->priority;
    double new_priority = last_elem->priority;
    void *data = to_delete->data;

    // 使用自定义键生成器删除哈希表条目
    char *key = pq->key_generator(data);
    if (key) {
        hash_table_remove(pq->data_to_idx, key);
        free(key);
    }

    pq->size--;
    
    if (idx != pq->size) {
        // 把最后一个元素移到被删除元素的位置
        void *last_data = last_elem->data;
        
        // 使用自定义键生成器更新最后元素在哈希表中的索引
        char *last_key = pq->key_generator(last_data);
        if (last_key) {
            // 从哈希表中移除最后元素的旧索引
            int *old_idx_ptr = hash_table_remove(pq->data_to_idx, last_key);
            if (old_idx_ptr) {
                free(old_idx_ptr);
            }

            // 添加最后元素的新索引
            int *new_idx_ptr = malloc(sizeof(int));
            if (new_idx_ptr) {
                *new_idx_ptr = idx;
                hash_table_insert(pq->data_to_idx, last_key, new_idx_ptr);
            }
            
            free(last_key);
        }

        // 将最后元素移到idx位置
        pq->elements[idx] = last_elem;
        pq->elements[pq->size] = NULL;

        // 维护堆性质
        if (new_priority > old_priority) {
            swim(pq, idx);
        } else if (new_priority < old_priority) {
            sink(pq, idx);
        }
    } else {
        // 如果删除的是最后一个元素，直接设置为NULL
        pq->elements[pq->size] = NULL;
    }

    // 调整游标位置
    if (pq->static_cursor >= pq->size) {
        pq->static_cursor = pq->size > 0 ? pq->size - 1 : 0;
    } else if (pq->static_cursor == idx && pq->static_cursor > 0) {
        pq->static_cursor--;
    }
    
    if (pq->base_cursor >= pq->size) {
        pq->base_cursor = pq->size > 0 ? pq->size - 1 : 0;
    } else if (pq->base_cursor == idx && pq->base_cursor > 0) {
        pq->base_cursor--;
    }

    if (pq->rotate_cursor >= pq->size) {
        pq->rotate_cursor = pq->size > 0 ? pq->size - 1 : 0;
    } else if (pq->rotate_cursor == idx && pq->rotate_cursor > 0) {
        pq->rotate_cursor--;
    }

    if (idx <= pq->rotate_cursor) {
        priority_queue_rotate_reset(pq);
    }

    // 释放要删除的元素
    free(to_delete);

    return 1;
}

void priority_queue_delete(struct priority_queue *pq)
{
	if (!pq) {
		return;
	}

	for (int i = 0; i < pq->size; i++) {
		if (pq->elements[i]) {
			free(pq->elements[i]);
		}
	}
	free(pq->elements);

	hash_table_delete(pq->data_to_idx);

	free(pq);
}
