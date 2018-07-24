#ifndef RUBY_TRANSIENT_HEAP_H
#define RUBY_TRANSIENT_HEAP_H

void rb_transient_heap_promote(VALUE obj);
void rb_transient_heap_dump(void);
void *rb_transient_heap_alloc(VALUE obj, size_t req_size);
void rb_transient_heap_mark(VALUE obj, const void *ptr);
void rb_transient_heap_start_marking(int full_marking);
void rb_transient_heap_finish_marking(void);
int rb_transient_heap_managed_ptr_p(const void *ptr);
void rb_transient_heap_verify(void);

#endif
