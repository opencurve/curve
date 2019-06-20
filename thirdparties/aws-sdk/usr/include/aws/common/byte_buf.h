#ifndef AWS_COMMON_BYTE_BUF_H
#define AWS_COMMON_BYTE_BUF_H
/*
 * Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <aws/common/array_list.h>
#include <aws/common/byte_order.h>
#include <aws/common/common.h>

#include <string.h>

/**
 * Represents a length-delimited binary string or buffer. If byte buffer points
 * to constant memory or memory that should otherwise not be freed by this
 * struct, set allocator to NULL and free function will be a no-op.
 *
 * This structure used to define the output for all functions that write to a buffer.
 *
 * Note that this structure allocates memory at the buffer pointer only. The
 * struct itself does not get dynamically allocated and must be either
 * maintained or copied to avoid losing access to the memory.
 */
struct aws_byte_buf {
    /* do not reorder this, this struct lines up nicely with windows buffer structures--saving us allocations.*/
    size_t len;
    uint8_t *buffer;
    size_t capacity;
    struct aws_allocator *allocator;
};

/**
 * Represents a movable pointer within a larger binary string or buffer.
 *
 * This structure is used to define buffers for reading.
 */
struct aws_byte_cursor {
    /* do not reorder this, this struct lines up nicely with windows buffer structures--saving us allocations */
    size_t len;
    uint8_t *ptr;
};

AWS_EXTERN_C_BEGIN

AWS_COMMON_API
int aws_byte_buf_init(struct aws_byte_buf *buf, struct aws_allocator *allocator, size_t capacity);

/**
 * Copies src buffer into dest and sets the correct len and capacity.
 * A new memory zone is allocated for dest->buffer. When dest is no longer needed it will have to be cleaned-up using
 * aws_byte_buf_clean_up(dest).
 * Dest capacity and len will be equal to the src len. Allocator of the dest will be identical with parameter allocator.
 * If src buffer is null the dest will have a null buffer with a len and a capacity of 0
 * Returns AWS_OP_SUCCESS in case of success or AWS_OP_ERR when memory can't be allocated.
 */
AWS_COMMON_API
int aws_byte_buf_init_copy_from_cursor(
    struct aws_byte_buf *dest,
    struct aws_allocator *allocator,
    struct aws_byte_cursor src);

AWS_COMMON_API
void aws_byte_buf_clean_up(struct aws_byte_buf *buf);

/**
 * Equivalent to calling aws_byte_buf_secure_zero and then aws_byte_buf_clean_up
 * on the buffer.
 */
AWS_COMMON_API
void aws_byte_buf_clean_up_secure(struct aws_byte_buf *buf);

/**
 * Sets all bytes of buffer to zero and resets len to zero.
 */
AWS_COMMON_API
void aws_byte_buf_secure_zero(struct aws_byte_buf *buf);

/**
 * Compares two aws_byte_buf structures
 * Returns true if a has the same length as b and their buffers have the same bytes
 * (or both buffers are null). When both a and b are null the function returns true
 */
AWS_COMMON_API
bool aws_byte_buf_eq(const struct aws_byte_buf *a, const struct aws_byte_buf *b);

/**
 * No copies, no buffer allocations. Iterates over input_str, and returns the next substring between split_on instances.
 *
 * Edge case rules are as follows:
 * If the input begins with split_on, an empty cursor will be the first entry returned.
 * If the input has two adjacent split_on tokens, an empty cursor will be returned.
 * If the input ends with split_on, an empty cursor will be returned last.
 *
 * It is the user's responsibility to properly zero-initialize substr.
 *
 * It is the user's responsibility to make sure the input buffer stays in memory
 * long enough to use the results.
 */
AWS_COMMON_API
bool aws_byte_cursor_next_split(
    const struct aws_byte_cursor *AWS_RESTRICT input_str,
    char split_on,
    struct aws_byte_cursor *AWS_RESTRICT substr);

/**
 * No copies, no buffer allocations. Fills in output with a list of
 * aws_byte_cursor instances where buffer is an offset into the input_str and
 * len is the length of that string in the original buffer.
 *
 * Edge case rules are as follows:
 * if the input begins with split_on, an empty cursor will be the first entry in
 * output. if the input has two adjacent split_on tokens, an empty cursor will
 * be inserted into the output. if the input ends with split_on, an empty cursor
 * will be appended to the output.
 *
 * It is the user's responsibility to properly initialize output. Recommended number of preallocated elements from
 * output is your most likely guess for the upper bound of the number of elements resulting from the split.
 *
 * The type that will be stored in output is struct aws_byte_cursor (you'll need
 * this for the item size param).
 *
 * It is the user's responsibility to make sure the input buffer stays in memory
 * long enough to use the results.
 */
AWS_COMMON_API
int aws_byte_cursor_split_on_char(
    const struct aws_byte_cursor *AWS_RESTRICT input_str,
    char split_on,
    struct aws_array_list *AWS_RESTRICT output);

/**
 * No copies, no buffer allocations. Fills in output with a list of aws_byte_cursor instances where buffer is
 * an offset into the input_str and len is the length of that string in the original buffer. N is the max number of
 * splits, if this value is zero, it will add all splits to the output.
 *
 * Edge case rules are as follows:
 * if the input begins with split_on, an empty cursor will be the first entry in output
 * if the input has two adjacent split_on tokens, an empty cursor will be inserted into the output.
 * if the input ends with split_on, an empty cursor will be appended to the output.
 *
 * It is the user's responsibility to properly initialize output. Recommended number of preallocated elements from
 * output is your most likely guess for the upper bound of the number of elements resulting from the split.
 *
 * If the output array is not large enough, input_str will be updated to point to the first character after the last
 * processed split_on instance.
 *
 * The type that will be stored in output is struct aws_byte_cursor (you'll need this for the item size param).
 *
 * It is the user's responsibility to make sure the input buffer stays in memory long enough to use the results.
 */
AWS_COMMON_API
int aws_byte_cursor_split_on_char_n(
    const struct aws_byte_cursor *AWS_RESTRICT input_str,
    char split_on,
    size_t n,
    struct aws_array_list *AWS_RESTRICT output);

/**
 * Copies from to to. If to is too small, AWS_ERROR_DEST_COPY_TOO_SMALL will be
 * returned. dest->len will contain the amount of data actually copied to dest.
 *
 * from and to may be the same buffer, permitting copying a buffer into itself.
 */
AWS_COMMON_API
int aws_byte_buf_append(struct aws_byte_buf *to, const struct aws_byte_cursor *from);

/**
 * Concatenates a variable number of struct aws_byte_buf * into destination.
 * Number of args must be greater than 1. If dest is too small,
 * AWS_ERROR_DEST_COPY_TOO_SMALL will be returned. dest->len will contain the
 * amount of data actually copied to dest.
 */
AWS_COMMON_API
int aws_byte_buf_cat(struct aws_byte_buf *dest, size_t number_of_args, ...);

/**
 * Compares two aws_byte_cursor structures
 * Returns true if a has the same length as b and their buffers have the same bytes
 * (or both buffers are null). When both a and b are null the function returns true
 */
AWS_COMMON_API
bool aws_byte_cursor_eq(const struct aws_byte_cursor *a, const struct aws_byte_cursor *b);

/**
 * Compares an aws_byte_cursor against an aws_byte_buf
 * Returns true if a has the same length as b and their buffers have the same bytes
 * (or both buffers are null). When both a and b are null the function returns true
 */
AWS_COMMON_API
bool aws_byte_cursor_eq_byte_buf(const struct aws_byte_cursor *a, const struct aws_byte_buf *b);

AWS_EXTERN_C_END

/**
 * For creating a byte buffer from a null-terminated string literal.
 */
AWS_STATIC_IMPL struct aws_byte_buf aws_byte_buf_from_c_str(const char *c_str) {
    struct aws_byte_buf buf;
    buf.buffer = (uint8_t *)c_str;
    buf.len = strlen(c_str);
    buf.capacity = buf.len;
    buf.allocator = NULL;
    return buf;
}

AWS_STATIC_IMPL struct aws_byte_buf aws_byte_buf_from_array(const void *bytes, size_t len) {
    struct aws_byte_buf buf;
    buf.buffer = (uint8_t *)bytes;
    buf.len = len;
    buf.capacity = len;
    buf.allocator = NULL;
    return buf;
}

AWS_STATIC_IMPL struct aws_byte_buf aws_byte_buf_from_empty_array(const void *bytes, size_t capacity) {
    struct aws_byte_buf buf;
    buf.buffer = (uint8_t *)bytes;
    buf.len = 0;
    buf.capacity = capacity;
    buf.allocator = NULL;
    return buf;
}

AWS_STATIC_IMPL struct aws_byte_cursor aws_byte_cursor_from_buf(const struct aws_byte_buf *buf) {
    struct aws_byte_cursor cur;
    cur.ptr = buf->buffer;
    cur.len = buf->len;
    return cur;
}

AWS_STATIC_IMPL struct aws_byte_cursor aws_byte_cursor_from_c_str(const char *c_str) {
    struct aws_byte_cursor cursor;
    cursor.ptr = (uint8_t *)c_str;
    cursor.len = strlen(c_str);
    return cursor;
}

AWS_STATIC_IMPL struct aws_byte_cursor aws_byte_cursor_from_array(const void *bytes, size_t len) {
    struct aws_byte_cursor cur;
    cur.ptr = (uint8_t *)bytes;
    cur.len = len;
    return cur;
}

AWS_STATIC_IMPL int aws_byte_buf_init_copy(
    struct aws_byte_buf *dest,
    struct aws_allocator *allocator,
    const struct aws_byte_buf *src) {

    struct aws_byte_cursor src_cur = aws_byte_cursor_from_buf(src);
    return aws_byte_buf_init_copy_from_cursor(dest, allocator, src_cur);
}

/**
 * If index >= bound, bound > (SIZE_MAX / 2), or index > (SIZE_MAX / 2), returns
 * 0. Otherwise, returns UINTPTR_MAX.  This function is designed to return the correct
 * value even under CPU speculation conditions, and is intended to be used for
 * SPECTRE mitigation purposes.
 */
AWS_STATIC_IMPL size_t aws_nospec_mask(size_t index, size_t bound) {
    /*
     * SPECTRE mitigation - we compute a mask that will be zero if len < 0
     * or len >= buf->len, and all-ones otherwise, and AND it into the index.
     * It is critical that we avoid any branches in this logic.
     */

    /*
     * Hide the index value from the optimizer. This helps ensure that all this
     * logic doesn't get eliminated.
     */
#if defined(__GNUC__) || defined(__clang__)
    __asm__ __volatile__("" : "+r"(index));
#endif
#if defined(_MSVC_LANG)
    /*
     * MSVC doesn't have a good way for us to blind the optimizer, and doesn't
     * even have inline asm on x64. Some experimentation indicates that this
     * hack seems to confuse it sufficiently for our needs.
     */
    *((volatile uint8_t *)&index) += 0;
#endif

    /*
     * If len > (SIZE_MAX / 2), then we can end up with len - buf->len being
     * positive simply because the sign bit got inverted away. So we also check
     * that the sign bit isn't set from the start.
     *
     * We also check that bound <= (SIZE_MAX / 2) to catch cases where the
     * buffer is _already_ out of bounds.
     */
    size_t negative_mask = index | bound;
    size_t toobig_mask = bound - index - (uintptr_t)1;
    size_t combined_mask = negative_mask | toobig_mask;

    /*
     * combined_mask needs to have its sign bit OFF for us to be in range.
     * We'd like to expand this to a mask we can AND into our index, so flip
     * that bit (and everything else), shift it over so it's the only bit in the
     * ones position, and multiply across the entire register.
     *
     * First, extract the (inverse) top bit and move it to the lowest bit.
     * Because there's no standard SIZE_BIT in C99, we'll divide by a mask with
     * just the top bit set instead.
     */

    combined_mask = (~combined_mask) / (SIZE_MAX - (SIZE_MAX >> 1));

    /*
     * Now multiply it to replicate it across all bits.
     *
     * Note that GCC is smart enough to optimize the divide-and-multiply into
     * an arithmetic right shift operation on x86.
     */
    combined_mask = combined_mask * UINTPTR_MAX;

    return combined_mask;
}

/**
 * Tests if the given aws_byte_cursor has at least len bytes remaining. If so,
 * *buf is advanced by len bytes (incrementing ->ptr and decrementing ->len),
 * and an aws_byte_cursor referring to the first len bytes of the original *buf
 * is returned. Otherwise, an aws_byte_cursor with ->ptr = NULL, ->len = 0 is
 * returned.
 *
 * Note that if len is above (SIZE_MAX / 2), this function will also treat it as
 * a buffer overflow, and return NULL without changing *buf.
 */
AWS_STATIC_IMPL struct aws_byte_cursor aws_byte_cursor_advance(struct aws_byte_cursor *cursor, size_t len) {
    struct aws_byte_cursor rv;
    if (cursor->len > (SIZE_MAX >> 1) || len > (SIZE_MAX >> 1) || len > cursor->len) {
        rv.ptr = NULL;
        rv.len = 0;
    } else {
        rv.ptr = cursor->ptr;
        rv.len = len;

        cursor->ptr += len;
        cursor->len -= len;
    }

    return rv;
}

/**
 * Behaves identically to aws_byte_cursor_advance, but avoids speculative
 * execution potentially reading out-of-bounds pointers (by returning an
 * empty ptr in such speculated paths).
 *
 * This should generally be done when using an untrusted or
 * data-dependent value for 'len', to avoid speculating into a path where
 * cursor->ptr points outside the true ptr length.
 */

AWS_STATIC_IMPL struct aws_byte_cursor aws_byte_cursor_advance_nospec(struct aws_byte_cursor *cursor, size_t len) {
    struct aws_byte_cursor rv;

    if (len <= cursor->len && len <= (SIZE_MAX >> 1) && cursor->len <= (SIZE_MAX >> 1)) {
        /*
         * If we're speculating past a failed bounds check, null out the pointer. This ensures
         * that we don't try to read past the end of the buffer and leak information about other
         * memory through timing side-channels.
         */
        uintptr_t mask = aws_nospec_mask(len, cursor->len + 1);

        /* Make sure we don't speculate-underflow len either */
        len = len & mask;
        cursor->ptr = (uint8_t *)((uintptr_t)cursor->ptr & mask);
        /* Make sure subsequent nospec accesses don't advance ptr past NULL */
        cursor->len = cursor->len & mask;

        rv.ptr = cursor->ptr;
        /* Make sure anything acting upon the returned cursor _also_ doesn't advance past NULL */
        rv.len = len & mask;

        cursor->ptr += len;
        cursor->len -= len;
    } else {
        rv.ptr = NULL;
        rv.len = 0;
    }

    return rv;
}

/**
 * Reads specified length of data from byte cursor and copies it to the
 * destination array.
 *
 * On success, returns true and updates the cursor pointer/length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_cursor_read(
    struct aws_byte_cursor *AWS_RESTRICT cur,
    void *AWS_RESTRICT dest,
    size_t len) {
    struct aws_byte_cursor slice = aws_byte_cursor_advance_nospec(cur, len);

    if (slice.ptr) {
        memcpy(dest, slice.ptr, len);
        return true;
    }
    return false;
}

/**
 * Reads as many bytes from cursor as size of buffer, and copies them to buffer.
 *
 * On success, returns true and updates the cursor pointer/length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_cursor_read_and_fill_buffer(
    struct aws_byte_cursor *AWS_RESTRICT cur,
    struct aws_byte_buf *AWS_RESTRICT dest) {
    if (aws_byte_cursor_read(cur, dest->buffer, dest->capacity)) {
        dest->len = dest->capacity;
        return true;
    }
    return false;
}

/**
 * Reads a single byte from cursor, placing it in *var.
 *
 * On success, returns true and updates the cursor pointer/length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_cursor_read_u8(struct aws_byte_cursor *AWS_RESTRICT cur, uint8_t *AWS_RESTRICT var) {
    return aws_byte_cursor_read(cur, var, 1);
}

/**
 * Reads a 16-bit value in network byte order from cur, and places it in host
 * byte order into var.
 *
 * On success, returns true and updates the cursor pointer/length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_cursor_read_be16(struct aws_byte_cursor *cur, uint16_t *var) {
    bool rv = aws_byte_cursor_read(cur, var, 2);

    if (AWS_LIKELY(rv)) {
        *var = aws_ntoh16(*var);
    }

    return rv;
}

/**
 * Reads a 32-bit value in network byte order from cur, and places it in host
 * byte order into var.
 *
 * On success, returns true and updates the cursor pointer/length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_cursor_read_be32(struct aws_byte_cursor *cur, uint32_t *var) {
    bool rv = aws_byte_cursor_read(cur, var, 4);

    if (AWS_LIKELY(rv)) {
        *var = aws_ntoh32(*var);
    }

    return rv;
}

/**
 * Reads a 64-bit value in network byte order from cur, and places it in host
 * byte order into var.
 *
 * On success, returns true and updates the cursor pointer/length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_cursor_read_be64(struct aws_byte_cursor *cur, uint64_t *var) {
    bool rv = aws_byte_cursor_read(cur, var, sizeof(*var));

    if (AWS_LIKELY(rv)) {
        *var = aws_ntoh64(*var);
    }

    return rv;
}

/**
 * Appends a sub-buffer to the specified buffer.
 *
 * If the buffer has at least `len' bytes remaining (buffer->capacity - buffer->len >= len),
 * then buffer->len is incremented by len, and an aws_byte_buf is assigned to *output corresponding
 * to the last len bytes of the input buffer. The aws_byte_buf at *output will have a null
 * allocator, a zero initial length, and a capacity of 'len'. The function then returns true.
 *
 * If there is insufficient space, then this function nulls all fields in *output and returns
 * false.
 */
AWS_STATIC_IMPL bool aws_byte_buf_advance(
    struct aws_byte_buf *AWS_RESTRICT buffer,
    struct aws_byte_buf *AWS_RESTRICT output,
    size_t len) {
    if (buffer->capacity - buffer->len >= len) {
        *output = aws_byte_buf_from_array(buffer->buffer + buffer->len, len);
        buffer->len += len;
        output->len = 0;
        return true;
    } else {
        memset(output, 0, sizeof(*output));
        return false;
    }
}

/**
 * Write specified number of bytes from array to byte buffer.
 *
 * On success, returns true and updates the buffer length accordingly.
 * If there is insufficient space in the buffer, returns false, leaving the
 * buffer unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_buf_write(
    struct aws_byte_buf *AWS_RESTRICT buf,
    const uint8_t *AWS_RESTRICT src,
    size_t len) {

    if (buf->len > (SIZE_MAX >> 1) || len > (SIZE_MAX >> 1) || buf->len + len > buf->capacity) {
        return false;
    }

    memcpy(buf->buffer + buf->len, src, len);
    buf->len += len;

    return true;
}

/**
 * Copies all bytes from buffer to buffer.
 *
 * On success, returns true and updates the buffer /length accordingly.
 * If there is insufficient space in the buffer, returns false, leaving the
 * buffer unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_buf_write_from_whole_buffer(
    struct aws_byte_buf *AWS_RESTRICT buf,
    struct aws_byte_buf src) {
    return aws_byte_buf_write(buf, src.buffer, src.len);
}

/**
 * Copies all bytes from buffer to buffer.
 *
 * On success, returns true and updates the buffer /length accordingly.
 * If there is insufficient space in the buffer, returns false, leaving the
 * buffer unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_buf_write_from_whole_cursor(
    struct aws_byte_buf *AWS_RESTRICT buf,
    struct aws_byte_cursor src) {
    return aws_byte_buf_write(buf, src.ptr, src.len);
}

/**
 * Copies one byte to buffer.
 *
 * On success, returns true and updates the cursor /length
 accordingly.

 * If there is insufficient space in the cursor, returns false, leaving the
 cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_buf_write_u8(struct aws_byte_buf *AWS_RESTRICT buf, uint8_t c) {
    return aws_byte_buf_write(buf, &c, 1);
}

/**
 * Writes a 16-bit integer in network byte order (big endian) to buffer.
 *
 * On success, returns true and updates the cursor /length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_buf_write_be16(struct aws_byte_buf *buf, uint16_t x) {
    x = aws_hton16(x);
    return aws_byte_buf_write(buf, (uint8_t *)&x, 2);
}

/**
 * Writes a 32-bit integer in network byte order (big endian) to buffer.
 *
 * On success, returns true and updates the cursor /length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_buf_write_be32(struct aws_byte_buf *buf, uint32_t x) {
    x = aws_hton32(x);
    return aws_byte_buf_write(buf, (uint8_t *)&x, 4);
}

/**
 * Writes a 64-bit integer in network byte order (big endian) to buffer.
 *
 * On success, returns true and updates the cursor /length accordingly.
 * If there is insufficient space in the cursor, returns false, leaving the
 * cursor unchanged.
 */
AWS_STATIC_IMPL bool aws_byte_buf_write_be64(struct aws_byte_buf *buf, uint64_t x) {
    x = aws_hton64(x);
    return aws_byte_buf_write(buf, (uint8_t *)&x, 8);
}

#endif /* AWS_COMMON_BYTE_BUF_H */
