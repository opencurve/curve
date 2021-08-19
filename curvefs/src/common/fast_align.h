/*
Copyright 2015 Glen Joseph Fernandes
(glenjofe@gmail.com)
Distributed under the Boost Software License, Version 1.0.
(http://www.boost.org/LICENSE_1_0.txt)
*/

/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * Project: curve
 * File Created: Fri Jul 16 21:22:40 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_COMMON_FAST_ALIGN_H_
#define CURVEFS_SRC_COMMON_FAST_ALIGN_H_

#include <cstdint>

namespace curvefs {
namespace common {

namespace detail {

template <typename T, typename U>
struct not_pointer {
    typedef U type;
};

template <typename T, typename U>
struct not_pointer<T*, U> {};

}  // namespace detail

template <typename T>
constexpr inline typename detail::not_pointer<T, T>::type align_down(
    T value, std::size_t alignment) noexcept {
    return T(value & ~T(alignment - 1));
}

inline void* align_down(void* ptr, std::size_t alignment) noexcept {
    return (void*)(align_down((std::size_t)ptr, alignment));  // NOLINT
}

template <typename T>
constexpr inline typename detail::not_pointer<T, T>::type align_up(
    T value, std::size_t alignment) noexcept {
    return T((value + (T(alignment) - 1)) & ~T(alignment - 1));
}

inline void* align_up(void* ptr, std::size_t alignment) noexcept {
    return (void*)(align_up((std::size_t)ptr, alignment));  // NOLINT
}

template <typename T>
constexpr inline typename detail::not_pointer<T, bool>::type is_aligned(
    T value, std::size_t alignment) noexcept {
    return (value & (T(alignment) - 1)) == 0;
}

inline bool is_aligned(const void* ptr, std::size_t alignment) noexcept {
    return is_aligned((std::size_t)ptr, alignment);  // NOLINT
}

}  // namespace common
}  // namespace curvefs

#endif  // CURVEFS_SRC_COMMON_FAST_ALIGN_H_
