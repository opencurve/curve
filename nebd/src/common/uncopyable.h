/*
 * Project: nebd
 * Created Date: 18-8-31
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef SRC_COMMON_UNCOPYABLE_H_
#define SRC_COMMON_UNCOPYABLE_H_

namespace nebd {
namespace common {

class Uncopyable {
 protected:
    Uncopyable() = default;
    virtual ~Uncopyable() = default;

 private:
    Uncopyable(const Uncopyable &) = delete;
    Uncopyable &operator=(const Uncopyable &) = delete;
};

}  // namespace common
}  // namespace nebd

#endif  // SRC_COMMON_UNCOPYABLE_H_
