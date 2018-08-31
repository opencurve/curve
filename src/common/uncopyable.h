/*
 * Project: curve
 * Created Date: 18-8-31
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_COMMON_UNCOPYABLE_H
#define CURVE_COMMON_UNCOPYABLE_H

namespace curve {
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
}  // namespace curve

#endif  // CURVE_COMMON_UNCOPYABLE_H
