/*
 * Project: curve
 * File Created: Tuesday, 30th July 2019 9:18:33 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */


#include <memory>
#include <atomic>
#include <thread>       //  NOLINT
#include <iostream>

class ob {
 public:
    explicit ob(int j) {
        i = j;
        std::cout << "construct!" << std::endl;
    }

    ~ob() {
        std::cout << "destruct!" << std::endl;
    }

    void Print() {
        std::cout << i << std::endl;
    }

 private:
    int i;
};

int main() {
    std::atomic<uint64_t> t(2);
    std::shared_ptr<ob> s1(new ob(1));

    auto f1 = [&]() {
        while (1) {
            auto newob = std::shared_ptr<ob>(new ob(t.fetch_add(1)));
            std::atomic_exchange(&s1, newob);
        }
    };

    auto f2 = [&]() {
        while (1) {
            auto oldob = std::atomic_load(&s1);
            oldob.get()->Print();
        }
    };

    std::thread t1(f1);
    std::thread t2(f1);
    std::thread t3(f2);
    std::thread t4(f2);
    std::thread t5(f2);

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    return 0;
}
