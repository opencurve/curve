/*
 * Project: curve
 * Created Date: Mon Aug 27 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_TOPOLOGY_SINGLETON_H_
#define CURVE_SRC_MDS_TOPOLOGY_SINGLETON_H_

#include <mutex> // NOLINT
#include <memory>

template <class T>
class Singleton {
 protected:
    Singleton() {}
 private:
    Singleton(const Singleton&) = default;
    Singleton& operator=(const Singleton&) = default;
    static std::shared_ptr<T> m_instance;
    static pthread_mutex_t mutex;
 public:
    virtual ~Singleton() {}
    static std::shared_ptr<T> GetInstance();
};


template <class T>
std::shared_ptr<T> Singleton<T>::GetInstance() {
    if (m_instance == nullptr) {
        pthread_mutex_lock(&mutex);
        if (m_instance == nullptr) {
            m_instance = std::shared_ptr<T>(new T());
        }
        pthread_mutex_unlock(&mutex);
    }
    return m_instance;
}


template <class T>
pthread_mutex_t Singleton<T>::mutex = PTHREAD_MUTEX_INITIALIZER;

template <class T>
std::shared_ptr<T> Singleton<T>::m_instance = nullptr;


#endif  // CURVE_SRC_MDS_TOPOLOGY_SINGLETON_H_
