#ifndef INCLUDE_STORAGECLIENT_STORAGECLIENT_H_
#define INCLUDE_STORAGECLIENT_STORAGECLIENT_H_

#include <string>
#include <vector>
#include <utility>
#include "include/etcdclient/etcdclient.h"

class StorageClient { 
 public:
    StorageClient() {}
    virtual ~StorageClient() {}

    /**
     * @brief Put store key-value pairs
     *
     * @param[in] key
     * @param[in] value
     *
     * @return error code EtcdErrCode
     */
    virtual int Put(const std::string &key, const std::string &value) = 0;

    /**
     * @brief PutRewithRevision store key-value
     *
     * @param[in] key
     * @param[in] value
     * @param[out] revision Return version number
     *
     * @return error code EtcdErrCode
     */
    virtual int PutRewithRevision(const std::string &key,
        const std::string &value, int64_t *revision) = 0;

    /**
     * @brief Get Get the value of the specified key
     *
     * @param[in] key
     * @param[out] value
     *
     * @return error code
     */
    virtual int Get(const std::string &key, std::string *out) = 0;

    /**
     * @brief List Get all the values ​​between [startKey, endKey)
     *
     * @param[in] startKey
     * @param[in] endKey
     * @param[out] values between [startKey, endKey)
     *
     * @return error code
     */
    virtual int List(const std::string &startKey, const std::string &endKey,
        std::vector<std::string> *values) = 0;

    /**
     * @brief List all the key and values between [startKey, endKey)
     *
     * @param[in] startKey
     * @param[in] endKey
     * @param[out] out store key/value pairs that key is between [startKey, endKey)
     *
     * @return error code
     */
    virtual int List(const std::string& startKey, const std::string& endKey,
                     std::vector<std::pair<std::string, std::string>>* out) = 0;

    /**
     * @brief Delete the value of the specified key
     *
     * @param[in] key
     *
     * @return error code
     */
    virtual int Delete(const std::string &key) = 0;

    /**
     * @brief DeleteRewithRevision Delete the value of the specified key
     *
     * @param[in] key
     * @param[out] revision Version number returned
     *
     * @return error code
     */
    virtual int DeleteRewithRevision(
        const std::string &key, int64_t *revision) = 0;

    /*
    * @brief TxnN Operate transactions in the order of ops[0] ops[1] ..., currently 2 and 3 operations are supported //NOLINT
    *
    * @param[in] ops Operation set
    *
    * @return error code
    */
    virtual int TxnN(const std::vector<Operation> &ops) = 0;

    /**
     * @brief CompareAndSwap Transaction, to achieve CAS
     *
     * @param[in] key
     * @param[in] preV Value conditions to be fulfilled
     * @param[in] target New value
     *
     * @return error code
     */
    virtual int CompareAndSwap(const std::string &key, const std::string &preV,
        const std::string &target) = 0;
};

#endif