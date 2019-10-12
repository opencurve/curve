/*
 * Project: curve
 * Created Date: Friday October 18th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#ifndef TEST_INTEGRATION_COMMON_CONFIG_GENERATOR_H_
#define TEST_INTEGRATION_COMMON_CONFIG_GENERATOR_H_

#include <string>

#include "src/common/configuration.h"

#define DEFAULT_CHUNKSERVER_CONF "conf/chunkserver.conf.example"

namespace curve {

using curve::common::Configuration;

// 各模块继承该接口，实现自己的初始化配置函数
class ConfigGenerator {
 public:
    ConfigGenerator() = default;
    virtual ~ConfigGenerator() = default;
    // 设置配置项
    virtual void SetKV(const std::string& key, const std::string& value) {
        config_.SetValue(key, value);
    }
    // 用于生成配置文件
    virtual bool Generate() {
        return config_.SaveConfig();
    }
    // 删除配置文件
    virtual int Remove() {
        return ::remove(configPath_.c_str());
    }
 protected:
    // 配置文件路径
    std::string configPath_;
    // 配置器
    Configuration config_;
};

// chunkserver test config
class CSTConfigGenerator : public ConfigGenerator {
 public:
    CSTConfigGenerator() {}
    ~CSTConfigGenerator() {}
    bool Init(const std::string& port) {
        // 加载配置文件模板
        config_.SetConfigPath(DEFAULT_CHUNKSERVER_CONF);
        if (!config_.LoadConfig()) {
            return false;
        }
        SetKV("global.port", port);

        std::string stor_uri = "local://./" + port + "/";
        std::string meta_uri = stor_uri + "chunkserver.dat";
        std::string data_uri = stor_uri + "copysets";
        std::string recycler_uri = stor_uri + "recycler";
        SetKV("chunkserver.stor_uri", stor_uri);
        SetKV("chunkserver.meta_uri", meta_uri);
        SetKV("copyset.chunk_data_uri", data_uri);
        SetKV("copyset.raft_log_uri", data_uri);
        SetKV("copyset.raft_meta_uri", data_uri);
        SetKV("copyset.raft_snapshot_uri", data_uri);
        SetKV("copyset.recycler_uri", recycler_uri);

        std::string cfpoolDir = "./" + port + "/chunkfilepool/";
        std::string cfpoolMetaPath = "./" + port + "/chunkfilepool.meta";
        SetKV("chunkfilepool.chunk_file_pool_dir", cfpoolDir);
        SetKV("chunkfilepool.meta_path", cfpoolMetaPath);

        SetKV("chunkfilepool.enable_get_chunk_from_pool", "false");

        configPath_ = "./" + port + "/chunkserver.conf";
        config_.SetConfigPath(configPath_);
        return true;
    }
};

}  // namespace curve

#endif  // TEST_INTEGRATION_COMMON_CONFIG_GENERATOR_H_
