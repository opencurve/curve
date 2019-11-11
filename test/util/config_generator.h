/*
 * Project: curve
 * Created Date: Mon Nov 11 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#ifndef TEST_UTIL_CONFIG_GENERATOR_H_
#define TEST_UTIL_CONFIG_GENERATOR_H_

#include <string>
#include <vector>

#include "src/common/configuration.h"

namespace curve {

using curve::common::Configuration;

// 各模块继承该接口，实现自己的初始化配置函数
class ConfigGenerator {
 public:
    ConfigGenerator() = default;

    virtual ~ConfigGenerator() = default;

    virtual bool LoadTemplete(const std::string &defaultConfigPath) {
        config_.SetConfigPath(defaultConfigPath);
        if (!config_.LoadConfig()) {
            return false;
        }
        return true;
    }

    virtual void SetConfigPath(const std::string &configPath) {
        configPath_ = configPath;
    }

    // 设置配置项
    virtual void SetKV(const std::string& key, const std::string& value) {
        config_.SetValue(key, value);
    }

    /**
     * @brief 批量设置配置项
     *
     * @param options 配置项表，形如 "Ip=127.0.0.1"
     */
    virtual void SetConfigOptions(
        const std::vector<std::string> &options) {
        for (const std::string &op : options) {
            int delimiterPos = op.find("=");
            std::string key = op.substr(0, delimiterPos);
            std::string value = op.substr(delimiterPos + 1);
            SetKV(key, value);
        }
    }

    // 用于生成配置文件
    virtual bool Generate() {
        if (configPath_ != "") {
            config_.SetConfigPath(configPath_);
            return config_.SaveConfig();
        }
        return false;
    }

    virtual bool Generate(const std::string &newConfigPath) {
        configPath_ = newConfigPath;
        return Generate();
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

#define DEFAULT_MDS_CONF "conf/mds.conf"

struct MDSConfigGenerator : public ConfigGenerator {
    explicit MDSConfigGenerator(const std::string &configPath) {
        LoadTemplete(DEFAULT_MDS_CONF);
        SetConfigPath(configPath);
    }
};

#define DEFAULT_CHUNKSERVER_CONF "conf/chunkserver.conf.example"

struct CSConfigGenerator : public ConfigGenerator {
    explicit CSConfigGenerator(const std::string &configPath) {
        LoadTemplete(DEFAULT_CHUNKSERVER_CONF);
        SetConfigPath(configPath);
    }
};

#define DEFAULT_CLIENT_CONF "conf/client.conf"

struct ClientConfigGenerator : public ConfigGenerator {
    explicit ClientConfigGenerator(const std::string &configPath) {
        LoadTemplete(DEFAULT_CLIENT_CONF);
        SetConfigPath(configPath);
    }
};

#define DEFAULT_CS_CLIENT_CONF "conf/cs_client.conf"

struct CSClientConfigGenerator : public ConfigGenerator {
    explicit CSClientConfigGenerator(const std::string &configPath) {
        LoadTemplete(DEFAULT_CS_CLIENT_CONF);
        SetConfigPath(configPath);
    }
};

#define DEFAULT_SNAP_CLIENT_CONF "conf/snap_client.conf"

struct SnapClientConfigGenerator : public ConfigGenerator {
    explicit SnapClientConfigGenerator(const std::string &configPath) {
        LoadTemplete(DEFAULT_SNAP_CLIENT_CONF);
        SetConfigPath(configPath);
    }
};

#define DEFAULT_S3_CONF "conf/s3.conf"

struct S3ConfigGenerator : public ConfigGenerator {
    explicit S3ConfigGenerator(const std::string &configPath) {
        LoadTemplete(DEFAULT_S3_CONF);
        SetConfigPath(configPath);
    }
};

#define DEFAULT_SCS_CONF "conf/snapshot_clone_server.conf"

struct SCSConfigGenerator : public ConfigGenerator {
    explicit SCSConfigGenerator(const std::string &configPath) {
        LoadTemplete(DEFAULT_SCS_CONF);
        SetConfigPath(configPath);
    }
};

}  // namespace curve

#endif  // TEST_UTIL_CONFIG_GENERATOR_H_
