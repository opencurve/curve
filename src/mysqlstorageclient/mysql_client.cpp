#include <glog/logging.h>
#include <cassert>
#include <chrono>
#include <thread>
#include "src/common/string_util.h"
#include "src/mysqlstorageclient/mysql_client.h"

namespace curve {
namespace mysqlstorage {

using ::curve::mysqlstorage::MySQLError;

int MysqlClientImp::Init(MysqlConf conf, int timeout, int retryTiems) {
    try {
        sql::Driver *driver = sql::mysql::get_driver_instance();
        conn_ = std::shared_ptr<sql::Connection>(driver->connect(conf.host_, conf.user_, conf.passwd_));
        is_connected_.store(true);
        conn_->setSchema(conf.db_);
        stmt_ = conn_->createStatement();
       
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp Init failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::CreateTable(const std::string &tableName) {
    try {
        std::string sql = "CREATE TABLE IF NOT EXISTS " + tableName +
                          "("
                          "storekey VARCHAR(255) NOT NULL,"
                          "storevalue LONGBLOB NOT NULL,"
                          "revision BIGINT NOT NULL"
                          ")";
        stmt_->execute(sql);
        LOG(INFO) << "MysqlClientImp CreateTable success: " << tableName;
        sql = "CREATE TABLE IF NOT EXISTS leader_election ("
              "elect_key varchar(255) NOT NULL ,"
              "leader_name varchar(255) NOT NULL ,"
              "create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ,"
              "update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP , "
              "leader_id bigint AUTO_INCREMENT,"
              "PRIMARY KEY (leader_id),"
              "UNIQUE KEY (elect_key)"
              ")";
        stmt_->execute(sql);
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp CreateTable failed, error: " << e.what();
        return -1;
    }
    return 0;
}

void MysqlClientImp::CloseClient() {
   //std::lock_guard<std::mutex> lock(conn_mutex_);
    //删除leader信息
    if(leaderOid_!=0)
    {
        std::string sql = "DELETE FROM leader_election WHERE leader_id = " + std::to_string(leaderOid_) + ";";
        stmt_->execute(sql);
    }
    is_connected_.store(false);
    if (stmt_ != nullptr) {
        delete stmt_;
    }
    
    LOG(INFO) << "MysqlClientImp CloseClient success";
}

int MysqlClientImp::Put(const std::string &key, const std::string &value) {
    try {
        std::string sql = "insert into curvebs_kv(storekey, storevalue, revision) values(?,?,?);";
        sql::PreparedStatement *prep_stmt_ = conn_->prepareStatement(sql);
        prep_stmt_->setString(1, key);
        std::stringbuf buf(value, std::ios_base::in);
        std::istream stream(&buf);
        prep_stmt_->setBlob(2, &stream);
        int64_t *curRevision=new int64_t(0);
        GetCurrentRevision(curRevision);
        prep_stmt_->setInt64(3, *curRevision + 1);
        prep_stmt_->executeUpdate();
        delete prep_stmt_;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp Put failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::PutRewithRevision(const std::string &key,
    const std::string &value, int64_t *revision) {
    try {
        std::string sql = "insert into curvebs_kv(storekey, storevalue, revision) values(?,?,?);";
         sql::PreparedStatement *prep_stmt_ = conn_->prepareStatement(sql);
        prep_stmt_->setString(1, key);
        std::stringbuf buf(value, std::ios_base::in);
        std::istream stream(&buf);
        prep_stmt_->setBlob(2, &stream);
        int64_t curRevision;
        GetCurrentRevision(&curRevision);
        prep_stmt_->setInt64(3, curRevision + 1);
        prep_stmt_->executeUpdate();
        *revision = curRevision + 1;
        delete prep_stmt_;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp PutRewithRevision failed, error: " << e.what();
        return -1;
    }
    return 0;
}
	
int MysqlClientImp::Get(const std::string &key, std::string *out) {
    MySQLError ret= MySQLError::MysqlOK;
    try {
        std::string sql = "select storevalue from curvebs_kv where storekey=? ORDER BY revision DESC LIMIT 1;";
        sql::PreparedStatement *prep_stmt_ = conn_->prepareStatement(sql);
        prep_stmt_->setString(1, key);
        std::unique_ptr<sql::ResultSet> res(prep_stmt_->executeQuery());
        while(res->next())
        {
            std::unique_ptr<std::istream> is(res->getBlob("storevalue"));
            is->seekg(0, is->end);
            int len = is->tellg();
            is->seekg(0, is->beg);
            if(len > 0)
            {
                out->resize(len);
                is->read(&(*out)[0], len);
            }
            else
            {
                out->clear();
            }
        }

    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp Get failed, error: " << e.what();
        ret = MySQLError::QueryError;
        return -1;
    }
    if(out->empty())
    {
        return -1;
    }
    return 0;
}

//put the same key will list together?
int MysqlClientImp::List(const std::string &startKey,
    const std::string &endKey, std::vector<std::string> *values) {
    try {
        std::string sql = "select storevalue from curvebs_kv where storekey>='" + startKey + "' and storekey<'" + endKey + "'";
        sql::ResultSet *res = stmt_->executeQuery(sql);
        while (res->next()) {
            std::string storevalue;
            std::unique_ptr<std::istream> is(res->getBlob("storevalue"));
			is->seekg(0, is->end);
			int len = is->tellg();
			is->seekg(0, is->beg);
			if(len > 0)
			{
				storevalue.resize(len);
				is->read(&storevalue[0], len);
			}
			else
			{
				storevalue.clear();
			}
            values->emplace_back(storevalue);
        }
        delete res;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp List failed, error: " << e.what();
        return -1;
    }
    return 0;
}


int MysqlClientImp::List(const std::string& startKey, const std::string& endKey,
    std::vector<std::pair<std::string, std::string> >* out) {
    try {
        std::string sql = "select storekey, storevalue from curvebs_kv where storekey>='" + startKey + "' and storekey<'" + endKey + "'";
        sql::ResultSet *res = stmt_->executeQuery(sql);
        while (res->next()) {
            std::pair<std::string, std::string> ret;
            ret.first = res->getString("storekey");
            std::string storevalue;
            std::unique_ptr<std::istream> is(res->getBlob("storevalue"));
			is->seekg(0, is->end);
			int len = is->tellg();
			is->seekg(0, is->beg);
			if(len > 0)
			{
				storevalue.resize(len);
				is->read(&storevalue[0], len);
			}
			else
			{
				storevalue.clear();
			}
            ret.second = storevalue;
            out->emplace_back(ret);
        }
        delete res;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp List failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::DropTable(const std::string &tableName) {
    try {
        std::string sql = "drop table if exists " + tableName;
        stmt_->execute(sql);
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp DropTable failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::Delete(const std::string &key) {
    try {
        std::string out;
        Get(key, &out);
        if(out.empty())
        {
            LOG(INFO)<<"key: "<<key<<" not exist";
            return -1;
        }
        Put("dummy", "dummy");
        std::string sql = "delete from curvebs_kv where storekey='" + key + "'";
        stmt_->execute(sql);     
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp Delete failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::DeleteRewithRevision(
    const std::string &key, int64_t *revision) {
    try {
        Put("dummy", "dummy");
        std::string sql = "delete from curvebs_kv where storekey='" + key + "'";
        stmt_->execute(sql);
        int64_t curRevision;
        GetCurrentRevision(&curRevision);
        LOG(INFO)<<"delete curRevision:"<<curRevision;
        *revision = curRevision;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp DeleteRewithRevision failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::CompareAndSwap(const std::string &key,
    const std::string &preV, const std::string &target) {
    //开始事务
    conn_->setAutoCommit(false);
    try {
        std::string sql = "select storevalue from curvebs_kv where storekey='" + key + "' order by revision desc limit 1;";
        sql::ResultSet *res = stmt_->executeQuery(sql);
        if (res->next()) {
            // 存在，比较
            std::string storevalue;
            std::unique_ptr<std::istream> is(res->getBlob("storevalue"));
            is->seekg(0, is->end);
            int len = is->tellg();
            is->seekg(0, is->beg);
            LOG(INFO) << len;
            if (len > 0) {
                storevalue.resize(len);
                is->read(&storevalue[0], len);
            } else {
                storevalue.clear();
            }
            if (storevalue != preV) {
                // 不满足条件，回滚
                conn_->rollback();
                delete res;
                return -1;
            } else {
                // 满足条件，更新
                std::string sql2 = "update curvebs_kv set storevalue=? "
                                   ",revision=? where storekey=?;";
                sql::PreparedStatement *prep_stmt_ =
                    conn_->prepareStatement(sql2);
                prep_stmt_->setString(3, key);
                std::stringbuf buf(target, std::ios_base::in);
                std::istream stream(&buf);
                prep_stmt_->setBlob(1, &stream);
                int64_t curRevision;
                GetCurrentRevision(&curRevision);
                LOG(INFO) << "curRevision:" << curRevision;
                prep_stmt_->setInt64(2, curRevision + 1);
                prep_stmt_->executeUpdate();

                conn_->commit();
                delete res;
                return 0;
            }
        } else {
            LOG(INFO) << "MysqlClientImp CompareAndSwap failed, key: " << key
                      << " not exist, insert it";
            // 不存在，插入
            int errCode = Put(key, target);
            LOG(INFO) << "put ret:" << errCode;
            conn_->commit();
            delete res;
            return errCode;
        }
    }catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp CompareAndSwap failed, error: " << e.what();
        conn_->rollback();
        return -1;
    }
    conn_->setAutoCommit(true);
}

int MysqlClientImp::ListWithLimitAndRevision(const std::string &startKey,
    const std::string &endKey, int64_t limit, int64_t revision,
    std::vector<std::string> *values, std::string *lastKey) {
    try {
        std::string sql = "select storevalue, storekey from curvebs_kv where storekey>='" + 
        startKey + "' and storekey<'" + endKey + "' and revision<=" + 
        std::to_string(revision) + " order by storekey limit " + std::to_string(limit);
        sql::ResultSet *res = stmt_->executeQuery(sql);
        while (res->next()) {
            std::string storevalue;
            std::unique_ptr<std::istream> is(res->getBlob("storevalue"));
            is->seekg(0, is->end);
            int len = is->tellg();
            is->seekg(0, is->beg);
            if(len > 0)
            {
                storevalue.resize(len);
                is->read(&storevalue[0], len);
            }
            else
            {
                storevalue.clear();
            }
            values->emplace_back(storevalue);
            LOG(INFO)<<"storekey:"<<res->getString("storekey");
            *lastKey = res->getString("storekey");
        }
        LOG(INFO)<<"out.size: "<<values->size();
        delete res;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp ListWithLimitAndRevision failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::GetCurrentRevision(int64_t *revision) {
    try {
        std::string sql = "select max(revision) as revision from curvebs_kv";
        sql::ResultSet *res = stmt_->executeQuery(sql);
        if (res->next()) {
            *revision = res->getInt64("revision");
        }else{
            LOG(INFO)<<"no revision";
            *revision = 0;
        }
        delete res;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp GetCurrentRevision failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::TxnN(const std::vector<Operation> &ops) {
    conn_->setAutoCommit(false);
    try {
        for (auto op : ops) { 
            std::string* key= new std::string(op.key,op.key+op.keyLen);
            std::string* value= new std::string(op.value,op.value+op.valueLen);
            if(op.opType == OpType::OpPut)
            {
                if(this->Put(*key, *value)==-1)
                {
                    LOG(ERROR) << "MysqlClientImp TxnN failed, error: " << "Put failed";
                    conn_->rollback();
                    return -1;
                }
            }
            else if(op.opType == OpType::OpDelete)
            {
                if(this->Delete(*key)==-1)
                {
                    LOG(ERROR) << "MysqlClientImp TxnN failed, error: " << "Delete failed";
                    conn_->rollback();
                    return -1;
                }
            }
            else
            {
                LOG(ERROR) << "MysqlClientImp TxnN do not support! ";
                conn_->rollback();
                return -1;
            }
        }
        conn_->commit();
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp TxnN failed, error: " << e.what();
        conn_->rollback();
        return -1;
    }
    conn_->setAutoCommit(true);
    LOG(INFO) << "MysqlClientImp TxnN success and setAutoCommit true";
    return 0;
}

int MysqlClientImp::CampaignLeader(const std::string &pfx,
                                   const std::string &leaderName,
                                   uint32_t sessionInterSec,
                                   uint32_t electionTimeoutMs,
                                   uint64_t *leaderOid) {
    try {
        LOG(INFO) << "MysqlClientImp CampaignLeader start";
        // 设置session超时时间
           
        // std::string sql =
        //     "SET SESSION wait_timeout = " + std::to_string(sessionInterSec);
        // sql::PreparedStatement *prep_stmt_ = conn_->prepareStatement(sql);
        // prep_stmt_->executeUpdate();
        sessionInterSec_=sessionInterSec;
        LOG(INFO) << "MysqlClientImp CampaignLeader set sessionInterSec success";


        //这里会  double free or corruption (!prev) 有问题
        // 开始选举  只有一个key会插入成功
        conn_->setAutoCommit(false);
        try {
            std::string sql = "INSERT INTO leader_election (elect_key, leader_name, "
                  "create_time, update_time) VALUES (?, ?, NOW(), NOW())";
            sql::PreparedStatement * prep_stmt_ = conn_->prepareStatement(sql);
            prep_stmt_->setString(1, pfx);
            prep_stmt_->setString(2, leaderName);
            prep_stmt_->executeUpdate();

            conn_->commit();
            LOG(INFO) << "CampaignLeader commit success";
            conn_->setAutoCommit(true);
           // 插入成功
        } catch (sql::SQLException &e) {
            conn_->rollback();
            conn_->setAutoCommit(true);

            if (e.getErrorCode() == 1062) {
                LOG(INFO) << "Duplicate entry for elect_key: " << pfx;
                return -2;  // 键冲突，插入失败
            } else {
                LOG(ERROR) << "Insert error: " << e.what();
                return -1;  // 其他插入错误
            }
        }

        std::string sql = "SELECT leader_id FROM leader_election WHERE elect_key = ? AND "
              "leader_name = ?";
        sql::PreparedStatement *prep_stmt_ = conn_->prepareStatement(sql);
        prep_stmt_->setString(1, pfx);
        prep_stmt_->setString(2, leaderName);
        sql::ResultSet *result = prep_stmt_->executeQuery();
        if (result->next()) {
            *leaderOid = result->getUInt64("leader_id");
            leaderOid_=*leaderOid;
        }else{
            LOG(ERROR)<<"no leader_id";
            *leaderOid = 0;
        }
        delete result;
        LOG(INFO) << " get leader_id success :" << *leaderOid;

        // leader，定时更新update_time mysql自己实现
        //更新update_time   每隔一半sessionInterSec更新一次
        sql = "CREATE EVENT IF NOT EXISTS update_leader_election "
              "ON SCHEDULE EVERY " +
              std::to_string(sessionInterSec) +
              " SECOND "
              "DO UPDATE leader_election SET update_time = NOW() "
              "WHERE leader_id = " +
              std::to_string(*leaderOid) + ";";
        stmt_ = conn_->createStatement();
        stmt_->executeUpdate(sql);

        LOG(INFO) << "MysqlClientImp CampaignLeader success";

    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp CampaignLeader failed, error: "
                   << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::LeaderObserve(uint64_t leaderOid, const std::string &leaderName){
    try {
        // 定时查询leader是否超时
        while (is_connected_.load() && LeaderKeyExist(leaderOid, 0)) {
            // 使用互斥锁保护对 conn_ 的访问
            //std::lock_guard<std::mutex> lock(conn_mutex_);
            // LOG(INFO)<<is_connected_.load();
            if (!is_connected_.load()) {
                LOG(INFO) << "LeaderObserve stop !";
                break;  // 已停止，退出子线程
            }
            LOG(INFO) << "leaderOid:  " << leaderOid << "  is being observed";
            std::string sql =
                "SELECT TIMESTAMPDIFF(MICROSECOND, update_time, now()) AS diff "
                "FROM leader_election WHERE leader_id = " +
                std::to_string(leaderOid) + ";";
            sql::ResultSet *res = stmt_->executeQuery(sql);
            if (res->next()) {
                LOG(INFO) << "leader_id:" << leaderOid;
                int64_t diff = res->getInt64("diff");
                LOG(INFO) << "diff:" << diff;
                if (diff > sessionInterSec_) {
                    LOG(ERROR) << "Leader timeout";
                    sql = "DELETE FROM leader_election WHERE leader_id = " +
                          std::to_string(leaderOid) + ";";
                    stmt_->execute(sql);
                    delete res;
                    return EtcdErrCode::EtcdObserverLeaderInternal;
                }
                std::this_thread::sleep_for(
                    std::chrono::seconds(sessionInterSec_ / 2));
            } else {
                LOG(ERROR) << "no such leader_id";
                delete res;
                return EtcdErrCode::EtcdObserverLeaderInternal;
            }
        }
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp LeaderObserve failed, error: "
                   << e.what();
        return EtcdErrCode::EtcdObserverLeaderInternal;
    }
}

int MysqlClientImp::LeaderResign(uint64_t leaderOid, uint64_t timeoutMs){
    //把leader_id对应的key删掉，让阻塞的client继续竞选
    try {
        std::string sql = "DELETE FROM leader_election WHERE leader_id = " + std::to_string(leaderOid) + ";";
        stmt_->execute(sql);
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp LeaderResign failed, error: " << e.what();
        return -1;
    }
    
    return EtcdErrCode::EtcdLeaderResiginSuccess;
}

bool  MysqlClientImp::LeaderKeyExist(uint64_t leaderOid, uint64_t timeoutMs){
    try {
        std::string sql = "select leader_id from leader_election where leader_id=" + std::to_string(leaderOid) + ";";
        sql::ResultSet *res = stmt_->executeQuery(sql);
        if (res->next()) {
            LOG(INFO)<<"leader_id:"<<res->getInt64("leader_id");
        }else{
            LOG(ERROR)<<"no leader_id";
            return false;
        }
        delete res;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp LeaderKeyExist failed, error: " << e.what();
        return false;
    }
    return true;
}


}  // namespace mysqlstorage
}  // namespace curve