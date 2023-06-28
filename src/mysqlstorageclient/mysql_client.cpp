#include <glog/logging.h>
#include <cassert>
#include "src/common/string_util.h"
#include "src/mysqlstorageclient/mysql_client.h"

namespace curve {
namespace mysqlstorage {

using ::curve::mysqlstorage::MySQLError;

int MysqlClientImp::Init(MysqlConf conf, int timeout, int retryTiems) {
    try {
        sql::Driver *driver = sql::mysql::get_driver_instance();
        conn_ = driver->connect(conf.host_, conf.user_, conf.passwd_);
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
        std::string sql = "CREATE TABLE IF NOT EXISTS " + tableName + "("
               "storekey VARCHAR(255) NOT NULL,"
               "storevalue LONGBLOB NOT NULL,"
               "revision BIGINT NOT NULL"
               ")";
        stmt_->execute(sql);
        LOG(INFO) << "MysqlClientImp CreateTable success: " << tableName;
        sql = "CREATE TABLE IF NOT EXISTS leader_election ("
	"elect_key varchar(255) NOT NULL ,"
	"version bigint NOT NULL DEFAULT 0 ,"
	"tick_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,"
	"leader_id bigint NOT NULL DEFAULT 0 ,"
	"create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ,"
	"update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP ,"
	"PRIMARY KEY (elect_key),"
	"KEY idx_version (version)"
")";
        stmt_->execute(sql);
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp CreateTable failed, error: " << e.what();
        return -1;
    }
    return 0;
}

void MysqlClientImp::CloseClient() {
    if (stmt_ != nullptr) {
        delete stmt_;
        stmt_ = nullptr;
    }
    if (conn_ != nullptr) {
        delete conn_;
        conn_ = nullptr;
    }
}

int MysqlClientImp::Put(const std::string &key, const std::string &value) {
    try {
        std::string sql = "insert into curvebs_kv(storekey, storevalue, revision) values(?,?,?);";
        sql::PreparedStatement *pstmt = conn_->prepareStatement(sql);
        pstmt->setString(1, key);
        std::stringbuf buf(value, std::ios_base::in);
        std::istream stream(&buf);
        pstmt->setBlob(2, &stream);
        int64_t *curRevision=new int64_t(0);
        GetCurrentRevision(curRevision);
        pstmt->setInt64(3, *curRevision + 1);
        pstmt->executeUpdate();
        delete pstmt;
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
         sql::PreparedStatement *pstmt = conn_->prepareStatement(sql);
        pstmt->setString(1, key);
        std::stringbuf buf(value, std::ios_base::in);
        std::istream stream(&buf);
        pstmt->setBlob(2, &stream);
        int64_t curRevision;
        GetCurrentRevision(&curRevision);
        pstmt->setInt64(3, curRevision + 1);
        pstmt->executeUpdate();
        *revision = curRevision + 1;
        delete pstmt;
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
        sql::PreparedStatement *pstmt = conn_->prepareStatement(sql);
        pstmt->setString(1, key);
        std::unique_ptr<sql::ResultSet> res(pstmt->executeQuery());
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
            //存在，比较
            std::string storevalue;
            std::unique_ptr<std::istream> is(res->getBlob("storevalue"));
            is->seekg(0, is->end);
            int len = is->tellg();
            is->seekg(0, is->beg);
            LOG(INFO)<<len;
            if(len > 0)
            {
                storevalue.resize(len);
                is->read(&storevalue[0], len);  
            }
            else
            {
                storevalue.clear();
            }
            if (storevalue != preV) {
                //不满足条件，回滚
                conn_->rollback();
                delete res;
                return -1;
            }else{
                //满足条件，更新
                std::string sql2 = "update curvebs_kv set storevalue=? ,revision=? where storekey=?;";
                sql::PreparedStatement *pstmt = conn_->prepareStatement(sql2);
                pstmt->setString(3, key);
                std::stringbuf buf(target, std::ios_base::in);
                std::istream stream(&buf);
                pstmt->setBlob(1, &stream);
                int64_t curRevision;
                GetCurrentRevision(&curRevision);
                LOG(INFO)<<"curRevision:"<<curRevision;
                pstmt->setInt64(2, curRevision + 1);
                pstmt->executeUpdate();

                conn_->commit();
                delete res;
                return 0;
            }
        } else {
            LOG(INFO) << "MysqlClientImp CompareAndSwap failed, key: " << key << " not exist, insert it";
            //不存在，插入
            int errCode=Put(key, target);
            LOG(INFO)<<"put ret:"<<errCode;
            conn_->commit();
            delete res;
            return errCode;
        }
        delete res;
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

int MysqlClientImp::CampaignLeader( const std::string &pfx, const std::string &leaderName,
    uint32_t sessionInterSec, uint32_t electionTimeoutMs,
    uint64_t *leaderOid) {
    try {
        std::string sql = "insert into leader_election(elect_key, leader_id, create_time, update_time) values(?,?,now(),now());";
        sql::PreparedStatement *pstmt = conn_->prepareStatement(sql);
        pstmt->setString(1, pfx);
        pstmt->setString(2, leaderName);
        pstmt->executeUpdate();
        delete pstmt;
        sql = "select last_insert_id() as leader_id;";
        sql::ResultSet *res = stmt_->executeQuery(sql);
        if (res->next()) {
            *leaderOid = res->getInt64("leader_id");
        }else{
            LOG(ERROR)<<"no leader_id";
            *leaderOid = 0;
        }
        delete res;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp CampaignLeader failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::LeaderObserve(uint64_t leaderOid, const std::string &leaderName){
    try {
        std::string sql = "select leader_id from leader_election where leader_id=" + std::to_string(leaderOid) + " and leader_id=(select max(leader_id) from leader_election where elect_key='" + leaderName + "');";
        sql::ResultSet *res = stmt_->executeQuery(sql);
        if (res->next()) {
            LOG(INFO)<<"leader_id:"<<res->getInt64("leader_id");
        }else{
            LOG(ERROR)<<"no leader_id";
            return -1;
        }
        delete res;
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp LeaderObserve failed, error: " << e.what();
        return -1;
    }
    return 0;
}

int MysqlClientImp::LeaderResign(uint64_t leaderOid, uint64_t timeoutMs){
    try {
        std::string sql = "update leader_election set leader_id=0, update_time=now() where leader_id=" + std::to_string(leaderOid) + ";";
        stmt_->execute(sql);
    } catch (sql::SQLException &e) {
        LOG(ERROR) << "MysqlClientImp LeaderResign failed, error: " << e.what();
        return -1;
    }
    return 0;
}

}  // namespace mysqlstorage
}  // namespace curve