#include "core/core_workload.h"
#include "level_db.h"
#include "global/global_init.h"

using namespace std;

namespace ycsbc {

    LevelDB::LevelDB(utils::Properties& props) {
        std::cout << "now inside leveldb " << std::endl;
         map<string,string> defaults = {
            { "debug_rocksdb", "2" }
        };
        vector<const char*> args;
        args.push_back("leveldb");
        args.push_back("/tmp/test-leveldb");

        auto cct = global_init(
            &defaults, args,
            CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
            CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
        common_init_finish(g_ceph_context);
        std::string path = props.GetProperty("dbpath","/tmp/test-leveldb");
        std::cout << "Finished initializing, dbpath=" << path << std::endl;

        auto db_ptr =  new LevelDBStore(g_ceph_context, path);
        if (int r = db_ptr->create_and_open(std::cerr); r < 0) {
            cerr << "failed to open path " << path << ": "
                 << cpp_strerror(r) << std::endl;
            exit(1);
        }
        db.reset(db_ptr);
    }

    int LevelDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) 
    {
        bufferlist bl_res;
        db->get("test", key, &bl_res);
        decode(result, bl_res);
        return LevelDB::kOK;
    }

    int LevelDB::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len,
                        const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) 
    {
        std::map<string, bufferlist> bl_res;
        long lowk = atol(key.c_str());
        long highk = atol(max_key.c_str());
        std::set<std::string> keys;
        for (int i = 0; i < len && lowk+i <= highk; i++) {
            keys.insert(std::to_string(lowk+i));
        }
        db->get("test", keys, &bl_res);

        for (auto blr : bl_res) {
            std::vector<KVPair> kvpairs;
            decode(kvpairs, blr.second);
            result.push_back(kvpairs);
        }
        return LevelDB::kOK;
    }

    int LevelDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        bufferlist bl_val;
        encode(values, bl_val);

        KeyValueDB::Transaction tx = db->get_transaction();
        tx->set("test", key, bl_val);
        int ret = db->submit_transaction_sync(tx);

        return ret == LevelDB::kOK;
    }

    int LevelDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        return Insert(table, key, values);
    }

    int LevelDB::Delete(const std::string &table, const std::string &key)
    {
        KeyValueDB::Transaction tx = db->get_transaction();
        tx->rmkey("test", key);
        int ret = db->submit_transaction_sync(tx);
        return ret == LevelDB::kOK;
    }

    LevelDB::~LevelDB() {}
}