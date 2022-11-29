#include "core/core_workload.h"
#include "cabin_db.h"
#include "global/global_init.h"

using namespace std;

namespace ycsbc {

    CabinDB::CabinDB(utils::Properties& props) {
        std::cout << "now inside cabindb " << std::endl;
         map<string,string> defaults = {
            { "debug_rocksdb", "2" }
        };
        vector<const char*> args;
        args.push_back("cabindb");
        args.push_back("/tmp/test-cabindb");

        auto cct = global_init(
            &defaults, args,
            CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
            CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
        common_init_finish(g_ceph_context);
        std::string path = props.GetProperty("dbpath","/tmp/test-cabindb");
        std::cout << "Finished initializing, dbpath=" << path << std::endl;

        const bool createdb = utils::StrToBool(props.GetProperty("createdb","false"));
        const int num_cf = stoi(props.GetProperty("columnfamilyshards", "0"));
        std::string cfs = "";
        for (int i=0; i < num_cf; i++) {
            cfs += "cf" + std::to_string(i+1) + "(1) ";
            vec_cf.push_back("cf"+std::to_string(i+1));
        }

        std::map<std::string,std::string> options = {};
		void *p = NULL;
        auto db_ptr = new CabinDBStore(g_ceph_context, path, options, p);
        if (int r = db_ptr->create_and_open(std::cerr, createdb, cfs); r < 0) {
            cerr << "failed to open path " << path << ": "
                 << cpp_strerror(r) << std::endl;
            exit(1);
        }
        db.reset(db_ptr);
    }

    int CabinDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) 
    {
        bufferlist bl_res;
        db->get("test", key, &bl_res);
        decode(result, bl_res);
        return CabinDB::kOK;
    }

    int CabinDB::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len,
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
        return CabinDB::kOK;
    }

    int CabinDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        bufferlist bl_val;
        encode(values, bl_val);

        KeyValueDB::Transaction tx = db->get_transaction();
        tx->set("row-wise", key, bl_val);
        int ret = db->submit_transaction_sync(tx);

        int cf_size = values.size();
        for (long i=0; i < cf_size; i++) {
            KVPair val = values[i];
            bufferlist bl_val_cf;
            encode(val, bl_val_cf);

            KeyValueDB::Transaction tx_cf = db->get_transaction();
            tx_cf->set("cf"+std::to_string(i+1), key, bl_val_cf);
            int ret_cf = db->submit_transaction_sync(tx_cf);
            if (ret_cf != CabinDB::kOK) {
                ret = -1;
            }
        }
        return ret == CabinDB::kOK;
    }

    int CabinDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        return Insert(table, key, values);
    }

    int CabinDB::Delete(const std::string &table, const std::string &key)
    {
        KeyValueDB::Transaction tx = db->get_transaction();
        tx->rmkey("row-wise", key);
        int ret = db->submit_transaction_sync(tx);

        for (int i=0; i < vec_cf.size(); i++) {
            KeyValueDB::Transaction tx_cf = db->get_transaction();
            tx_cf->rmkey(vec_cf[i], key);
            int ret_cf = db->submit_transaction_sync(tx_cf);

            if (ret_cf != CabinDB::kOK) {
                ret = -1;
            }
        }
        return ret == CabinDB::kOK;
    }

    CabinDB::~CabinDB() {}
}