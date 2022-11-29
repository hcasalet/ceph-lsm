#ifndef YCSB_C_CABIN_DB_H
#define YCSB_C_CABIN_DB_H

#include "core/db.h"

#include <stdlib.h>
#include <errno.h>
#include <string>

#include "include/types.h"
#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include "core/utils.h"
#include "core/properties.h"

#include "common/ceph_context.h"
#include "kv/CabinDBStore.h"


using namespace librados;

namespace ycsbc {
    class CabinDB : public DB{
    public :
        CabinDB(utils::Properties& props);
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key, const std::string &max_key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair> > &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Delete(const std::string &table, const std::string &key);

        ~CabinDB();
    
    private:
        std::unique_ptr<CabinDBStore> db;
        std::vector<std::string> vec_cf;

        void SetOptions(const char *dbfilename, utils::Properties &props);
        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);
    };
}

#endif