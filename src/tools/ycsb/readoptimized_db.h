#ifndef YCSB_C_READOPTIMIZED_DB_H
#define YCSB_C_READOPTIMIZED_DB_H

#include "core/db.h"

#include <stdlib.h>
#include <errno.h>
#include <string>

#include "include/types.h"
#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include "core/utils.h"
#include "core/properties.h"

#include "cls/lsm/cls_lsm_types.h"
#include "cls/lsm/cls_lsm_client.h"
#include "cls/lsm/cls_lsm_ops.h"
#include "cls/lsm/cls_lsm_read_optimized.h"

using namespace librados;

namespace ycsbc {
    class ReadOptimizedDB : public DB{
    public :
        ReadOptimizedDB(utils::Properties& props);
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

        int Compact(const std::string &table);

        ~ReadOptimizedDB();
    
    private:
        std::string pool_name;
        Rados cluster;
        IoCtx ioctx;
        ClsReadOptimizedClient dbClient;
        unsigned noResult;
    };
}

#endif