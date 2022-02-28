#ifndef YCSB_C_CEPHLSM_DB_H
#define YCSB_C_CEPHLSM_DB_H

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

using namespace librados;

namespace ycsbc {
    class CephLsmDB : public DB{
    public :
        CephLsmDB(utils::Properties& props);
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

        ~CephLsmDB();
    
    private:
        std::string pool_name;
        Rados cluster;
        IoCtx ioctx;
        ClsLsmClient dbClient;
        unsigned noResult;
    };
}


#endif