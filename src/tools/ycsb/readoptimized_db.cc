#include "core/core_workload.h"
#include "readoptimized_db.h"

using namespace std;

namespace ycsbc {

    ReadOptimizedDB::ReadOptimizedDB(utils::Properties& props) {

        std::string pool_name = props.GetProperty(CoreWorkload::POOLNAME_PROPERTY,CoreWorkload::POOLNAME_DEFAULT);
        create_one_pool_pp(pool_name, cluster);

        int s = cluster.ioctx_create(pool_name.c_str(), ioctx);
        if (s != 0) {
            cerr << "Cannot open ceph db " << pool_name << endl;
            exit(0);
        }

        int levels = 2;
        uint32_t field_count = stoi(props.GetProperty(CoreWorkload::FIELD_COUNT_PROPERTY,CoreWorkload::FIELD_COUNT_DEFAULT));
        std::map<int, std::vector<std::vector<std::string>>> col_map;

        uint32_t fields_in_group = field_count;
        for (int i = 0; i <= levels; i++) {
            std::vector<std::vector<std::string>> cols_0;

            if (i > 0) {
                fields_in_group = 1;
            }

            for (uint32_t j = 0; j < field_count/fields_in_group; j++) {

                std::vector<std::string> cols_0_0;

                for (uint32_t k = j * fields_in_group; k < (j+1) * fields_in_group; k++) {
                    cols_0_0.push_back(std::string("field").append(std::to_string(k)));
                }

                cols_0.push_back(cols_0_0);
            }

            col_map[i] = cols_0;
        }

        dbClient.InitClient(props["dbname"], 0, 10240000000000000, field_count, levels, col_map);
    }

    int ReadOptimizedDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) 
    {
        cls_lsm_entry return_entry;
        dbClient.cls_read_optimized_read(ioctx, table, strtoul(key.c_str(), nullptr, 10), fields, return_entry);
        return ReadOptimizedDB::kOK;
    }

    int ReadOptimizedDB::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len,
                        const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) 
    {
        cls_lsm_entry return_entry;
        dbClient.cls_read_optimized_read(ioctx, table, std::stoul(key.c_str(), nullptr, 10), fields, return_entry);
        return ReadOptimizedDB::kOK;
    }

    int ReadOptimizedDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        cls_lsm_entry entry;
        entry.key = strtoul(key.c_str(), nullptr, 10);
        
        for (auto value : values) {
            bufferlist bl;
            encode(value.second, bl);
            entry.value.insert(std::pair<std::string, bufferlist>(value.first, bl));
        }

        dbClient.cls_read_optimized_write(ioctx, table, entry);

        return ReadOptimizedDB::kOK;
    }

    int ReadOptimizedDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        return Insert(table, key, values);
    }

    int ReadOptimizedDB::Delete(const std::string &table, const std::string &key)
    {
        return 0;
    }

    int ReadOptimizedDB::Compact(const std::string &table)
    {
        dbClient.cls_read_optimized_compact(ioctx, table);
        return ReadOptimizedDB::kOK;
    }

    ReadOptimizedDB::~ReadOptimizedDB() {
        ioctx.close();
        destroy_one_pool_pp(pool_name, cluster);
    }

}