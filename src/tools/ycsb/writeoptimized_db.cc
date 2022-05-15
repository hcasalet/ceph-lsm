#include "core/core_workload.h"
#include "writeoptimized_db.h"

using namespace std;

namespace ycsbc {

    WriteOptimizedDB::WriteOptimizedDB(utils::Properties& props) {

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

            for (uint32_t j = 0; j < field_count/fields_in_group; j++) {

                std::vector<std::string> cols_0_0;
                for (uint32_t k = 0; k < field_count; k++) {
                    cols_0_0.push_back(std::string("field").append(std::to_string(k)));
                }
                cols_0.push_back(cols_0_0);

            }

            col_map[i] = cols_0;
        }

        dbClient.InitClient(props["dbname"], 0, 10240000000000000, field_count, levels, col_map);
    }

    int WriteOptimizedDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) 
    {
        cls_lsm_entry return_entry;
        dbClient.cls_write_optimized_read(ioctx, table, strtoul(key.c_str(), nullptr, 10), fields, return_entry);
        return WriteOptimizedDB::kOK;
    }

    int WriteOptimizedDB::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len,
                        const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) 
    {
        cls_lsm_entry return_entry;
        dbClient.cls_write_optimized_read(ioctx, table, std::stoul(key.c_str(), nullptr, 10), fields, return_entry);
        return WriteOptimizedDB::kOK;
    }

    int WriteOptimizedDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        cls_lsm_entry entry;
        entry.key = strtoul(key.c_str(), nullptr, 10);
        
        for (auto value : values) {
            bufferlist bl;
            encode(value.second, bl);
            entry.value.insert(std::pair<std::string, bufferlist>(value.first, bl));
        }

        dbClient.cls_write_optimized_write(ioctx, table, entry);

        return WriteOptimizedDB::kOK;
    }

    int WriteOptimizedDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        return Insert(table, key, values);
    }

    int WriteOptimizedDB::Delete(const std::string &table, const std::string &key)
    {
        return 0;
    }

    int WriteOptimizedDB::Compact(const std::string &table)
    {
        dbClient.cls_write_optimized_compact(ioctx, table);
        return WriteOptimizedDB::kOK;
    }

    WriteOptimizedDB::~WriteOptimizedDB() {
        ioctx.close();
        destroy_one_pool_pp(pool_name, cluster);
    }

}