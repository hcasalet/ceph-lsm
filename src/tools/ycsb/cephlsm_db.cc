#include "core/core_workload.h"
#include "cephlsm_db.h"

using namespace std;

namespace ycsbc {

    CephLsmDB::CephLsmDB(utils::Properties& props) {

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

        for (int i = 0; i <= levels; i++) {
            std::vector<std::vector<std::string>> cols_0;

            uint32_t fields_in_group = field_count/(i+1);
            if (fields_in_group == 0) {
                fields_in_group++;
            }

            for (int j = 0; j < i + 1; j++) {

                std::vector<std::string> cols_0_0;

                for (uint32_t k = j * fields_in_group; k < field_count; k++) {
                    cols_0_0.push_back(std::string("field").append(std::to_string(k)));
                }

                cols_0.push_back(cols_0_0);
            }

            col_map[i] = cols_0;
        }

        dbClient.InitClient(props["dbname"], props["dbname"], 0, 10240000000000000, 8, levels, col_map);
    }

    int CephLsmDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) 
    {
        cls_lsm_entry return_entry;
        dbClient.cls_lsm_read(ioctx, table, strtoul(key.c_str(), nullptr, 10), fields, return_entry);
        return CephLsmDB::kOK;
    }

    int CephLsmDB::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len,
                        const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) 
    {
        cls_lsm_entry return_entry;
        dbClient.cls_lsm_read(ioctx, table, std::stoul(key.c_str(), nullptr, 10), fields, return_entry);
        return CephLsmDB::kOK;
    }

    int CephLsmDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        cls_lsm_entry entry;
        entry.key = strtoul(key.c_str(), nullptr, 10);
        
        for (auto value : values) {
            bufferlist bl;
            encode(value.second, bl);
            entry.value.insert(std::pair<std::string, bufferlist>(value.first, bl));
        }

        dbClient.cls_lsm_write(ioctx, table, entry);

        return CephLsmDB::kOK;
    }

    int CephLsmDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values)
    {
        return Insert(table, key, values);
    }

    int CephLsmDB::Delete(const std::string &table, const std::string &key)
    {
        return 0;
    }

    CephLsmDB::~CephLsmDB() {
        /*ioctx.close();
        destroy_one_pool_pp(pool_name, cluster);*/
    }

}