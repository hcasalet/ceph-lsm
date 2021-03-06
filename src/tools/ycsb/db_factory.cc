//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db_factory.h"

#include <string>

#include "cephlsm_db.h"
#include "readoptimized_db.h"
#include "writeoptimized_db.h"

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "cephlsm") {
    return new CephLsmDB(props);
  } else if (props["dbname"] == "readoptimized") {
    return new ReadOptimizedDB(props);
  } else if (props["dbname"] == "writeoptimized") {
    return new WriteOptimizedDB(props);
  } else return NULL;
}
