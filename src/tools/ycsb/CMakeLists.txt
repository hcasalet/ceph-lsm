set(ycsb_cephlsm_srcs
  ycsbc.cc
  cephlsm_db.cc
  readoptimized_db.cc
  writeoptimized_db.cc
  db_factory.cc
  core/core_workload.cc)
add_executable(ycsb_cephlsm ${ycsb_cephlsm_srcs})
target_link_libraries(
    ycsb_cephlsm
    cls_lsm_client
    cls_lsm_read_optimized
    cls_lsm_write_optimized
    librados
    global
    ${BLKID_LIBRARIES}
    ${UNITTEST_LIBS}
    ${EXTRALIBS}
    ${CMAKE_DL_LIBS}
    radostest-cxx)
install(TARGETS
  ycsb_cephlsm
  DESTINATION bin)
