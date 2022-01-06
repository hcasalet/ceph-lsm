add_executable(ceph_test_cls_lsm_write
  test_cls_lsm_write.cc
)
target_link_libraries(ceph_test_cls_lsm_write
  cls_lsm_client
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx)

install(TARGETS
  ceph_test_cls_lsm_write
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_cls_lsm_compact
  test_cls_lsm_compact.cc
)
target_link_libraries(ceph_test_cls_lsm_compact
  cls_lsm_client
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx)

install(TARGETS
  ceph_test_cls_lsm_compact
  DESTINATION ${CMAKE_INSTALL_BINDIR})

add_executable(ceph_test_cls_lsm_read
  test_cls_lsm_read.cc
)
target_link_libraries(ceph_test_cls_lsm_read
  cls_lsm_client
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx)

install(TARGETS
  ceph_test_cls_lsm_read
  DESTINATION ${CMAKE_INSTALL_BINDIR})
