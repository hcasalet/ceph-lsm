add_executable(ceph_test_cls_lsm
  test_cls_lsm.cc
)
target_link_libraries(ceph_test_cls_lsm
  cls_lsm_client
  librados
  global
  ${UNITTEST_LIBS}
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx)

install(TARGETS
  ceph_test_cls_lsm
  DESTINATION ${CMAKE_INSTALL_BINDIR})
