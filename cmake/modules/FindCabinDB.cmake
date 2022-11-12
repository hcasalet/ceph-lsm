# Find the native Cabindb includes and library
# This module defines
#  CABINDB_INCLUDE_DIR, where to find cabindb/db.h, Set when
#                       CABINDB_INCLUDE_DIR is found.
#  CABINDB_LIBRARIES, libraries to link against to use Cabindb.
#  CABINDB_FOUND, If false, do not try to use Cabindb.
#  CABINDB_VERSION_STRING
#  CABINDB_VERSION_MAJOR
#  CABINDB_VERSION_MINOR
#  CABINDB_VERSION_PATCH

find_path(CABINDB_INCLUDE_DIR cabindb/db.h)

find_library(CABINDB_LIBRARIES cabindb)

if(CABINDB_INCLUDE_DIR AND EXISTS "${CABINDB_INCLUDE_DIR}/cabindb/version.h")
  foreach(ver "MAJOR" "MINOR" "PATCH")
    file(STRINGS "${CABINDB_INCLUDE_DIR}/cabindb/version.h" CABINDB_VER_${ver}_LINE
      REGEX "^#define[ \t]+CABINDB_${ver}[ \t]+[0-9]+$")
    string(REGEX REPLACE "^#define[ \t]+CABINDB_${ver}[ \t]+([0-9]+)$"
      "\\1" CABINDB_VERSION_${ver} "${CABINDB_VER_${ver}_LINE}")
    unset(${CABINDB_VER_${ver}_LINE})
  endforeach()
  set(CABINDB_VERSION_STRING
    "${CABINDB_VERSION_MAJOR}.${CABINDB_VERSION_MINOR}.${CABINDB_VERSION_PATCH}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CabinDB
  REQUIRED_VARS CABINDB_LIBRARIES CABINDB_INCLUDE_DIR
  VERSION_VAR CABINDB_VERSION_STRING)

mark_as_advanced(
  CABINDB_INCLUDE_DIR
  CABINDB_LIBRARIES)

if(CabinDB_FOUND)
  if(NOT TARGET CabinDB::CabinDB)
    add_library(CabinDB::CabinDB UNKNOWN IMPORTED)
    set_target_properties(CabinDB::CabinDB PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${CABINDB_INCLUDE_DIR}"
      IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
      IMPORTED_LOCATION "${CABINDB_LIBRARIES}"
      VERSION "${CABINDB_VERSION_STRING}")
  endif()
endif()

