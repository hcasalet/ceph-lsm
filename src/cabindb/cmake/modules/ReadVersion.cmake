# Read cabindb version from version.h header file.

function(get_cabindb_version version_var)
  file(READ "${CMAKE_CURRENT_SOURCE_DIR}/include/cabindb/version.h" version_header_file)
  foreach(component MAJOR MINOR PATCH)
    string(REGEX MATCH "#define CABINDB_${component} ([0-9]+)" _ ${version_header_file})
    set(CABINDB_VERSION_${component} ${CMAKE_MATCH_1})
  endforeach()
  set(${version_var} "${CABINDB_VERSION_MAJOR}.${CABINDB_VERSION_MINOR}.${CABINDB_VERSION_PATCH}" PARENT_SCOPE)
endfunction()
