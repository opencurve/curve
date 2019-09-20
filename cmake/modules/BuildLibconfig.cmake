# This module builds libconfig library. Following hints are respected
#
# LIBCONFIG_J: integer (defanult 1)

function(do_build_libconfig)
  set(configure_command ./configure --prefix=/)

  set(build_command make)
  if(LIBCONFIG_J)
    message(STATUS "BUILDING libconfig Libraries at j ${LIBCONFIG_J}")
    list(APPEND BUILD_COMMAND -j${LIBCONFIG_J})
  endif()

  set(install_command make DESTDIR=${CMAKE_BINARY_DIR} install)

  set(source_dir SOURCE_DIR "${PROJECT_SOURCE_DIR}/3rdparty/libconfig")
  include(ExternalProject)
  ExternalProject_Add(Libconfig
                      ${source_dir}
                      CONFIGURE_COMMAND ${configure_command}
                      BUILD_COMMAND ${build_command}
                      BUILD_IN_SOURCE 1
                      INSTALL_COMMAND ${install_command}
                      PREFIX ${CMAKE_BINARY_DIR})
endfunction()

macro(build_libconfig)
  do_build_libconfig()
  ExternalProject_Get_Property(Libconfig install_dir)
  add_library(config
              SHARED
              IMPORTED
              GLOBAL)
  set(LIBCONFIG_SHARED ${install_dir}/lib/libconfig.so.11.0.2)
  set_property(TARGET config PROPERTY IMPORTED_LOCATION ${LIBCONFIG_SHARED})
  install(FILES ${LIBCONFIG_SHARED} DESTINATION lib RENAME libconfig.so.11)
endmacro()
