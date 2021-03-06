cmake_minimum_required(VERSION 2.8)
project (prices)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin/)

include_directories(BEFORE src/imtjson/src src src/docdb/src)
add_compile_options(-std=c++17)
add_compile_options(-Wall -Werror -Wno-noexcept-type)

set(USERVER_NO_SSL 1)

add_subdirectory (src/imtjson/src/imtjson)
add_subdirectory (src/docdb/src/docdblib)
add_subdirectory (src/userver)
add_subdirectory (src/main)

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/opt/prices" CACHE PATH "Default path to install" FORCE)
endif()

install(PROGRAMS bin/prices DESTINATION bin)
install(FILES conf/prices.conf DESTINATION "conf") 
install(FILES www/index.html www/code.js www/style.css www/favicon.png DESTINATION "www") 
install(PROGRAMS collect DESTINATION bin)
install(DIRECTORY DESTINATION "data")
install(DIRECTORY DESTINATION "log")
