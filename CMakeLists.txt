cmake_minimum_required(VERSION 2.6)
include_directories(/usr/local/BerkeleyDB.18.1/include)
include_directories(/usr/local/sql-parser/src)
link_directories(/usr/local/sql-parser)
link_directories(/usr/local/BerkeleyDB.18.1/lib)

set(CMAKE_CXX_FLAGS "-std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT")

ADD_EXECUTABLE(sql5300 sql5300.cpp)

target_link_libraries(sql5300 db_cxx sqlparser)