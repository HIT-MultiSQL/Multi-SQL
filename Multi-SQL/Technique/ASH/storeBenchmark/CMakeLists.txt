cmake_minimum_required(VERSION 3.14)
project(storeBenchmark VERSION 1.0)

# set the project name and version
set(CMAKE_CXX_STANDARD 14)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(SCRIPT_DIR ${PROJECT_SOURCE_DIR}/script)
set(TBL_DIR /home/weiyan/tpchdata)
set(DB_DIR /home/weiyan/dbtest)

link_libraries(pthread)
link_libraries(dl)
link_libraries(rt)
link_libraries(snappy)
link_libraries(gflags)
link_libraries(z)
link_libraries(bz2)
link_libraries(zstd)
link_libraries(lz4)
find_library(rocksdb rocksdb)
find_library(wiredtiger wiredtiger)
find_package(Boost COMPONENTS filesystem REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})

# add the executable
aux_source_directory(./src SRC_LIST)
aux_source_directory(./src/tools TOOLS_LIST)
aux_source_directory(./src/query QUERY_LIST)
add_executable(main ${SRC_LIST} ${TOOLS_LIST} ${QUERY_LIST} src/query/LSMGroupResolver.cpp src/query/LSMGroupResolver.h src/query/ColumnStoreResolver.cpp src/query/ColumnStoreResolver.h src/query/RowStoreResolver.cpp src/query/RowStoreResolver.h src/query/BaseResolver.cpp src/query/BaseResolver.h src/UserInformSource.h src/UserInform.h src/UserInform.cpp src/UserInformSource.cpp src/UserBehavior.h src/UserBehavior.cpp src/UserBehaviorSource.h src/UserBehaviorSource.cpp src/UserInformColumnSource.h src/UserInformColumnSource.cpp src/UserBehaviorColumnSource.h src/UserBehaviorColumnSource.cpp src/query/UserBaseResolver.h src/query/UserBaseResolver.cpp src/query/UserRowStoreResolver.h src/query/UserRowStoreResolver.cpp src/query/UserColumnStoreResolver.h src/query/UserColumnStoreResolver.cpp src/query/UserLSMFullResolver.h src/query/UserLSMFullResolver.cpp src/query/UserLSMGroupResolver.h src/query/UserLSMGroupResolver.cpp)

# configure a header file to pass some of the CMake settings
# to the source code
configure_file(BenchmarkConfig.h.in BenchmarkConfig.h)

# config includes
target_include_directories(main PUBLIC
        # get BenchmarkConfig.h
        ${PROJECT_BINARY_DIR}
        )

# link library
target_link_libraries(main PUBLIC
        rocksdb
        wiredtiger
        ${Boost_LIBRARIES}
        )
link_directories("/usr/local/lib/")





