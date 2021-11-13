cmake_minimum_required(VERSION 2.8)
project(mongo-c-performance)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/build/cmake/")

find_package (Threads)

set(LIBMONGOC_REQUIRED_VERSION 1.9.2)
find_package(mongoc-1.0 ${LIBMONGOC_REQUIRED_VERSION} REQUIRED)

include_directories(
    ${MONGOC_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/src
)

set(SOURCE_FILES
    ${CMAKE_SOURCE_DIR}/src/bson-performance.c
    ${CMAKE_SOURCE_DIR}/src/main.c
    ${CMAKE_SOURCE_DIR}/src/mongo-c-performance.h
    ${CMAKE_SOURCE_DIR}/src/mongo-c-performance.c
    ${CMAKE_SOURCE_DIR}/src/driver-performance.c
    ${CMAKE_SOURCE_DIR}/src/gridfs-performance.c
    ${CMAKE_SOURCE_DIR}/src/gridfs-parallel-performance.c
    ${CMAKE_SOURCE_DIR}/src/ldjson-performance.c
    ${CMAKE_SOURCE_DIR}/src/findone-parallel-performance.c
)

add_executable(mongo-c-performance ${SOURCE_FILES})
target_link_libraries(
        mongo-c-performance
        mongo::mongoc_shared
        ${CMAKE_THREAD_LIBS_INIT}
)
