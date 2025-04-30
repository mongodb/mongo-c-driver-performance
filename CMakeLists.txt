cmake_minimum_required(VERSION 3.15)
project(mongo-c-performance)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/build/cmake/")

find_package (Threads)

find_package(mongoc 2.0 REQUIRED)

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
    ${CMAKE_SOURCE_DIR}/src/parallel-client-performance.c
)

add_executable(mongo-c-performance ${SOURCE_FILES})
target_link_libraries(
        mongo-c-performance
        mongoc::shared
        ${CMAKE_THREAD_LIBS_INIT}
)
