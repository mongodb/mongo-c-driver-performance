cmake_minimum_required(VERSION 3.3)
project(mongo-c-performance)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/build/cmake/")

set(LIBBSON_REQUIRED_VERSION 1.3.4)
set(LIBBSON_REQUIRED_ABI_VERSION 1.0)
find_package(LibBSON ${LIBBSON_REQUIRED_VERSION} REQUIRED)

set(LIBMONGOC_REQUIRED_VERSION 1.3.4)
set(LIBMONGOC_REQUIRED_ABI_VERSION 1.0)
find_package(LibMongoC ${LIBMONGOC_REQUIRED_VERSION} REQUIRED)

include_directories(
    ${LIBBSON_INCLUDE_DIRS}
    ${LIBMONGOC_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/src
)

link_directories(${LIBBSON_LIBRARY_DIRS} ${LIBMONGOC_LIBRARY_DIRS})

set(SOURCE_FILES
    ${CMAKE_SOURCE_DIR}/src/bson-performance.c
    ${CMAKE_SOURCE_DIR}/src/main.c
    ${CMAKE_SOURCE_DIR}/src/mongo-c-performance.h
    ${CMAKE_SOURCE_DIR}/src/mongo-c-performance.c
    ${CMAKE_SOURCE_DIR}/src/driver-performance.c
)

add_executable(mongo-c-performance ${SOURCE_FILES})
target_link_libraries(mongo-c-performance mongoc-1.0 bson-1.0)
