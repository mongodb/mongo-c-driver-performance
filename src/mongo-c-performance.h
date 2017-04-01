/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MONGO_C_PERFORMANCE_MONGO_C_PERFORMANCE_H
#define MONGO_C_PERFORMANCE_MONGO_C_PERFORMANCE_H

#include <bson.h>
#include <mongoc.h>

#include <stddef.h>
#include <assert.h>

const int  NUM_ITERATIONS;
const int  NUM_DOCS;
char      *g_test_dir;

typedef struct _perf_test_t perf_test_t;

typedef void (*perf_callback_t)(perf_test_t *test);

struct _perf_test_t {
   const char       *name;
   const char       *data_path;
   int64_t           data_sz;
   perf_callback_t   setup;
   perf_callback_t   before;
   perf_callback_t   task;
   perf_callback_t   after;
   perf_callback_t   teardown;
};


void        prep_tmp_dir        (const char      *path);
void        parse_args          (int              argc,
                                 char           **argv);
const char *get_ext             (const char      *filename);
void        read_json_file      (const char      *data_path,
                                 bson_t          *bson);
void        write_one_byte_file (mongoc_gridfs_t *gridfs);
void        run_test_as_utility (perf_test_t     *test);
void        perf_test_init      (perf_test_t     *test,
                                 const char      *name,
                                 const char      *data_path,
                                 int64_t          data_sz);
void        perf_test_teardown  (perf_test_t     *test);
void        perf_test_setup     (perf_test_t     *test);
void        perf_test_before    (perf_test_t     *test);
void        perf_test_task      (perf_test_t     *test);
void        perf_test_after     (perf_test_t     *test);
void        open_output         (void);
void        close_output        (void);
void        print_header        (void);
void        print_footer        (void);
void        run_perf_tests      (perf_test_t    **tests);

#endif //MONGO_C_PERFORMANCE_MONGO_C_PERFORMANCE_H
