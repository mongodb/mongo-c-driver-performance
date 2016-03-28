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

const int NUM_ITERATIONS;
const int NUM_DOCS;

typedef struct _perf_test_t perf_test_t;

typedef void (*perf_callback_t)(perf_test_t *test);

struct _perf_test_t {
   const char       *name;
   const char       *data_path;
   perf_callback_t   setup;
   perf_callback_t   before;
   perf_callback_t   task;
   perf_callback_t   after;
   perf_callback_t   teardown;
   void             *context;
};

void parse_args     (int argc, char **argv);
void run_perf_tests (perf_test_t *tests);

#endif //MONGO_C_PERFORMANCE_MONGO_C_PERFORMANCE_H
