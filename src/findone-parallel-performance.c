/*
 * Copyright 2021-present MongoDB, Inc.
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

/* Tests performance of parallel find operations on a client pool.
 * This workload was identified in CDRIVER-4002. */

#include "mongo-c-performance.h"

void findone_parallel_perf_task (perf_test_t *pt) {
   int i;
   int computation = 1;

   for (i = 0; i < 100; i++) {
      computation = computation + 1 * 2;
      usleep (10);
   }
}

perf_test_t * findone_parallel_perf_new (const char* name, int nthreads) {
   perf_test_t * pt = bson_malloc0 (sizeof (perf_test_t));

   perf_test_init (pt, name, NULL /* data path */, 100 /* TODO: choose an appropriate data size. */);
   pt->task = findone_parallel_perf_task;
   return pt;
}

void findone_parallel_perf (void) {
   perf_test_t *perf_tests[] = {
      findone_parallel_perf_new ("FindOneParallel1Threads", 1),
      findone_parallel_perf_new ("FindOneParallel10Threads", 10),
      findone_parallel_perf_new ("FindOneParallel100Threads", 100),
      NULL
   };

   run_perf_tests (perf_tests);
}