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

#include "mongo-c-performance.h"

#include <bson.h>
#include <mongoc.h>

extern void bson_perf            (void);
extern void driver_perf          (void);
extern void gridfs_perf          (void);
extern void parallel_perf        (void);
extern void gridfs_parallel_perf (void);

int
main (int    argc,
      char **argv)
{
   mongoc_init ();

   parse_args (argc, argv);

   bson_perf ();
   driver_perf ();
   gridfs_perf ();
   parallel_perf ();
   gridfs_parallel_perf ();

   mongoc_cleanup ();
}
