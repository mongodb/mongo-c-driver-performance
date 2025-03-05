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

#include <bson/bson.h>
#include <mongoc/mongoc.h>

typedef struct {
   perf_test_t base;
   bson_t bson;
} bson_perf_test_t;


static void
bson_perf_setup (perf_test_t *test)
{
   bson_perf_test_t *bson_test;

   perf_test_setup (test);

   bson_test = (bson_perf_test_t *) test;
   read_json_file (test->data_path, &bson_test->bson);
}


static bool
_visit_document (const bson_iter_t *iter,
                 const char *key,
                 const bson_t *v_document,
                 void *data);


static const bson_visitor_t visitors = {
   NULL, /* visit_before */
   NULL, /* visit_after */
   NULL, /* visit_corrupt */
   NULL, /* visit_double */
   NULL, /* visit_utf8 */
   _visit_document,
   _visit_document, /* use same function for visit_array */
};


static bool
_visit_document (const bson_iter_t *iter,
                 const char *key,
                 const bson_t *v_document,
                 void *data)
{
   bson_iter_t child;

   BSON_ASSERT (bson_iter_init (&child, v_document));
   bson_iter_visit_all (&child, &visitors, NULL);

   return false; /* continue */
}


static void
bson_perf_task (perf_test_t *test)
{
   bson_perf_test_t *bson_test;
   bson_iter_t iter;
   int i;

   bson_test = (bson_perf_test_t *) test;

   for (i = 0; i < NUM_DOCS; i++) {
      /* Other drivers test "encoding" some data structure to BSON. libbson has
       * no analog; just visit all elements recursively. */
      bson_iter_init (&iter, &bson_test->bson);
      bson_iter_visit_all (&iter, &visitors, NULL);
   }
}


static void
bson_perf_teardown (perf_test_t *test)
{
   bson_perf_test_t *bson_test;

   bson_test = (bson_perf_test_t *) test;
   bson_destroy (&bson_test->bson);

   perf_test_teardown (test);
}


static void
bson_perf_init (bson_perf_test_t *bson_perf_test,
                const char *name,
                const char *data_path,
                int64_t data_sz)
{
   perf_test_init ((perf_test_t *) bson_perf_test, name, data_path, data_sz);
   bson_perf_test->base.setup = bson_perf_setup;
   bson_perf_test->base.task = bson_perf_task;
   bson_perf_test->base.teardown = bson_perf_teardown;
}

static perf_test_t *
bson_perf_new (const char *name, const char *data_path, int64_t data_sz)
{
   bson_perf_test_t *bson_perf_test;

   bson_perf_test = bson_malloc0 (sizeof (bson_perf_test_t));
   bson_perf_init (bson_perf_test, name, data_path, data_sz);

   return (perf_test_t *) bson_perf_test;
}


void
bson_perf (void)
{
   /* other drivers' idea of encoding vs decoding doesn't apply to libbson */
   perf_test_t *tests[] = {
      bson_perf_new (
         "TestFlatEncoding", "extended_bson/flat_bson.json", 75310000),
      bson_perf_new (
         "TestDeepEncoding", "extended_bson/deep_bson.json", 19640000),
      bson_perf_new (
         "TestFullEncoding", "extended_bson/full_bson.json", 57340000),
      bson_perf_new (
         "TestFlatDecoding", "extended_bson/flat_bson.json", 75310000),
      bson_perf_new (
         "TestDeepDecoding", "extended_bson/deep_bson.json", 19640000),
      bson_perf_new (
         "TestFullDecoding", "extended_bson/full_bson.json", 57340000),
      NULL,
   };

   run_perf_tests (tests);
}
