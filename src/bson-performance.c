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

typedef struct {
   bson_t bson;
} bson_perf_test_t;


static void
bson_perf_setup (perf_test_t *test)
{
   bson_perf_test_t *context;

   context = (bson_perf_test_t *) test->context;
   read_json_file (test->data_path, &context->bson);
}


static bool
_visit_document (const bson_iter_t *iter,
                 const char        *key,
                 const bson_t      *v_document,
                 void              *data);


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
                 const char        *key,
                 const bson_t      *v_document,
                 void              *data)
{
   bson_iter_t child;

   assert (bson_iter_init (&child, v_document));
   bson_iter_visit_all (&child, &visitors, NULL);

   return false; /* continue */
}


static void
bson_perf_task (perf_test_t *test)
{
   bson_perf_test_t *context;
   bson_iter_t iter;
   int i;

   context = (bson_perf_test_t *) test->context;

   for (i = 0; i < NUM_DOCS; i++) {
      /* Other drivers test "encoding" some data structure to BSON. libbson has
       * no analog; just visit all elements recursively. */
      bson_iter_init (&iter, &context->bson);
      bson_iter_visit_all (&iter, &visitors, NULL);
   }
}


static void
bson_perf_teardown (perf_test_t *test)
{
   bson_perf_test_t *context;

   context = (bson_perf_test_t *) test->context;
   bson_destroy (&context->bson);
}


#define BSON_TEST(name, filename) \
   { sizeof (bson_perf_test_t), #name, "EXTENDED_BSON/" #filename ".json", \
     bson_perf_setup, NULL, bson_perf_task, NULL, bson_perf_teardown }


void
bson_perf (void)
{
   /* other drivers' idea of encoding vs decoding doesn't apply to libbson */
   perf_test_t tests[] = {
      BSON_TEST (TestFlatEncoding, flat_bson),
      BSON_TEST (TestDeepEncoding, deep_bson),
      BSON_TEST (TestFullEncoding, full_bson),

      BSON_TEST (TestFlatDecoding, flat_bson),
      BSON_TEST (TestDeepDecoding, deep_bson),
      BSON_TEST (TestFullDecoding, full_bson),
      { 0 },
   };

   run_perf_tests (tests);
}
