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
   char *path;
   bson_perf_test_t *bson_test;
   bson_json_reader_t *reader;
   bson_error_t error;
   int r;

   test->context = bson_malloc0 (sizeof (bson_perf_test_t));
   bson_test = (bson_perf_test_t *) test->context;
   bson_init (&bson_test->bson);

   path = bson_strdup_printf ("performance-testdata/%s", test->data_path);
   reader = bson_json_reader_new_from_file (path, &error);
   if (!reader) {
      MONGOC_ERROR ("%s: %s\n", path, error.message);
      abort ();
   }

   r = bson_json_reader_read (reader, &bson_test->bson, &error);
   if (r < 0) {
      MONGOC_ERROR ("%s: %s\n", test->data_path, error.message);
      abort ();
   }

   if (r == 0) {
      MONGOC_ERROR ("%s: no data\n", test->data_path);
      abort ();
   }

   bson_json_reader_destroy (reader);
   bson_free (path);
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
   bson_perf_test_t *bson_test;
   bson_iter_t iter;
   int i;

   bson_test = (bson_perf_test_t *) test->context;

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

   bson_test = (bson_perf_test_t *) test->context;
   bson_destroy (&bson_test->bson);
   bson_free (bson_test);
}


#define BSON_TEST(name, filename) \
   { #name, "EXTENDED_BSON/" #filename ".json", \
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
