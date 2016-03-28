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
   mongoc_client_t *client;
   bson_t           ismaster;
} single_doc_perf_test_t;


static void
single_doc_perf_setup (perf_test_t *test)
{
   single_doc_perf_test_t *single_doc_test;

   test->context = bson_malloc0 (sizeof (single_doc_perf_test_t));
   single_doc_test = (single_doc_perf_test_t *) test->context;
   single_doc_test->client = mongoc_client_new (NULL);

   bson_init (&single_doc_test->ismaster);
   BSON_APPEND_INT32 (&single_doc_test->ismaster, "ismaster", 1);
}


static void
ismaster_perf_task (perf_test_t *test)
{
   single_doc_perf_test_t *single_doc_test;
   bson_error_t error;
   int i;
   bool r;

   single_doc_test = (single_doc_perf_test_t *) test->context;

   for (i = 0; i < NUM_DOCS; i++) {
      r = mongoc_client_command_simple (single_doc_test->client, "admin",
                                        &single_doc_test->ismaster, NULL, NULL,
                                        &error);

      if (!r) {
         MONGOC_ERROR ("ismaster: %s\n", error.message);
         abort ();
      }
   }
}


static void
ismaster_perf_teardown (perf_test_t *test)
{
   single_doc_perf_test_t *single_doc_test;

   single_doc_test = (single_doc_perf_test_t *) test->context;
   mongoc_client_destroy (single_doc_test->client);
   bson_destroy (&single_doc_test->ismaster);
   bson_free (single_doc_test);
}


#define SINGLE_DOC_TEST(name, filename, task) \
   { #name, "SINGLE_DOCUMENT/" #filename ".json", \
     single_doc_perf_setup, NULL, task, NULL, ismaster_perf_teardown }


void
single_doc_perf (void)
{
   perf_test_t tests[] = {
      { "TestRunCommand", NULL, single_doc_perf_setup, NULL,
        ismaster_perf_task, NULL, ismaster_perf_teardown },
      { 0 },
   };

   run_perf_tests (tests);
}
