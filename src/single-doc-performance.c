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
   mongoc_client_t       *client;
   bson_t                 ismaster;
} run_cmd_test_t;


static void
run_cmd_setup (perf_test_t *test)
{
   run_cmd_test_t *context;

   context = (run_cmd_test_t *) test->context;
   context->client = mongoc_client_new (NULL);

   bson_init (&context->ismaster);
   BSON_APPEND_INT32 (&context->ismaster, "ismaster", 1);
}


static void
run_cmd_task (perf_test_t *test)
{
   run_cmd_test_t *context;
   bson_error_t error;
   int i;
   bool r;

   context = (run_cmd_test_t *) test->context;

   for (i = 0; i < NUM_DOCS; i++) {
      r = mongoc_client_command_simple (context->client, "admin",
                                        &context->ismaster, NULL,
                                        NULL, &error);

      if (!r) {
         MONGOC_ERROR ("ismaster: %s\n", error.message);
         abort ();
      }
   }
}


static void
run_cmd_teardown (perf_test_t *test)
{
   run_cmd_test_t *context;

   context = (run_cmd_test_t *) test->context;
   bson_destroy (&context->ismaster);
   mongoc_client_destroy (context->client);
}


typedef struct
{
   mongoc_client_t     *client;
   mongoc_collection_t *collection;
   bson_oid_t          *oids;
} find_one_test_t;



static void
find_one_setup (perf_test_t *test)
{
   find_one_test_t *context;
   bson_t tweet;
   bson_t empty = BSON_INITIALIZER;
   bson_oid_t oid;
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   int i;

   context = (find_one_test_t *) test->context;
   context->client = mongoc_client_new (NULL);
   context->collection = mongoc_client_get_collection (context->client,
                                                        "perftest", "corpus");
   context->oids = bson_malloc0 (sizeof (bson_oid_t) * NUM_DOCS);

   read_json_file (test->data_path, &tweet);

   bson_init (&empty);
   if (!mongoc_collection_remove (context->collection, MONGOC_REMOVE_NONE,
                                  &empty, NULL, &error)) {
      MONGOC_ERROR ("collection_remove: %s\n", error.message);
      abort ();
   }

   bulk = mongoc_collection_create_bulk_operation (context->collection,
                                                   true, NULL);

   for (i = 0; i < NUM_DOCS; i++) {
      bson_init (&empty);
      bson_oid_init (&oid, NULL);
      BSON_APPEND_OID (&empty, "_id", &oid);
      bson_concat (&empty, &tweet);
      mongoc_bulk_operation_insert (bulk, &empty);
      bson_destroy (&empty);

      bson_oid_copy (&oid, &context->oids[i]);
   }

   if (!mongoc_bulk_operation_execute (bulk, NULL, &error)) {
      MONGOC_ERROR ("bulk insert: %s\n", error.message);
      abort ();
   }

   mongoc_bulk_operation_destroy (bulk);
   bson_destroy (&tweet);
}


static void
find_one_task (perf_test_t *test)
{
   find_one_test_t *context;
   bson_t query;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_error_t error;
   int i;

   context = (find_one_test_t *) test->context;

   for (i = 0; i < NUM_DOCS; i++) {
      bson_init (&query);
      bson_append_oid (&query, "_id", 3, &context->oids[i]);
      cursor = mongoc_collection_find (context->collection, MONGOC_QUERY_NONE,
                                       0, 1, 0, &query, NULL, NULL);

      if (!mongoc_cursor_next (cursor, &doc)) {
         if (mongoc_cursor_error (cursor, &error)) {
            MONGOC_ERROR ("find_one: %s\n", error.message);
         } else {
            MONGOC_ERROR ("empty result\n");
         }

         abort ();
      }

      mongoc_cursor_destroy (cursor);
      bson_destroy (&query);
   }
}


static void
find_one_teardown (perf_test_t *test)
{
   find_one_test_t *context;

   context = (find_one_test_t *) test->context;
   mongoc_collection_destroy (context->collection);
   mongoc_client_destroy (context->client);
   bson_free (context->oids);
}


typedef struct
{
   mongoc_client_t     *client;
   mongoc_collection_t *collection;
   bson_t               doc;
} small_doc_test_t;


static void
small_doc_setup (perf_test_t *test)
{
   small_doc_test_t *context;
   bson_t empty = BSON_INITIALIZER;
   bson_error_t error;

   context = (small_doc_test_t *) test->context;
   context->client = mongoc_client_new (NULL);
   context->collection = mongoc_client_get_collection (context->client,
                                                       "perftest", "corpus");

   read_json_file (test->data_path, &context->doc);

   bson_init (&empty);
   if (!mongoc_collection_remove (context->collection, MONGOC_REMOVE_NONE,
                                  &empty, NULL, &error)) {
      MONGOC_ERROR ("collection_remove: %s\n", error.message);
      abort ();
   }
}


static void
small_doc_task (perf_test_t *test)
{
   small_doc_test_t *context;
   bson_error_t error;
   int i;

   context = (small_doc_test_t *) test->context;

   for (i = 0; i < NUM_DOCS; i++) {
      if (!mongoc_collection_insert (context->collection, MONGOC_INSERT_NONE,
                                &context->doc, NULL, &error)) {
         MONGOC_ERROR ("insert: %s\n", error.message);
         abort ();
      }
   }
}


static void
small_doc_teardown (perf_test_t *test)
{
   small_doc_test_t *context;

   context = (small_doc_test_t *) test->context;
   mongoc_collection_destroy (context->collection);
   mongoc_client_destroy (context->client);
   bson_destroy (&context->doc);
}


#define SINGLE_DOC_TEST(prefix, name, filename) \
   { sizeof (prefix ## _test_t), #name, "SINGLE_DOCUMENT/" #filename ".json", \
     prefix ## _setup, NULL, prefix ## _task, NULL, prefix ## _teardown }


void
single_doc_perf (void)
{
   perf_test_t tests[] = {
      SINGLE_DOC_TEST (run_cmd, TestRunCommand, NULL),
      SINGLE_DOC_TEST (find_one, TestFindOneByID, TWEET),
      SINGLE_DOC_TEST (small_doc, TestSmallDocInsertOne, SMALL_DOC),
      { 0 },
   };

   run_perf_tests (tests);
}
