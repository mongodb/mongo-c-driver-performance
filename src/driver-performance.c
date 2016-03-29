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


/*
 *  -------- BENCHMARK BASE CODE ----------------------------------------------
 */

typedef struct {
   mongoc_client_t     *client;
   mongoc_collection_t *collection;
} driver_test_t;

static void
driver_test_setup (perf_test_t *test)
{
   driver_test_t *context;
   mongoc_database_t *db;
   bson_error_t error;

   context = (driver_test_t *) test->context;
   context->client = mongoc_client_new (NULL);
   context->collection = mongoc_client_get_collection (context->client,
                                                       "perftest", "corpus");

   db = mongoc_client_get_database (context->client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }

   mongoc_database_destroy (db);
}

static void
driver_test_teardown (perf_test_t *test)
{
   driver_test_t *context;

   context = (driver_test_t *) test->context;
   mongoc_collection_destroy (context->collection);
   mongoc_client_destroy (context->client);
}


/*
 *  -------- RUN-COMMAND BENCHMARK -------------------------------------------
 */

typedef struct
{
   driver_test_t base;
   bson_t        ismaster;
} run_cmd_test_t;

static void
run_cmd_setup (perf_test_t *test)
{
   run_cmd_test_t *context;

   driver_test_setup (test);

   context = (run_cmd_test_t *) test->context;
   bson_init (&context->ismaster);
   BSON_APPEND_BOOL (&context->ismaster, "ismaster", true);
}

#define run_cmd_before NULL

static void
run_cmd_task (perf_test_t *test)
{
   run_cmd_test_t *context;
   bson_error_t error;
   int i;
   bool r;

   context = (run_cmd_test_t *) test->context;

   for (i = 0; i < NUM_DOCS; i++) {
      r = mongoc_client_command_simple (context->base.client, "admin",
                                        &context->ismaster, NULL,
                                        NULL, &error);

      if (!r) {
         MONGOC_ERROR ("ismaster: %s\n", error.message);
         abort ();
      }
   }
}

#define run_cmd_after NULL

static void
run_cmd_teardown (perf_test_t *test)
{
   run_cmd_test_t *context;

   context = (run_cmd_test_t *) test->context;
   bson_destroy (&context->ismaster);

   driver_test_teardown (test);
}


/*
 *  -------- FIND-ONE BENCHMARK ----------------------------------------------
 */

typedef driver_test_t find_one_test_t;

static void
find_one_setup (perf_test_t *test)
{
   find_one_test_t *context;
   bson_t tweet;
   bson_t empty;
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   int32_t i;

   driver_test_setup (test);

   context = (find_one_test_t *) test->context;
   read_json_file (test->data_path, &tweet);

   bulk = mongoc_collection_create_bulk_operation (context->collection,
                                                   true, NULL);

   for (i = 0; i < NUM_DOCS; i++) {
      bson_init (&empty);
      BSON_APPEND_INT32 (&empty, "_id", i);
      bson_concat (&empty, &tweet);
      mongoc_bulk_operation_insert (bulk, &empty);
      bson_destroy (&empty);
   }

   if (!mongoc_bulk_operation_execute (bulk, NULL, &error)) {
      MONGOC_ERROR ("bulk insert: %s\n", error.message);
      abort ();
   }

   mongoc_bulk_operation_destroy (bulk);
   bson_destroy (&tweet);
}

#define find_one_before NULL

static void
find_one_task (perf_test_t *test)
{
   find_one_test_t *context;
   bson_t query;
   bson_iter_t iter;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_error_t error;
   int i;

   context = (find_one_test_t *) test->context;
   bson_init (&query);
   bson_append_int32 (&query, "_id", 3, 1);
   bson_iter_init_find (&iter, &query, "_id");

   for (i = 0; i < NUM_DOCS; i++) {
      bson_iter_overwrite_int32 (&iter, (int32_t) i);
      cursor = mongoc_collection_find (context->collection,
                                       MONGOC_QUERY_NONE, 0, 1, 0,
                                       &query, NULL, NULL);

      if (!mongoc_cursor_next (cursor, &doc)) {
         if (mongoc_cursor_error (cursor, &error)) {
            MONGOC_ERROR ("find_one: %s\n", error.message);
         } else {
            MONGOC_ERROR ("empty result\n");
         }

         abort ();
      }

      mongoc_cursor_destroy (cursor);
   }

   bson_destroy (&query);
}

#define find_one_after NULL

static void
find_one_teardown (perf_test_t *test)
{
   driver_test_teardown (test);
}


/*
 *  -------- SINGLE-DOCUMENT BENCHMARKS --------------------------------------
 */

/* a "base" struct for tests that load one document from JSON */
typedef struct
{
   driver_test_t base;
   bson_t        doc;
} single_doc_test_t;

static void
single_doc_setup (perf_test_t *test)
{
   single_doc_test_t *context;

   driver_test_setup (test);

   context = (single_doc_test_t *) test->context;
   assert (test->data_path);
   read_json_file (test->data_path, &context->doc);
}

static void
single_doc_before (perf_test_t *test)
{
   single_doc_test_t *context;
   bson_t cmd = BSON_INITIALIZER;
   bson_error_t error;

   context = (single_doc_test_t *) test->context;

   if (!mongoc_collection_drop (context->base.collection, &error) &&
       !strstr (error.message, "ns not found")) {
      MONGOC_ERROR ("drop collection: %s\n", error.message);
      abort ();
   }

   BSON_APPEND_UTF8 (&cmd, "create", "corpus");

   if (!mongoc_collection_command_simple (context->base.collection, &cmd,
                                          NULL, NULL, &error)) {
      MONGOC_ERROR ("create collection: %s\n", error.message);
      abort ();
   }

   bson_destroy (&cmd);
}

static void
single_doc_task (perf_test_t *test,
                 int          num_docs)
{
   single_doc_test_t *context;
   bson_error_t error;
   int i;

   context = (single_doc_test_t *) test->context;

   for (i = 0; i < num_docs; i++) {
      if (!mongoc_collection_insert (context->base.collection,
                                     MONGOC_INSERT_NONE,
                                     &context->doc, NULL, &error)) {
         MONGOC_ERROR ("insert: %s\n", error.message);
         abort ();
      }
   }
}

static void
single_doc_teardown (perf_test_t *test)
{
   single_doc_test_t *context;

   context = (single_doc_test_t *) test->context;
   bson_destroy (&context->doc);

   driver_test_teardown (test);
}

typedef single_doc_test_t small_doc_test_t;

#define small_doc_setup single_doc_setup
#define small_doc_before single_doc_before

static void
small_doc_task (perf_test_t *test)
{
   single_doc_task (test, NUM_DOCS);
}

#define small_doc_after NULL
#define small_doc_teardown single_doc_teardown

typedef single_doc_test_t large_doc_test_t;

#define large_doc_setup single_doc_setup
#define large_doc_before single_doc_before

static void
large_doc_task (perf_test_t *test)
{
   single_doc_task (test, 10);
}

#define large_doc_after NULL
#define large_doc_teardown single_doc_teardown

typedef single_doc_test_t find_many_test_t;


/*
 *  -------- FIND-MANY BENCHMARK ---------------------------------------------
 */

static void
find_many_setup (perf_test_t *test)
{
   single_doc_test_t *context;
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   int i;

   test->data_path = "SINGLE_DOCUMENT/TWEET.json";
   single_doc_setup (test);

   context = (single_doc_test_t *) test->context;
   bulk = mongoc_collection_create_bulk_operation (context->base.collection,
                                                   true, NULL);

   for (i = 0; i < NUM_DOCS; i++) {
      mongoc_bulk_operation_insert (bulk, &context->doc);
   }

   if (!mongoc_bulk_operation_execute (bulk, NULL, &error)) {
      MONGOC_ERROR ("bulk insert: %s\n", error.message);
      abort ();
   }

   mongoc_bulk_operation_destroy (bulk);
}

#define find_many_before NULL

static void
find_many_task (perf_test_t *test)
{
   single_doc_test_t *context;
   bson_t query = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_error_t error;

   context = (single_doc_test_t *) test->context;
   cursor = mongoc_collection_find (context->base.collection, MONGOC_QUERY_NONE,
                                    0, 1, 0, &query, NULL, NULL);

   while (mongoc_cursor_next (cursor, &doc)) {
   }

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("find_many: %s\n", error.message);
      abort ();
   }

   mongoc_cursor_destroy (cursor);
}

#define find_many_after NULL
#define find_many_teardown single_doc_teardown


/*
 *  -------- BULK-INSERT BENCHMARKS ------------------------------------------
 */

/* base for test_bulk_insert_small_doc / large_doc */
typedef struct {
   single_doc_test_t   base;
   bson_t            **docs;
   int                 num_docs;
} bulk_insert_test_t;

static void
bulk_insert_setup (perf_test_t *test,
                   int          num_docs)
{
   bulk_insert_test_t *context;
   int i;

   single_doc_setup (test);

   context = (bulk_insert_test_t *) test->context;
   context->num_docs = num_docs;
   context->docs = (bson_t **) bson_malloc (num_docs * sizeof (bson_t *));
   for (i = 0; i < num_docs; i++) {
      context->docs[i] = &context->base.doc;
   }
}

static void
bulk_insert_task (perf_test_t *test)
{
   bulk_insert_test_t *context;
   bson_error_t error;
   uint32_t num_docs;

   context = (bulk_insert_test_t *) test->context;
   num_docs = (uint32_t) context->num_docs;

BEGIN_IGNORE_DEPRECATIONS
   if (!mongoc_collection_insert_bulk (context->base.base.collection,
                                       MONGOC_INSERT_NONE,
                                       (const bson_t **) context->docs,
                                       num_docs, NULL, &error)) {
      MONGOC_ERROR ("insert_bulk: %s\n", error.message);
      abort ();
   }
END_IGNORE_DEPRECATIONS
}

static void
bulk_insert_teardown (perf_test_t *test)
{
   bulk_insert_test_t *context;

   context = (bulk_insert_test_t *) test->context;
   bson_free (context->docs);

   single_doc_teardown (test);
}

static void
bulk_insert_small_doc_setup (perf_test_t *test)
{
   bulk_insert_setup (test, NUM_DOCS);
}

#define bulk_insert_small_doc_before single_doc_before
#define bulk_insert_small_doc_task bulk_insert_task
#define bulk_insert_small_doc_after NULL
#define bulk_insert_small_doc_teardown bulk_insert_teardown

static void
bulk_insert_large_doc_setup (perf_test_t *test)
{
   bulk_insert_setup (test, 10);
}

#define bulk_insert_large_doc_before single_doc_before
#define bulk_insert_large_doc_task bulk_insert_task
#define bulk_insert_large_doc_after NULL
#define bulk_insert_large_doc_teardown bulk_insert_teardown


#define NO_DOC NULL

#define DRIVER_TEST(prefix, name, filename) \
   { sizeof (prefix ## _test_t), #name, "SINGLE_DOCUMENT/" #filename ".json", \
     prefix ## _setup, prefix ## _before, prefix ## _task, \
     prefix ## _after, prefix ## _teardown }

void
driver_perf (void)
{
   perf_test_t tests[] = {
      DRIVER_TEST (run_cmd,   TestRunCommand,             NO_DOC),
      DRIVER_TEST (find_one,  TestFindOneByID,            TWEET),
      DRIVER_TEST (small_doc, TestSmallDocInsertOne,      SMALL_DOC),
      DRIVER_TEST (large_doc, TestLargeDocInsertOne,      LARGE_DOC),
      DRIVER_TEST (find_many, TestFindManyAndEmptyCursor, NO_DOC),
      DRIVER_TEST (small_doc, TestSmallDocBulkInsert,     SMALL_DOC),
      DRIVER_TEST (large_doc, TestLargeDocBulkInsert,     LARGE_DOC),
      { 0 },
   };

   run_perf_tests (tests);
}
