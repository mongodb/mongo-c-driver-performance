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
   perf_test_t          base;
   mongoc_client_t     *client;
   mongoc_collection_t *collection;
} driver_test_t;

static void
driver_test_setup (perf_test_t *test)
{
   driver_test_t *driver_test;
   mongoc_database_t *db;
   bson_error_t error;

   perf_test_setup (test);

   driver_test = (driver_test_t *) test;
   driver_test->client = mongoc_client_new (NULL);
   driver_test->collection = mongoc_client_get_collection (driver_test->client,
                                                           "perftest",
                                                           "corpus");

   db = mongoc_client_get_database (driver_test->client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }

   mongoc_database_destroy (db);
}

static void
driver_test_teardown (perf_test_t *test)
{
   driver_test_t *driver_test;

   driver_test = (driver_test_t *) test;
   mongoc_collection_destroy (driver_test->collection);
   mongoc_client_destroy (driver_test->client);

   perf_test_teardown (test);
}

static void
driver_test_init (driver_test_t *driver_test,
                  const char    *name,
                  const char    *data_path,
                  int64_t        data_sz)
{
   perf_test_init (&driver_test->base, name, data_path, data_sz);

   driver_test->base.setup = driver_test_setup;
   driver_test->base.teardown = driver_test_teardown;
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
   run_cmd_test_t *run_cmd_test;

   driver_test_setup (test);

   run_cmd_test = (run_cmd_test_t *) test;
   bson_init (&run_cmd_test->ismaster);
   BSON_APPEND_BOOL (&run_cmd_test->ismaster, "ismaster", true);
}

static void
run_cmd_task (perf_test_t *test)
{
   run_cmd_test_t *run_cmd_test;
   bson_error_t error;
   int i;
   bool r;

   run_cmd_test = (run_cmd_test_t *) test;

   for (i = 0; i < NUM_DOCS; i++) {
      r = mongoc_client_command_simple (run_cmd_test->base.client, "admin",
                                        &run_cmd_test->ismaster, NULL,
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
   run_cmd_test_t *run_cmd_test;

   run_cmd_test = (run_cmd_test_t *) test;
   bson_destroy (&run_cmd_test->ismaster);

   driver_test_teardown (test);
}

static void
run_cmd_init (run_cmd_test_t *run_cmd_test)
{
   driver_test_init (&run_cmd_test->base, "TestRunCommand", NULL, 160000);
   run_cmd_test->base.base.setup = run_cmd_setup;
   run_cmd_test->base.base.task = run_cmd_task;
   run_cmd_test->base.base.teardown = run_cmd_teardown;
}

static perf_test_t *
run_cmd_new (void)
{
   run_cmd_test_t *run_cmd_test;

   run_cmd_test = bson_malloc0 (sizeof (run_cmd_test_t));
   run_cmd_init (run_cmd_test);

   return (perf_test_t *) run_cmd_test;
}


/*
 *  -------- FIND-ONE BENCHMARK ----------------------------------------------
 */

typedef driver_test_t find_one_test_t;

static void
find_one_setup (perf_test_t *test)
{
   find_one_test_t *find_one_test;
   bson_t tweet;
   bson_t empty;
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   int32_t i;

   driver_test_setup (test);

   find_one_test = (find_one_test_t *) test;
   read_json_file (test->data_path, &tweet);

   bulk = mongoc_collection_create_bulk_operation (find_one_test->collection,
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

static void
find_one_task (perf_test_t *test)
{
   find_one_test_t *driver_test;
   bson_t query;
   bson_iter_t iter;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_error_t error;
   int i;

   driver_test = (find_one_test_t *) test;
   bson_init (&query);
   bson_append_int32 (&query, "_id", 3, 1);
   bson_iter_init_find (&iter, &query, "_id");

   for (i = 0; i < NUM_DOCS; i++) {
      bson_iter_overwrite_int32 (&iter, (int32_t) i);
#if MONGOC_CHECK_VERSION(1, 5, 0)
      cursor = mongoc_collection_find_with_opts (driver_test->collection,
                                                 &query, NULL, NULL);
#else
      cursor = mongoc_collection_find (driver_test->collection,
                                       MONGOC_QUERY_NONE, 0, 0, 0,
                                       &query, NULL, NULL);
#endif
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

static void
find_one_init (find_one_test_t *find_one_test)
{
   driver_test_init (find_one_test,
                     "TestFindOneByID",
                     "single_and_multi_document/tweet.json",
                     16220000);
   find_one_test->base.setup = find_one_setup;
   find_one_test->base.task = find_one_task;
}

static perf_test_t *
find_one_new (void)
{
   find_one_test_t *find_one_test;

   find_one_test = (find_one_test_t *) bson_malloc0 (sizeof (find_one_test_t));
   find_one_init (find_one_test);

   return (perf_test_t *) find_one_test;
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
   single_doc_test_t *driver_test;

   driver_test_setup (test);

   driver_test = (single_doc_test_t *) test;
   assert (test->data_path);
   read_json_file (test->data_path, &driver_test->doc);
}

static void
single_doc_before (perf_test_t *test)
{
   single_doc_test_t *driver_test;
   bson_t cmd = BSON_INITIALIZER;
   bson_error_t error;

   driver_test = (single_doc_test_t *) test;

   if (!mongoc_collection_drop (driver_test->base.collection, &error) &&
       !strstr (error.message, "ns not found")) {
      MONGOC_ERROR ("drop collection: %s\n", error.message);
      abort ();
   }

   BSON_APPEND_UTF8 (&cmd, "create", "corpus");

   if (!mongoc_collection_command_simple (driver_test->base.collection, &cmd,
                                          NULL, NULL, &error)) {
      MONGOC_ERROR ("create collection: %s\n", error.message);
      abort ();
   }

   bson_destroy (&cmd);
}

static void
_single_doc_task (perf_test_t *test,
                  int          num_docs)
{
   single_doc_test_t *driver_test;
   bson_error_t error;
   int i;

   driver_test = (single_doc_test_t *) test;

   for (i = 0; i < num_docs; i++) {
      if (!mongoc_collection_insert (driver_test->base.collection,
                                     MONGOC_INSERT_NONE,
                                     &driver_test->doc, NULL, &error)) {
         MONGOC_ERROR ("insert: %s\n", error.message);
         abort ();
      }
   }
}

static void
single_doc_teardown (perf_test_t *test)
{
   single_doc_test_t *driver_test;

   driver_test = (single_doc_test_t *) test;
   bson_destroy (&driver_test->doc);

   driver_test_teardown (test);
}

static void
single_doc_init (single_doc_test_t *single_doc_test,
                 const char        *name,
                 const char        *data_path,
                 int64_t            data_sz)
{
   driver_test_init (&single_doc_test->base, name, data_path, data_sz);
   single_doc_test->base.base.setup = single_doc_setup;
   single_doc_test->base.base.before = single_doc_before;
   single_doc_test->base.base.teardown = single_doc_teardown;
}


/*
 *  -------- SMALL-DOC BENCHMARK ---------------------------------------------
 */

typedef single_doc_test_t small_doc_test_t;

static void
small_doc_task (perf_test_t *test)
{
   _single_doc_task (test, NUM_DOCS);
}

static void
small_doc_init (small_doc_test_t *small_doc_test)
{
   single_doc_init (small_doc_test,
                    "TestSmallDocInsertOne",
                    "single_and_multi_document/small_doc.json",
                    2750000);
   small_doc_test->base.base.task = small_doc_task;
}

static perf_test_t *
small_doc_new (void)
{
   small_doc_test_t *small_doc_test;

   small_doc_test =
      (small_doc_test_t *) bson_malloc0 (sizeof (small_doc_test_t));
   small_doc_init (small_doc_test);

   return (perf_test_t *) small_doc_test;
}


/*
 *  -------- LARGE-DOC BENCHMARK ---------------------------------------------
 */

typedef single_doc_test_t large_doc_test_t;

static void
large_doc_task (perf_test_t *test)
{
   _single_doc_task (test, 10);
}

static void
large_doc_init (large_doc_test_t *large_doc_test)
{
   single_doc_init (large_doc_test,
                    "TestLargeDocInsertOne",
                    "single_and_multi_document/large_doc.json",
                    27310890);
   large_doc_test->base.base.task = large_doc_task;
}

static perf_test_t *
large_doc_new (void)
{
   large_doc_test_t *large_doc_test;

   large_doc_test =
      (large_doc_test_t *) bson_malloc0 (sizeof (large_doc_test_t));
   large_doc_init (large_doc_test);

   return (perf_test_t *) large_doc_test;
}


/*
 *  -------- FIND-MANY BENCHMARK ---------------------------------------------
 */

typedef single_doc_test_t find_many_test_t;

static void
find_many_setup (perf_test_t *test)
{
   single_doc_test_t *driver_test;
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   int i;

   single_doc_setup (test);

   driver_test = (single_doc_test_t *) test;
   bulk = mongoc_collection_create_bulk_operation (driver_test->base.collection,
                                                   true, NULL);

   for (i = 0; i < NUM_DOCS; i++) {
      mongoc_bulk_operation_insert (bulk, &driver_test->doc);
   }

   if (!mongoc_bulk_operation_execute (bulk, NULL, &error)) {
      MONGOC_ERROR ("bulk insert: %s\n", error.message);
      abort ();
   }

   mongoc_bulk_operation_destroy (bulk);
}

static void
find_many_task (perf_test_t *test)
{
   single_doc_test_t *driver_test;
   bson_t query = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_error_t error;

   driver_test = (single_doc_test_t *) test;
#if MONGOC_CHECK_VERSION(1, 5, 0)
      cursor = mongoc_collection_find_with_opts (driver_test->base.collection,
                                                 &query, NULL, NULL);
#else
      cursor = mongoc_collection_find (driver_test->base.collection,
                                       MONGOC_QUERY_NONE, 0, 0, 0,
                                       &query, NULL, NULL);
#endif

   while (mongoc_cursor_next (cursor, &doc)) {
   }

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("find_many: %s\n", error.message);
      abort ();
   }

   mongoc_cursor_destroy (cursor);
}

static void
find_many_init (find_many_test_t *find_many_test)
{
   single_doc_init (find_many_test,
                    "TestFindManyAndEmptyCursor",
                    "single_and_multi_document/tweet.json",
                    16220000);
   find_many_test->base.base.setup = find_many_setup;
   find_many_test->base.base.before = perf_test_before;  /* no "before" */
   find_many_test->base.base.task = find_many_task;
}

static perf_test_t *
find_many_new (void)
{
   find_many_test_t *find_many_test;

   find_many_test =
      (find_many_test_t *) bson_malloc0 (sizeof (find_many_test_t));
   find_many_init (find_many_test);

   return (perf_test_t *) find_many_test;
}


/*
 *  -------- BULK-INSERT BENCHMARKS ------------------------------------------
 */

/* base for test_bulk_insert_small_doc / large_doc */
typedef struct {
   single_doc_test_t   base;
   int                 num_docs;
} bulk_insert_test_t;

static void
_bulk_insert_setup (perf_test_t *test,
                    int          num_docs)
{
   bulk_insert_test_t *driver_test;

   single_doc_setup (test);

   driver_test = (bulk_insert_test_t *) test;
   driver_test->num_docs = num_docs;
}

static void
bulk_insert_task (perf_test_t *test)
{
   bulk_insert_test_t *driver_test;
   bson_error_t error;
   uint32_t num_docs;
   mongoc_bulk_operation_t *bulk;
   int i;

   driver_test = (bulk_insert_test_t *) test;
   num_docs = (uint32_t) driver_test->num_docs;

   bulk = mongoc_collection_create_bulk_operation (
      driver_test->base.base.collection, false, NULL);

   for (i = 0; i < num_docs; i++) {
      mongoc_bulk_operation_insert (bulk, &driver_test->base.doc);
   }

   if (!mongoc_bulk_operation_execute (bulk, NULL, &error)) {
      MONGOC_ERROR ("insert_bulk: %s\n", error.message);
      abort ();
   }
}

static void
bulk_insert_init (bulk_insert_test_t *bulk_insert_test,
                  const char         *name,
                  const char         *data_path,
                  int64_t             data_sz)
{
   single_doc_init (&bulk_insert_test->base, name, data_path, data_sz);
   bulk_insert_test->base.base.base.task = bulk_insert_task;
}

static void
bulk_insert_small_doc_setup (perf_test_t *test)
{
   _bulk_insert_setup (test, NUM_DOCS);
}

static void
bulk_insert_small_init (bulk_insert_test_t *bulk_insert_test)
{
   bulk_insert_init (bulk_insert_test,
                     "TestSmallDocBulkInsert",
                     "single_and_multi_document/small_doc.json",
                     2750000);
   bulk_insert_test->base.base.base.setup = bulk_insert_small_doc_setup;
}

static perf_test_t *
bulk_insert_small_new (void)
{
   bulk_insert_test_t *bulk_insert_test;

   bulk_insert_test =
      (bulk_insert_test_t *) bson_malloc0 (sizeof (bulk_insert_test_t));
   bulk_insert_small_init (bulk_insert_test);

   return (perf_test_t *) bulk_insert_test;
}

static void
bulk_insert_large_doc_setup (perf_test_t *test)
{
   _bulk_insert_setup (test, 10);
}

static void
bulk_insert_large_init (bulk_insert_test_t *bulk_insert_test)
{
   bulk_insert_init (bulk_insert_test,
                     "TestLargeDocBulkInsert",
                     "single_and_multi_document/large_doc.json",
                     27310890);
   bulk_insert_test->base.base.base.setup = bulk_insert_large_doc_setup;
}

static perf_test_t *
bulk_insert_large_new (void)
{
   bulk_insert_test_t *bulk_insert_test;

   bulk_insert_test =
      (bulk_insert_test_t *) bson_malloc0 (sizeof (bulk_insert_test_t));
   bulk_insert_large_init (bulk_insert_test);

   return (perf_test_t *) bulk_insert_test;
}

void
driver_perf (void)
{
   perf_test_t *tests[] = {
      run_cmd_new (),
      find_one_new (),
      small_doc_new (),
      large_doc_new (),
      find_many_new (),
      bulk_insert_small_new (),
      bulk_insert_large_new (),
      NULL,
   };

   run_perf_tests (tests);
}
