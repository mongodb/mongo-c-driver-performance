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
 * The task definition mirrors the workload identified in CDRIVER-4002.
 * The task definition is not part of the "MongoDB Driver Performance Benchmarking" specification. */

#include "mongo-c-performance.h"

#include <bson.h>
#include <mongoc.h>

#include <pthread.h>

typedef struct {
   pthread_t thread;
   mongoc_client_t *client;
} findone_parallel_thread_context_t;

typedef struct {
   perf_test_t base;
   mongoc_client_pool_t *pool;
   int nthreads;
   findone_parallel_thread_context_t *contexts;
} findone_parallel_perf_test_t;

/* FINDONE_FILTER_SIZE is the size of the BSON document {"id": 0}.
 * Determined with this Python script:
 *    import bson
 *    print (len(bson.encode({"id": 0}))) # prints "13"
 */
#define FINDONE_FILTER_SIZE 13
/* FINDONE_COUNT is the number of "find" operations done by one thread in one iteration. */
#define FINDONE_COUNT 10000

static void* _findone_parallel_perf_thread (void* p) {
   findone_parallel_thread_context_t *ctx = (findone_parallel_thread_context_t*) p;
   mongoc_collection_t *coll;
   int i;
   int computation = 1;
   bson_t filter = BSON_INITIALIZER;

   coll = mongoc_client_get_collection (ctx->client, "perftest", "coll");
   bson_append_int32 (&filter, "_id", 3, 0);

   for (i = 0; i < FINDONE_COUNT; i++) {
      mongoc_cursor_t *cursor;
      const bson_t *doc;
      bson_error_t error;

      cursor = mongoc_collection_find_with_opts (coll, &filter, NULL /* opts */, NULL /* read_prefs */);
      /* iterate once to send "find" */
      if (mongoc_cursor_next (cursor, &doc)) {
         MONGOC_ERROR ("Error: unexpected document returned from collection: %s", bson_as_json (doc, NULL));
         abort ();
      }
      if (mongoc_cursor_error (cursor, &error)) {
         MONGOC_ERROR ("Error from cursor iteration: %s", error.message);
         abort ();
      }
      mongoc_cursor_destroy (cursor);
   }

   mongoc_collection_destroy (coll);
   bson_destroy (&filter);
   return NULL;
}

static void findone_parallel_perf_task (perf_test_t *test) {
   findone_parallel_perf_test_t *findone_parallel_test = (findone_parallel_perf_test_t*) test;
   int i;
   int ret;

   for (i = 0; i < findone_parallel_test->nthreads; i++) {
      findone_parallel_thread_context_t* ctx;

      ctx = &findone_parallel_test->contexts[i];
      ret = pthread_create (&ctx->thread, NULL /* attr */, _findone_parallel_perf_thread, ctx);
      if (ret != 0) {
         MONGOC_ERROR ("Error: pthread_create returned %d", ret);
         abort ();
      }
   }

   for (i = 0; i < findone_parallel_test->nthreads; i++) {
      findone_parallel_thread_context_t* ctx;

      ctx = &findone_parallel_test->contexts[i];
      ret = pthread_join (ctx->thread, NULL /* out */);
      if (ret != 0) {
         MONGOC_ERROR ("Error: pthread_join returned %d", ret);
         abort ();
      }
   }
}

static void findone_parallel_perf_setup (perf_test_t *test) {
   mongoc_uri_t *uri;
   findone_parallel_perf_test_t *findone_parallel_test = (findone_parallel_perf_test_t*) test;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_database_t *db;
   bson_error_t error;

   MONGOC_DEBUG ("findone_parallel_perf_setup");

   uri = mongoc_uri_new (NULL);
   pool = mongoc_client_pool_new (uri);
   findone_parallel_test->pool = pool;
   findone_parallel_test->contexts = (findone_parallel_thread_context_t*) bson_malloc0 (findone_parallel_test->nthreads * sizeof (findone_parallel_thread_context_t));

   client = mongoc_client_pool_pop (findone_parallel_test->pool);
   db = mongoc_client_get_database (client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }
   mongoc_database_destroy (db);
   mongoc_client_pool_push (pool, client);
   mongoc_uri_destroy (uri);
}

static void findone_parallel_perf_teardown (perf_test_t *test) {
   findone_parallel_perf_test_t *findone_parallel_test = (findone_parallel_perf_test_t*) test;

   MONGOC_DEBUG ("findone_parallel_perf_teardown");
   mongoc_client_pool_destroy (findone_parallel_test->pool);
   bson_free (findone_parallel_test->contexts);
}

static void
findone_parallel_perf_before (perf_test_t *test)
{
   findone_parallel_perf_test_t *findone_parallel_test =
      (findone_parallel_perf_test_t *) test;
   int i;

   for (i = 0; i < findone_parallel_test->nthreads; i++) {
      findone_parallel_test->contexts[i].client =
         mongoc_client_pool_pop (findone_parallel_test->pool);
   }
}

static void
findone_parallel_perf_after (perf_test_t *test)
{
   findone_parallel_perf_test_t *findone_parallel_test =
      (findone_parallel_perf_test_t *) test;
   int i;

   if (findone_parallel_test->nthreads > 100) {
      MONGOC_ERROR ("Error: trying to start test with %d threads.",
                    findone_parallel_test->nthreads);
      MONGOC_ERROR ("Cannot start test with nthreads > 100.");
      MONGOC_ERROR ("libmongoc uses a default maxPoolSize of 100. Cannot pop "
                    "more than 100.");
      MONGOC_ERROR ("Consider revising this test to use a larger pool size.");
      abort ();
   }

   for (i = 0; i < findone_parallel_test->nthreads; i++) {
      mongoc_client_pool_push (findone_parallel_test->pool,
                               findone_parallel_test->contexts[i].client);
   }
}

static perf_test_t *
findone_parallel_perf_new (const char *name, int nthreads)
{
   findone_parallel_perf_test_t *findone_parallel_test =
      bson_malloc0 (sizeof (findone_parallel_perf_test_t));
   perf_test_t *test = (perf_test_t *) findone_parallel_test;
   int64_t data_size;

   findone_parallel_test->nthreads = nthreads;
   data_size = FINDONE_FILTER_SIZE * nthreads * FINDONE_COUNT;

   perf_test_init (test,
                   name,
                   NULL /* data path */,
                   data_size);
   test->task = findone_parallel_perf_task;
   test->setup = findone_parallel_perf_setup;
   test->teardown = findone_parallel_perf_teardown;
   test->before = findone_parallel_perf_before;
   test->after = findone_parallel_perf_after;
   return test;
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