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
 * The task definition is not part of the "MongoDB Driver Performance
 * Benchmarking" specification. */

#include "mongo-c-performance.h"

#include <bson.h>
#include <mongoc.h>

#include <pthread.h>

typedef struct {
   pthread_t thread;
   mongoc_client_t *client;
   int n_operations_to_run;
} parallel_pool_thread_context_t;

typedef struct {
   perf_test_t base;
   mongoc_client_pool_t *pool;
   int n_threads;
   parallel_pool_thread_context_t *contexts;
} parallel_pool_perf_test_t;

/* FINDONE_FILTER_SIZE is the size of the BSON document {"id": 0}.
 * Determined with this Python script:
 *    import bson
 *    print (len(bson.encode({"id": 0}))) # prints "13"
 */
#define FINDONE_FILTER_SIZE 13
/* FINDONE_COUNT is the total number of "find" operations done by all threads.
 * Each thread runs FINDONE_COUNT / n_threads "find" operations. */
#define FINDONE_COUNT 10000

/* MONGOC_DEFAULT_MAX_POOL_SIZE is the default number of clients that can be
 * popped at one time in a mongoc_client_pool_t */
#define MONGOC_DEFAULT_MAX_POOL_SIZE 100

typedef struct {
   pthread_t thread;
   mongoc_client_t *client;
   int n_operations_to_run;
} parallel_single_thread_context_t;
typedef struct {
   perf_test_t base;
   mongoc_client_t *clients[MONGOC_DEFAULT_MAX_POOL_SIZE];
   int n_threads;
   parallel_single_thread_context_t *contexts;
} parallel_single_perf_test_t;

static void
parallel_pool_perf_setup (perf_test_t *test)
{
   mongoc_uri_t *uri;
   parallel_pool_perf_test_t *parallel_pool_test =
      (parallel_pool_perf_test_t *) test;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_database_t *db;
   bson_error_t error;
   int i;
   mongoc_client_t *clients[MONGOC_DEFAULT_MAX_POOL_SIZE];

   uri = mongoc_uri_new (NULL);
   pool = mongoc_client_pool_new (uri);
   parallel_pool_test->pool = pool;
   parallel_pool_test->contexts =
      (parallel_pool_thread_context_t *) bson_malloc0 (
         parallel_pool_test->n_threads *
         sizeof (parallel_pool_thread_context_t));

   client = mongoc_client_pool_pop (parallel_pool_test->pool);
   db = mongoc_client_get_database (client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }
   mongoc_database_destroy (db);
   mongoc_client_pool_push (pool, client);
   /* Warm up each connection by popping all clients and sending one ping. */
   for (i = 0; i < MONGOC_DEFAULT_MAX_POOL_SIZE; i++) {
      bson_t *cmd = BCON_NEW ("ping", BCON_INT32 (1));

      clients[i] = mongoc_client_pool_pop (pool);
      if (!mongoc_client_command_simple (clients[i],
                                         "db",
                                         cmd,
                                         NULL /* read prefs */,
                                         NULL /* reply */,
                                         &error)) {
         MONGOC_ERROR ("client_command_simple error: %s", error.message);
         abort ();
      }
      bson_destroy (cmd);
   }
   for (i = 0; i < MONGOC_DEFAULT_MAX_POOL_SIZE; i++) {
      mongoc_client_pool_push (pool, clients[i]);
   }
   mongoc_uri_destroy (uri);
}

static void
parallel_pool_perf_teardown (perf_test_t *test)
{
   parallel_pool_perf_test_t *parallel_pool_test =
      (parallel_pool_perf_test_t *) test;

   mongoc_client_pool_destroy (parallel_pool_test->pool);
   bson_free (parallel_pool_test->contexts);
}

static void
parallel_pool_perf_before (perf_test_t *test)
{
   parallel_pool_perf_test_t *parallel_pool_test =
      (parallel_pool_perf_test_t *) test;
   int i;

   // if (FINDONE_COUNT % findone_parallel_test->n_threads != 0) {
   //    MONGOC_ERROR (
   //       "FINDONE_COUNT (%d) is not divisible by number of threads: %d",
   //       FINDONE_COUNT,
   //       findone_parallel_test->n_threads);
   //    MONGOC_ERROR ("Consider revising test or using integer division.");
   //    abort ();
   // }

   for (i = 0; i < parallel_pool_test->n_threads; i++) {
      parallel_pool_test->contexts[i].client =
         mongoc_client_pool_pop (parallel_pool_test->pool);
      parallel_pool_test->contexts[i].n_operations_to_run = FINDONE_COUNT;
   }
}

static void
parallel_pool_perf_after (perf_test_t *test)
{
   parallel_pool_perf_test_t *parallel_pool_test =
      (parallel_pool_perf_test_t *) test;
   int i;

   if (parallel_pool_test->n_threads > 100) {
      /* TODO should this check be moved? */
      MONGOC_ERROR ("Error: trying to start test with %d threads.",
                    parallel_pool_test->n_threads);
      MONGOC_ERROR ("Cannot start test with n_threads > 100.");
      MONGOC_ERROR ("libmongoc uses a default maxPoolSize of 100. Cannot pop "
                    "more than 100.");
      MONGOC_ERROR ("Consider revising this test to use a larger pool size.");
      abort ();
   }

   for (i = 0; i < parallel_pool_test->n_threads; i++) {
      mongoc_client_pool_push (parallel_pool_test->pool,
                               parallel_pool_test->contexts[i].client);
   }
}

static void *
_ping_parallel_perf_thread (void *p)
{
   parallel_pool_thread_context_t *ctx =
      (parallel_pool_thread_context_t *) p;
   int i;
   bson_t cmd = BSON_INITIALIZER;

   bson_append_int32 (&cmd, "ping", 4, 1);

   for (i = 0; i < ctx->n_operations_to_run; i++) {
      bson_error_t error;

      if (!mongoc_client_command_simple (ctx->client, "db", &cmd, NULL /* read prefs */, NULL /* reply */, &error)) {
         MONGOC_ERROR ("Error from ping: %s", error.message);
         abort ();
      }
   }

   bson_destroy (&cmd);
   return NULL;
}

static void
ping_parallel_perf_task (perf_test_t *test)
{
   parallel_pool_perf_test_t *parallel_pool_test =
      (parallel_pool_perf_test_t *) test;
   int i;
   int ret;

   for (i = 0; i < parallel_pool_test->n_threads; i++) {
      parallel_pool_thread_context_t *ctx;

      ctx = &parallel_pool_test->contexts[i];
      ret = pthread_create (
         &ctx->thread, NULL /* attr */, _ping_parallel_perf_thread, ctx);
      if (ret != 0) {
         MONGOC_ERROR ("Error: pthread_create returned %d", ret);
         abort ();
      }
   }

   for (i = 0; i < parallel_pool_test->n_threads; i++) {
      parallel_pool_thread_context_t *ctx;

      ctx = &parallel_pool_test->contexts[i];
      ret = pthread_join (ctx->thread, NULL /* out */);
      if (ret != 0) {
         MONGOC_ERROR ("Error: pthread_join returned %d", ret);
         abort ();
      }
   }
}

static perf_test_t *
ping_parallel_perf_new (const char *name, int n_threads)
{
   parallel_pool_perf_test_t *parallel_pool_test =
      bson_malloc0 (sizeof (parallel_pool_perf_test_t));
   perf_test_t *test = (perf_test_t *) parallel_pool_test;
   int64_t data_size;

   parallel_pool_test->n_threads = n_threads;
   data_size = PING_COMMAND_SIZE * FINDONE_COUNT * n_threads;

   perf_test_init (test, name, NULL /* data path */, data_size);
   test->task = ping_parallel_perf_task;
   test->setup = parallel_pool_perf_setup;
   test->teardown = parallel_pool_perf_teardown;
   test->before = parallel_pool_perf_before;
   test->after = parallel_pool_perf_after;
   return test;
}

static void *
_parallel_single_perf_thread (void *p)
{
   parallel_single_thread_context_t *ctx =
      (parallel_single_thread_context_t *) p;
   int i;
   bson_t cmd = BSON_INITIALIZER;

   bson_append_int32 (&cmd, "ping", 4, 1);

   for (i = 0; i < ctx->n_operations_to_run; i++) {
      bson_error_t error;

      if (!mongoc_client_command_simple (ctx->client, "db", &cmd, NULL /* read prefs */, NULL /* reply */, &error)) {
         MONGOC_ERROR ("Error from ping: %s", error.message);
         abort ();
      }
   }

   bson_destroy (&cmd);
   return NULL;
}

static void
parallel_single_perf_task (perf_test_t *test)
{
   parallel_single_perf_test_t *parallel_single_test =
      (parallel_single_perf_test_t *) test;
   int i;
   int ret;

   for (i = 0; i < parallel_single_test->n_threads; i++) {
      parallel_single_thread_context_t *ctx;

      ctx = &parallel_single_test->contexts[i];
      ret = pthread_create (
         &ctx->thread, NULL /* attr */, _parallel_single_perf_thread, ctx);
      if (ret != 0) {
         MONGOC_ERROR ("Error: pthread_create returned %d", ret);
         abort ();
      }
   }

   for (i = 0; i < parallel_single_test->n_threads; i++) {
      parallel_single_thread_context_t *ctx;

      ctx = &parallel_single_test->contexts[i];
      ret = pthread_join (ctx->thread, NULL /* out */);
      if (ret != 0) {
         MONGOC_ERROR ("Error: pthread_join returned %d", ret);
         abort ();
      }
   }
}

static void
parallel_single_perf_setup (perf_test_t *test)
{
   mongoc_uri_t *uri;
   parallel_single_perf_test_t *parallel_single_test =
      (parallel_single_perf_test_t *) test;
   mongoc_client_t *client;
   mongoc_database_t *db;
   bson_error_t error;
   int i;

   uri = mongoc_uri_new (NULL);
   for (i = 0; i < MONGOC_DEFAULT_MAX_POOL_SIZE; i++) {
      parallel_single_test->clients[i] = mongoc_client_new_from_uri (uri);
   }
   parallel_single_test->contexts =
      (parallel_single_thread_context_t *) bson_malloc0 (
         parallel_single_test->n_threads *
         sizeof (parallel_single_thread_context_t));

   client = parallel_single_test->clients[0];
   db = mongoc_client_get_database (client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }
   mongoc_database_destroy (db);
   /* Warm up each connection by popping all clients and sending one ping. */
   for (i = 0; i < MONGOC_DEFAULT_MAX_POOL_SIZE; i++) {
      bson_t *cmd = BCON_NEW ("ping", BCON_INT32 (1));

      client = parallel_single_test->clients[i];
      if (!mongoc_client_command_simple (client,
                                         "db",
                                         cmd,
                                         NULL /* read prefs */,
                                         NULL /* reply */,
                                         &error)) {
         MONGOC_ERROR ("client_command_simple error: %s", error.message);
         abort ();
      }
      bson_destroy (cmd);
   }
   mongoc_uri_destroy (uri);
}

static void
parallel_single_perf_teardown (perf_test_t *test)
{
   int i;
   parallel_single_perf_test_t *parallel_single_test =
      (parallel_single_perf_test_t *) test;

   for (i = 0; i < MONGOC_DEFAULT_MAX_POOL_SIZE; i++) {
      mongoc_client_destroy (parallel_single_test->clients[i]);
   }
   bson_free (parallel_single_test->contexts);
}

static void
parallel_single_perf_before (perf_test_t *test)
{
   parallel_single_perf_test_t *parallel_single_test =
      (parallel_single_perf_test_t *) test;
   int i;

   // if (FINDONE_COUNT % parallel_single_test->n_threads != 0) {
   //    MONGOC_ERROR (
   //       "FINDONE_COUNT (%d) is not divisible by number of threads: %d",
   //       FINDONE_COUNT,
   //       parallel_single_test->n_threads);
   //    MONGOC_ERROR ("Consider revising test or using integer division.");
   //    abort ();
   // }

   for (i = 0; i < parallel_single_test->n_threads; i++) {
      parallel_single_test->contexts[i].client = parallel_single_test->clients[i];
      parallel_single_test->contexts[i].n_operations_to_run = FINDONE_COUNT;
   }
}

static void
parallel_single_perf_after (perf_test_t *test)
{
   parallel_single_perf_test_t *parallel_single_test =
      (parallel_single_perf_test_t *) test;
   int i;

   if (parallel_single_test->n_threads > 100) {
      MONGOC_ERROR ("Error: trying to start test with %d threads.",
                    parallel_single_test->n_threads);
      MONGOC_ERROR ("Cannot start test with n_threads > 100.");
      MONGOC_ERROR ("libmongoc uses a default maxPoolSize of 100. Cannot pop "
                    "more than 100.");
      MONGOC_ERROR ("Consider revising this test to use a larger pool size.");
      abort ();
   }
}

static perf_test_t *
parallel_single_perf_new (const char *name, int n_threads)
{
   parallel_single_perf_test_t *parallel_single_test =
      bson_malloc0 (sizeof (parallel_single_perf_test_t));
   perf_test_t *test = (perf_test_t *) parallel_single_test;
   int64_t data_size;

   parallel_single_test->n_threads = n_threads;
   data_size = FINDONE_FILTER_SIZE * FINDONE_COUNT * n_threads;

   perf_test_init (test, name, NULL /* data path */, data_size);
   test->task = parallel_single_perf_task;
   test->setup = parallel_single_perf_setup;
   test->teardown = parallel_single_perf_teardown;
   test->before = parallel_single_perf_before;
   test->after = parallel_single_perf_after;
   return test;
}

void
findone_parallel_perf (void)
{
   perf_test_t *perf_tests[] = {
      ping_parallel_perf_new ("PingParallel1Threads", 1),
      ping_parallel_perf_new ("PingParallel10Threads", 10),
      ping_parallel_perf_new ("PingParallel100Threads", 100),
      parallel_single_perf_new ("PingParallelSingle1Threads", 1),
      parallel_single_perf_new ("PingParallelSingle10Threads", 10),
      parallel_single_perf_new ("PingParallelSingle100Threads", 100),
      NULL};

   run_perf_tests (perf_tests);
}
