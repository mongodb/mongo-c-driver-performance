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

#include <stdio.h>
#include <bson.h>
#include <mongoc.h>
#include <dirent.h>
#include <pthread.h>


/*
 *  -------- LDJSON MULTI-FILE IMPORT BENCHMARK -------------------------------
 */

typedef struct {
   perf_test_t            base;
   mongoc_client_pool_t  *pool;
   int                    cnt;
   char                 **file_names;
   pthread_t             *threads;
} import_test_t;


typedef struct {
   import_test_t *test;
   int            offset;
} import_thread_context_t;


static const char *
get_ext (const char *filename)
{
   const char *dot;

   dot = strrchr (filename, '.');
   if (!dot || dot == filename) {
      return "";
   }

   return dot + 1;
}


static void
import_setup (perf_test_t *test)
{
   import_test_t *import_test;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_database_t *db;
   bson_error_t error;
   char *path;
   DIR *dirp;
   struct dirent *dp;
   int i;

   perf_test_setup (test);

   import_test = (import_test_t *) test;
   uri = mongoc_uri_new (NULL);
   import_test->pool = mongoc_client_pool_new (uri);

   client = mongoc_client_pool_pop (import_test->pool);
   db = mongoc_client_get_database (client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }

   path = bson_strdup_printf ("performance-testdata/%s", test->data_path);
   dirp = opendir(path);
   if (!dirp) {
      perror ("opening data path");
      abort ();
   }

   i = 0;

   while ((dp = readdir(dirp)) != NULL) {
      if (!strcmp (get_ext (dp->d_name), "txt")) {
         ++i;
      }
   }

   import_test->cnt = i;
   import_test->file_names = (char **) bson_malloc0 (i * sizeof (char *));
   rewinddir (dirp);
   i = 0;

   while ((dp = readdir (dirp)) != NULL) {
      if (!strcmp (get_ext (dp->d_name), "txt")) {
         import_test->file_names[i] = bson_strdup_printf (
            "performance-testdata/%s/%s", test->data_path, dp->d_name);

         ++i;
      }
   }

   assert (i == import_test->cnt);

   closedir (dirp);
   mongoc_database_destroy (db);
   mongoc_client_pool_push (import_test->pool, client);
   bson_free (path);
   mongoc_uri_destroy (uri);
}


static void
import_before (perf_test_t *test)
{
   import_test_t *import_test;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t cmd = BSON_INITIALIZER;
   bson_error_t error;

   perf_test_before (test);

   import_test = (import_test_t *) test;
   client = mongoc_client_pool_pop (import_test->pool);
   collection = mongoc_client_get_collection (client, "perftest", "corpus");

   if (!mongoc_collection_drop (collection, &error) &&
       !strstr (error.message, "ns not found")) {
      MONGOC_ERROR ("drop collection: %s\n", error.message);
      abort ();
   }

   BSON_APPEND_UTF8 (&cmd, "create", "corpus");
   if (!mongoc_collection_command_simple (collection, &cmd, NULL,
                                          NULL, &error)) {
      MONGOC_ERROR ("create collection: %s\n", error.message);
      abort ();
   }

   import_test->threads = bson_malloc (import_test->cnt * sizeof (pthread_t));

   bson_destroy (&cmd);
   mongoc_collection_destroy (collection);
   mongoc_client_pool_push (import_test->pool, client);
}


static void *
_import_thread (void *p)
{
   import_thread_context_t *ctx;
   bson_error_t error;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   bson_json_reader_t *reader;
   int r;
   bson_t bson = BSON_INITIALIZER;

   ctx = (import_thread_context_t *) p;
   client = mongoc_client_pool_pop (ctx->test->pool);
   collection = mongoc_client_get_collection (client, "perftest", "corpus");
   bulk = mongoc_collection_create_bulk_operation (collection, false, NULL);

   reader = bson_json_reader_new_from_file (ctx->test->file_names[ctx->offset],
                                            &error);

   if (!reader) {
      MONGOC_ERROR ("%s\n", error.message);
      abort ();
   }

   while ((r = bson_json_reader_read (reader, &bson, &error))) {
      if (r < 0) {
         MONGOC_ERROR ("reader_read: %s\n", error.message);
         abort ();
      }

      mongoc_bulk_operation_insert (bulk, &bson);
      bson_reinit (&bson);
   }

   if (!mongoc_bulk_operation_execute (bulk, NULL, &error)) {
      MONGOC_ERROR ("bulk_operation_execute: %s\n", error.message);
      abort ();
   }

   bson_destroy (&bson);
   bson_json_reader_destroy (reader);
   mongoc_bulk_operation_destroy (bulk);
   mongoc_collection_destroy (collection);
   mongoc_client_pool_push (ctx->test->pool, client);

   return (void *) 1;
}


static void
import_task (perf_test_t *test)
{
   import_test_t *import_test;
   import_thread_context_t *contexts;
   int i;
   void *r;

   import_test = (import_test_t *) test;
   contexts = bson_malloc (import_test->cnt * sizeof (import_thread_context_t));

   for (i = 0; i < import_test->cnt; i++) {
      contexts[i].test = import_test;
      contexts[i].offset = i;
      pthread_create (&import_test->threads[i], NULL,
                      _import_thread, (void *) &contexts[i]);
   }

   for (i = 0; i < import_test->cnt; i++) {
      if (pthread_join (import_test->threads[i], &r)) {
         perror ("pthread_join");
         abort ();
      }

      if ((int) r != 1) {
         MONGOC_ERROR ("import_thread returned %d\n", (int) r);
         abort ();
      }
   }

   bson_free (contexts);
}


static void
import_after (perf_test_t *test)
{
   import_test_t *import_test;

   import_test = (import_test_t *) test;
   bson_free (import_test->threads);
   import_test->threads = NULL;

   perf_test_after (test);
}


static void
import_teardown (perf_test_t *test)
{
   import_test_t *import_test;
   int i;

   import_test = (import_test_t *) test;

   for (i = 0; i < import_test->cnt; i++) {
      bson_free (import_test->file_names[i]);
   }

   bson_free (import_test->file_names);
   mongoc_client_pool_destroy (import_test->pool);


   perf_test_teardown (test);
}


static void
import_init (import_test_t *import_test)
{
   perf_test_init (&import_test->base,
                   "TestJsonMultiImport",
                   "PARALLEL/LDJSON_MULTI");
   import_test->base.setup = import_setup;
   import_test->base.before = import_before;
   import_test->base.task = import_task;
   import_test->base.after = import_after;
   import_test->base.teardown = import_teardown;
}


static perf_test_t *
import_perf_new (void)
{
   import_test_t *import_test;

   import_test = (import_test_t *) bson_malloc0 (sizeof (import_test_t));
   import_init (import_test);

   return (perf_test_t *) import_test;
}


void
parallel_perf (void)
{
   perf_test_t *tests[] = {
      import_perf_new (),
      NULL,
   };

   run_perf_tests (tests);
}
