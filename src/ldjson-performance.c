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
   char                 **filenames;
   char                 **paths;
   pthread_t             *threads;
   bool                   add_file_id;
} import_test_t;


typedef struct {
   import_test_t *test;
   int            offset;
} import_thread_context_t;


static void
import_setup (perf_test_t *test)
{
   import_test_t *import_test;
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_database_t *db;
   bson_error_t error;
   char *data_dir;
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

   data_dir = bson_strdup_printf ("%s/%s", g_test_dir, test->data_path);
   dirp = opendir(data_dir);
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
   import_test->filenames = (char **) bson_malloc0 (i * sizeof (char *));
   import_test->paths = (char **) bson_malloc0 (i * sizeof (char *));
   rewinddir (dirp);
   i = 0;

   while ((dp = readdir (dirp)) != NULL) {
      if (!strcmp (get_ext (dp->d_name), "txt")) {
         import_test->filenames[i] = bson_strdup (dp->d_name);
         import_test->paths[i] = bson_strdup_printf (
            "%s/%s/%s", g_test_dir, test->data_path, dp->d_name);

         ++i;
      }
   }

   assert (i == import_test->cnt);

   closedir (dirp);
   mongoc_database_destroy (db);
   mongoc_client_pool_push (import_test->pool, client);
   bson_free (data_dir);
   mongoc_uri_destroy (uri);
}


static void
import_before (perf_test_t *test)
{
   import_test_t *import_test;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t cmd = BSON_INITIALIZER;
   bson_t index_keys = BSON_INITIALIZER;
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

   if (import_test->add_file_id) {
      BSON_APPEND_INT32 (&index_keys, "file", 1);
      if (!mongoc_collection_create_index (collection, &index_keys, NULL, &error)) {
         MONGOC_ERROR ("create_index: %s\n", error.message);
         abort ();
      }
   }

   import_test->threads = bson_malloc (import_test->cnt * sizeof (pthread_t));

   bson_destroy (&index_keys);
   bson_destroy (&cmd);
   mongoc_collection_destroy (collection);
   mongoc_client_pool_push (import_test->pool, client);
}


static void *
_import_thread (void *p)
{
   import_thread_context_t *ctx;
   bool add_file_id;
   bson_error_t error;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_bulk_operation_t *bulk;
   const char *path;
   bson_json_reader_t *reader;
   int r;
   bson_t bson = BSON_INITIALIZER;

   ctx = (import_thread_context_t *) p;
   add_file_id = ctx->test->add_file_id;
   client = mongoc_client_pool_pop (ctx->test->pool);
   collection = mongoc_client_get_collection (client, "perftest", "corpus");
   bulk = mongoc_collection_create_bulk_operation (collection, false, NULL);

   path = ctx->test->paths[ctx->offset];
   reader = bson_json_reader_new_from_file (path, &error);
   if (!reader) {
      MONGOC_ERROR ("%s\n", error.message);
      abort ();
   }

   while ((r = bson_json_reader_read (reader, &bson, &error))) {
      if (r < 0) {
         MONGOC_ERROR ("reader_read: %s\n", error.message);
         abort ();
      }

      if (add_file_id) {
         BSON_APPEND_UTF8 (&bson, "file", ctx->test->filenames[ctx->offset]);
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

      if ((intptr_t) r != 1) {
         MONGOC_ERROR ("import_thread returned %p\n", r);
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
      bson_free (import_test->filenames[i]);
      bson_free (import_test->paths[i]);
   }

   bson_free (import_test->filenames);
   bson_free (import_test->paths);
   mongoc_client_pool_destroy (import_test->pool);


   perf_test_teardown (test);
}


static void
import_init (import_test_t *import_test)
{
   perf_test_init (&import_test->base,
                   "TestJsonMultiImport",
                   "parallel/ldjson_multi");
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


/*
 *  -------- LDJSON MULTI-FILE EXPORT BENCHMARK -------------------------------
 */

typedef struct {
   perf_test_t            base;
   mongoc_client_pool_t  *pool;
   int                    cnt;
   pthread_t             *threads;
} export_test_t;


typedef struct {
   export_test_t *test;
   int            offset;
} export_thread_context_t;


static void 
_setup_load_docs (void)
{
   perf_test_t *import;

   /* insert all the documents, with a "file" field */
   import = import_perf_new ();
   ((import_test_t *) import)->add_file_id = true;
   run_test_as_utility (import);
   bson_free (import);
}


static void
export_setup (perf_test_t *test)
{

   export_test_t *export_test;
   mongoc_uri_t *uri;

   _setup_load_docs ();
   perf_test_setup (test);

   export_test = (export_test_t *) test;
   uri = mongoc_uri_new (NULL);
   export_test->pool = mongoc_client_pool_new (uri);

   mongoc_uri_destroy (uri);
}


static void
export_before (perf_test_t *test)
{
   export_test_t *export_test;

   prep_tmp_dir (test->data_path);
   perf_test_before (test);

   export_test = (export_test_t *) test;
   export_test->cnt = 100;  /* DANGER!: assumes test corpus won't change */
   export_test->threads = bson_malloc (export_test->cnt * sizeof (pthread_t));
}


static void *
_export_thread (void *p)
{
   export_thread_context_t *ctx;
   char filename[PATH_MAX];
   char path[PATH_MAX];
   FILE *fp;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   bson_t query = BSON_INITIALIZER;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   char *json;
   size_t sz;
   size_t total_sz;
   bson_error_t error;

   ctx = (export_thread_context_t *) p;

   /* these filenames are 0-indexed */
   bson_snprintf (filename, PATH_MAX, "ldjson%03d.txt", ctx->offset);
   bson_snprintf (path, PATH_MAX, "/tmp/TestJsonMultiExport/%s", filename);
   fp = fopen (path, "w+");
   if (!fp) {
      perror ("fopen");
      abort ();
   }

   client = mongoc_client_pool_pop (ctx->test->pool);
   collection = mongoc_client_get_collection (client, "perftest", "corpus");
   BSON_APPEND_UTF8 (&query, "file", filename);
   cursor = mongoc_collection_find (collection, MONGOC_QUERY_NONE, 0, 0, 0,
                                    &query, NULL, NULL);

   total_sz = 0;
   while (mongoc_cursor_next (cursor, &doc)) {
      json = bson_as_json (doc, &sz);
      if (fwrite (json, sizeof (char), sz, fp) < sz) {
         perror ("fwrite");
         abort ();
      }

      bson_free (json);
      total_sz += sz;
   }

   if (mongoc_cursor_error (cursor, &error)) {
      MONGOC_ERROR ("cursor error: %s\n", error.message);
      abort ();
   }

   assert (total_sz > 0);

   fclose (fp);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (collection);
   mongoc_client_pool_push (ctx->test->pool, client);

   return (void *) 1;
}


static void
export_task (perf_test_t *test)
{
   export_test_t *export_test;
   export_thread_context_t *contexts;
   int i;
   void *r;

   export_test = (export_test_t *) test;
   contexts = bson_malloc (export_test->cnt * sizeof (export_thread_context_t));

   for (i = 0; i < export_test->cnt; i++) {
      contexts[i].test = export_test;
      contexts[i].offset = i;
      pthread_create (&export_test->threads[i], NULL,
                      _export_thread, (void *) &contexts[i]);
   }

   for (i = 0; i < export_test->cnt; i++) {
      if (pthread_join (export_test->threads[i], &r)) {
         perror ("pthread_join");
         abort ();
      }

      if ((intptr_t) r != 1) {
         MONGOC_ERROR ("export_thread returned %p\n", r);
         abort ();
      }
   }

   bson_free (contexts);
}


static void
export_after (perf_test_t *test)
{
   export_test_t *export_test;

   export_test = (export_test_t *) test;
   bson_free (export_test->threads);
   export_test->threads = NULL;

   perf_test_after (test);
}


static void
export_teardown (perf_test_t *test)
{
   export_test_t *export_test;

   export_test = (export_test_t *) test;
   mongoc_client_pool_destroy (export_test->pool);

   perf_test_teardown (test);
}

static void
export_init (export_test_t *export_test)
{
   perf_test_init (&export_test->base,
                   "TestJsonMultiExport",
                   "/tmp/TestJsonMultiExport");
   export_test->base.setup = export_setup;
   export_test->base.before = export_before;
   export_test->base.task = export_task;
   export_test->base.after = export_after;
   export_test->base.teardown = export_teardown;
}


static perf_test_t *
export_perf_new (void)
{
   export_test_t *export_test;

   export_test = (export_test_t *) bson_malloc0 (sizeof (export_test_t));
   export_init (export_test);

   return (perf_test_t *) export_test;
}


void
parallel_perf (void)
{
   perf_test_t *tests[] = {
      import_perf_new (),
      export_perf_new (),
      NULL,
   };

   run_perf_tests (tests);
}
