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

#include <dirent.h>
#include <pthread.h>


/*
 *  -------- GRIDFS MULTI-FILE UPLOAD BENCHMARK -------------------------------
 */

typedef struct _multi_upload_thread_context_t multi_upload_thread_context_t;

typedef struct _multi_upload_thread_context_t {
   char                            *filename;
   char                            *path;
   pthread_t                        thread;
   mongoc_client_t                 *client;
   mongoc_stream_t                 *stream;
   mongoc_gridfs_t                 *gridfs;
} _multi_upload_thread_context_t;

typedef struct {
   perf_test_t                    base;
   mongoc_client_pool_t          *pool;
   int                            cnt;
   multi_upload_thread_context_t *contexts;
} multi_upload_test_t;


static void
multi_upload_setup (perf_test_t *test)
{
   multi_upload_test_t *upload_test;
   multi_upload_thread_context_t *ctx;
   mongoc_uri_t *uri;
   char *data_dir;
   DIR *dirp;
   struct dirent *dp;
   int i;

   perf_test_setup (test);

   upload_test = (multi_upload_test_t *) test;

   uri = mongoc_uri_new (NULL);
   upload_test->pool = mongoc_client_pool_new (uri);

   data_dir = bson_strdup_printf ("performance-testdata/%s", test->data_path);
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

   upload_test->cnt = i;
   upload_test->contexts = (multi_upload_thread_context_t *) bson_malloc0 (
      i * sizeof (multi_upload_thread_context_t));

   rewinddir (dirp);
   i = 0;

   while ((dp = readdir (dirp)) != NULL) {
      if (!strcmp (get_ext (dp->d_name), "txt")) {
         ctx = &upload_test->contexts[i];
         ctx->filename = bson_strdup (dp->d_name);
         ctx->path = bson_strdup_printf (
            "performance-testdata/%s/%s", test->data_path, dp->d_name);

         ++i;
      }
   }

   assert (i == upload_test->cnt);

   closedir (dirp);
   bson_free (data_dir);
   mongoc_uri_destroy (uri);
}


static void
multi_upload_before (perf_test_t *test)
{
   multi_upload_test_t *upload_test;
   mongoc_client_t *client;
   mongoc_gridfs_t *gridfs;
   bson_error_t error;
   mongoc_database_t *db;
   multi_upload_thread_context_t *ctx;
   int i;

   perf_test_before (test);

   upload_test = (multi_upload_test_t *) test;
   client = mongoc_client_pool_pop (upload_test->pool);
   db = mongoc_client_get_database (client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }

   gridfs = mongoc_client_get_gridfs (client, "perftest", NULL, &error);
   if (!gridfs) {
      MONGOC_ERROR ("get_gridfs: %s\n", error.message);
      abort ();
   }

   write_one_byte_file (gridfs);
   mongoc_gridfs_destroy (gridfs);
   mongoc_client_pool_push (upload_test->pool, client);

   for (i = 0; i < upload_test->cnt; i++) {
      ctx = &upload_test->contexts[i];
      ctx->client = mongoc_client_pool_pop (upload_test->pool);
      ctx->gridfs = mongoc_client_get_gridfs (ctx->client, "perftest",
                                              NULL, &error);
      if (!ctx->gridfs) {
         MONGOC_ERROR ("get_gridfs: %s\n", error.message);
         abort ();
      }

      ctx->stream = mongoc_stream_file_new_for_path (ctx->path, O_RDONLY, 0);
      if (!ctx->stream) {
         perror ("stream_new_for_path");
         abort ();
      }
   }

   mongoc_database_destroy (db);
}


static void *
_multi_upload_thread (void *p)
{
   multi_upload_thread_context_t *ctx;
   mongoc_gridfs_file_opt_t opt = { 0 };
   mongoc_gridfs_file_t *file;
   bson_error_t error;

   ctx = (multi_upload_thread_context_t *) p;
   opt.filename = ctx->filename;

   /* upload chunks */
   file = mongoc_gridfs_create_file_from_stream (ctx->gridfs,
                                                 ctx->stream,
                                                 &opt);

   /* create fs.files document */
   if (!mongoc_gridfs_file_save (file)) {
      if (mongoc_gridfs_file_error (file, &error)) {
         MONGOC_ERROR ("file_readv: %s\n", error.message);
      } else {
         MONGOC_ERROR ("file_readv: unknown error\n");
      }

      abort ();
   }

   mongoc_gridfs_file_destroy (file);

   return (void *) 1;
}


static void
multi_upload_task (perf_test_t *test)
{
   multi_upload_test_t *upload_test;
   multi_upload_thread_context_t *ctx;
   int i;
   void *r;

   upload_test = (multi_upload_test_t *) test;

   for (i = 0; i < upload_test->cnt; i++) {
      ctx = &upload_test->contexts[i];
      pthread_create (&ctx->thread, NULL, _multi_upload_thread, (void *) ctx);
   }

   for (i = 0; i < upload_test->cnt; i++) {
      if (pthread_join (upload_test->contexts[i].thread, &r)) {
         perror ("pthread_join");
         abort ();
      }

      if ((int) r != 1) {
         MONGOC_ERROR ("upload_thread returned %d\n", (int) r);
         abort ();
      }
   }
}


static void
multi_upload_after (perf_test_t *test)
{
   multi_upload_test_t *upload_test;
   multi_upload_thread_context_t *ctx;
   int i;

   upload_test = (multi_upload_test_t *) test;
   for (i = 0; i < upload_test->cnt; i++) {
      /* ctx->stream was destroyed by mongoc_gridfs_create_file_from_stream */
      ctx = &upload_test->contexts[i];
      mongoc_gridfs_destroy (ctx->gridfs);
      mongoc_client_pool_push (upload_test->pool, ctx->client);
   }

   perf_test_after (test);
}


static void
multi_upload_teardown (perf_test_t *test)
{
   multi_upload_test_t *upload_test;
   multi_upload_thread_context_t *ctx;
   int i;

   upload_test = (multi_upload_test_t *) test;

   for (i = 0; i < upload_test->cnt; i++) {
      ctx = &upload_test->contexts[i];
      bson_free (ctx->filename);
      bson_free (ctx->path);
   }

   bson_free (upload_test->contexts);
   mongoc_client_pool_destroy (upload_test->pool);

   perf_test_teardown (test);
}


static void
multi_upload_init (multi_upload_test_t *upload_test)
{
   perf_test_init (&upload_test->base,
                   "TestGridFsMultiFileUpload",
                   "PARALLEL/GRIDFS_MULTI");
   upload_test->base.setup = multi_upload_setup;
   upload_test->base.before = multi_upload_before;
   upload_test->base.task = multi_upload_task;
   upload_test->base.after = multi_upload_after;
   upload_test->base.teardown = multi_upload_teardown;
}


static perf_test_t *
multi_upload_perf_new (void)
{
   multi_upload_test_t *upload_test;

   upload_test = (multi_upload_test_t *) bson_malloc0 (
      sizeof (multi_upload_test_t));
   multi_upload_init (upload_test);

   return (perf_test_t *) upload_test;
}


void
gridfs_parallel_perf (void)
{
   perf_test_t *tests[] = {
      multi_upload_perf_new (),
      NULL,
   };

   run_perf_tests (tests);
}
