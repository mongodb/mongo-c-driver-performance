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
#include <bson/bson.h>
#include <mongoc/mongoc.h>


/*
 *  -------- BENCHMARK BASE CODE ----------------------------------------------
 */

typedef struct {
   perf_test_t base;
   mongoc_client_t *client;
   mongoc_gridfs_t *gridfs;
   char *data;
   size_t data_sz;
} gridfs_test_t;


static void
_init_gridfs (gridfs_test_t *gridfs_test)
{
   bson_error_t error;

   /* ensures indexes */
   gridfs_test->gridfs =
      mongoc_client_get_gridfs (gridfs_test->client, "perftest", NULL, &error);

   if (!gridfs_test->gridfs) {
      MONGOC_ERROR ("get_gridfs: %s\n", error.message);
      abort ();
   }
}

static void
_drop_database (gridfs_test_t *gridfs_test)
{
   mongoc_database_t *db;
   bson_error_t error;

   db = mongoc_client_get_database (gridfs_test->client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }

   mongoc_database_destroy (db);
}

static void
_upload_big_file (gridfs_test_t *gridfs_test, bson_oid_t *oid /* OUT */)
{
   mongoc_gridfs_file_t *file;
   mongoc_iovec_t iov;
   bson_error_t error;
   const bson_value_t *value;

   file = mongoc_gridfs_create_file (gridfs_test->gridfs, NULL);

   iov.iov_base = (void *) gridfs_test->data;
   iov.iov_len = gridfs_test->data_sz;
   if (iov.iov_len != mongoc_gridfs_file_writev (file, &iov, 1, 0)) {
      if (mongoc_gridfs_file_error (file, &error)) {
         MONGOC_ERROR ("file_writev: %s\n", error.message);
      } else {
         MONGOC_ERROR ("file_writev: unknown error\n");
      }

      abort ();
   }

   mongoc_gridfs_file_save (file);
   value = mongoc_gridfs_file_get_id (file);
   assert (value->value_type == BSON_TYPE_OID);
   bson_oid_copy (&value->value.v_oid, oid);

   mongoc_gridfs_file_destroy (file);
}

static void
gridfs_setup (perf_test_t *test)
{
   char *path;
   gridfs_test_t *gridfs_test;
   FILE *fp;

   perf_test_setup (test);

   gridfs_test = (gridfs_test_t *) test;
   gridfs_test->client = mongoc_client_new (NULL);

   path = bson_strdup_printf ("%s/single_and_multi_document/gridfs_large.bin",
                              g_test_dir);
   fp = fopen (path, "rb");
   if (!fp) {
      perror ("cannot open GRIDFS_LARGE");
      abort ();
   }

   /* get the file size */
   if (0 != fseek (fp, 0L, SEEK_END)) {
      perror ("fseek");
      abort ();
   }

   gridfs_test->data_sz = (size_t) ftell (fp);
   fseek (fp, 0L, SEEK_SET);

   /* read the file */
   gridfs_test->data = bson_malloc (gridfs_test->data_sz);
   assert (gridfs_test->data);
   assert (gridfs_test->data_sz ==
           fread (gridfs_test->data, 1, gridfs_test->data_sz, fp));

   bson_free (path);
}

static void
gridfs_teardown (perf_test_t *test)
{
   gridfs_test_t *gridfs_test;

   gridfs_test = (gridfs_test_t *) test;
   bson_free (gridfs_test->data);
   mongoc_gridfs_destroy (gridfs_test->gridfs);
   mongoc_client_destroy (gridfs_test->client);

   perf_test_teardown (test);
}

static void
gridfs_init (gridfs_test_t *gridfs_test, const char *name, int64_t data_sz)
{
   perf_test_init (&gridfs_test->base, name, NULL, data_sz);

   gridfs_test->base.setup = gridfs_setup;
   gridfs_test->base.teardown = gridfs_teardown;
}

/*
 *  -------- UPLOAD BENCHMARK ------------------------------------------------
 */

typedef gridfs_test_t upload_test_t;

static void
upload_before (perf_test_t *test)
{
   upload_test_t *gridfs_test;

   perf_test_before (test);

   gridfs_test = (upload_test_t *) test;
   _drop_database (gridfs_test);
   _init_gridfs (gridfs_test);
   write_one_byte_file (gridfs_test->gridfs);
}

static void
upload_task (perf_test_t *test)
{
   bson_oid_t oid;

   _upload_big_file ((upload_test_t *) test, &oid);
}

static void
upload_init (upload_test_t *upload_test)
{
   gridfs_init (upload_test, "TestGridFsUpload", 52428800);
   upload_test->base.before = upload_before;
   upload_test->base.task = upload_task;
}

static perf_test_t *
upload_perf_new (void)
{
   upload_test_t *upload_test;

   upload_test = (upload_test_t *) bson_malloc0 (sizeof (upload_test_t));
   upload_init (upload_test);

   return (perf_test_t *) upload_test;
}


/*
 *  -------- DOWNLOAD BENCHMARK ----------------------------------------------
 */

typedef struct {
   gridfs_test_t base;
   bson_oid_t file_id;
} download_test_t;

static void
download_setup (perf_test_t *test)
{
   download_test_t *gridfs_test;

   gridfs_setup (test);

   gridfs_test = (download_test_t *) test;
   _drop_database (&gridfs_test->base);
   _init_gridfs (&gridfs_test->base);

   /* save the file's _id */
   _upload_big_file (&gridfs_test->base, &gridfs_test->file_id);
}

static void
download_task (perf_test_t *test)
{
   download_test_t *gridfs_test;
   bson_t query = BSON_INITIALIZER;
   mongoc_gridfs_file_t *file;
   mongoc_iovec_t iov;
   bson_error_t error;
   ssize_t read_sz;

   gridfs_test = (download_test_t *) test;

   bson_append_oid (&query, "_id", 3, &gridfs_test->file_id);
#if MONGOC_CHECK_VERSION(1, 5, 0)
   file = mongoc_gridfs_find_one_with_opts (
      gridfs_test->base.gridfs, &query, NULL, &error);
#else
   file = mongoc_gridfs_find_one (gridfs_test->base.gridfs, &query, &error);
#endif
   if (!file) {
      MONGOC_ERROR ("gridfs_find_one: %s\n", error.message);
      abort ();
   }

   /* overwrite the buffer we used for _upload_big_file */
   iov.iov_base = (void *) gridfs_test->base.data;
   iov.iov_len = gridfs_test->base.data_sz;
   read_sz =
      mongoc_gridfs_file_readv (file, &iov, 1, gridfs_test->base.data_sz, 0);

   if (read_sz != gridfs_test->base.data_sz) {
      if (mongoc_gridfs_file_error (file, &error)) {
         MONGOC_ERROR ("file_readv: %s\n", error.message);
      } else {
         MONGOC_ERROR ("file_readv: unknown error\n");
      }

      abort ();
   }

   mongoc_gridfs_file_destroy (file);
   bson_destroy (&query);
}

static void
download_init (download_test_t *download_test)
{
   gridfs_init (&download_test->base, "TestGridFsDownload", 52428800);
   download_test->base.base.setup = download_setup;
   download_test->base.base.task = download_task;
}

static perf_test_t *
download_perf_new (void)
{
   download_test_t *download_test;

   download_test = (download_test_t *) bson_malloc0 (sizeof (download_test_t));
   download_init (download_test);

   return (perf_test_t *) download_test;
}

void
gridfs_perf (void)
{
   perf_test_t *tests[] = {
      upload_perf_new (),
      download_perf_new (),
      NULL,
   };

   run_perf_tests (tests);
}
