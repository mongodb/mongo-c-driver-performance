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
#include <bson-types.h>


typedef struct {
   mongoc_client_t     *client;
   mongoc_gridfs_t     *gridfs;
   char                *data;
   size_t               data_sz;
} gridfs_test_t;


static void
init_gridfs (gridfs_test_t *context)
{
   bson_error_t error;

   /* ensures indexes */
   context->gridfs = mongoc_client_get_gridfs (context->client, "perftest",
                                               NULL, &error);

   if (!context->gridfs) {
      MONGOC_ERROR ("get_gridfs: %s\n", error.message);
      abort ();
   }
}

static void
drop_database (gridfs_test_t *context)
{
   mongoc_database_t *db;
   bson_error_t error;

   db = mongoc_client_get_database (context->client, "perftest");
   if (!mongoc_database_drop (db, &error)) {
      MONGOC_ERROR ("database_drop: %s\n", error.message);
      abort ();
   }

   mongoc_database_destroy (db);
}

static void
upload_big_file (gridfs_test_t *context,
                 bson_oid_t    *oid /* OUT */)
{
   mongoc_gridfs_file_t *file;
   mongoc_iovec_t iov;
   bson_error_t error;
   const bson_value_t *value;

   file = mongoc_gridfs_create_file (context->gridfs, NULL);

   iov.iov_base = (void *) context->data;
   iov.iov_len = context->data_sz;
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
   gridfs_test_t *context;
   FILE *fp;

   context = (gridfs_test_t *) test->context;
   context->client = mongoc_client_new (NULL);

   fp = fopen ("performance-testdata/SINGLE_DOCUMENT/GRIDFS_LARGE", "rb");
   if (!fp) {
      perror ("cannot open GRIDFS_LARGE");
      abort ();
   }

   /* get the file size */
   if (0 != fseek (fp, 0L, SEEK_END)) {
      perror ("fseek");
      abort();
   }

   context->data_sz = (size_t) ftell (fp);
   fseek(fp, 0L, SEEK_SET);

   /* read the file */
   context->data = bson_malloc (context->data_sz);
   assert (context->data);
   assert (context->data_sz == fread (context->data, 1, context->data_sz, fp));
}

static void
gridfs_teardown (perf_test_t *test)
{
   gridfs_test_t *context;

   context = (gridfs_test_t *) test->context;
   bson_free (context->data);
   mongoc_gridfs_destroy (context->gridfs);
   mongoc_client_destroy (context->client);
}

typedef gridfs_test_t upload_test_t;

#define upload_setup gridfs_setup

static void
upload_before (perf_test_t *test)
{
   upload_test_t *context;
   mongoc_gridfs_file_t *file;
   char c = '\0';
   mongoc_iovec_t iov;
   bson_error_t error;

   context = (upload_test_t *) test->context;
   drop_database (context);
   init_gridfs (context);

   /* write 1-byte file */
   file = mongoc_gridfs_create_file (context->gridfs, NULL);
   iov.iov_base = (void *) &c;
   iov.iov_len = 1;
   if (1 != mongoc_gridfs_file_writev (file, &iov, 1, 0)) {
      if (mongoc_gridfs_file_error (file, &error)) {
         MONGOC_ERROR ("file_writev: %s\n", error.message);
      } else {
         MONGOC_ERROR ("file_writev: unknown error\n");
      }

      abort ();
   }

   mongoc_gridfs_file_save (file);
   mongoc_gridfs_file_destroy (file);
}

static void
upload_task (perf_test_t *test)
{
   bson_oid_t oid;

   upload_big_file ((upload_test_t *) test->context, &oid);
}

#define upload_after NULL
#define upload_teardown gridfs_teardown

typedef struct {
   gridfs_test_t base;
   bson_oid_t    file_id;
} download_test_t;

static void
download_setup (perf_test_t *test)
{
   download_test_t *context;

   context = (download_test_t *) test->context;
   gridfs_setup (test);
   drop_database (&context->base);
   init_gridfs (&context->base);

   /* save the file's _id */
   upload_big_file (&context->base, &context->file_id);
}

#define download_before NULL

static void
download_task (perf_test_t *test)
{
   download_test_t *context;
   bson_t query = BSON_INITIALIZER;
   mongoc_gridfs_file_t *file;
   mongoc_iovec_t iov;
   bson_error_t error;
   ssize_t read_sz;

   context = (download_test_t *) test->context;

   bson_append_oid (&query, "_id", 3, &context->file_id);
   file = mongoc_gridfs_find_one (context->base.gridfs, &query, &error);
   if (!file) {
      MONGOC_ERROR ("gridfs_find_one: %s\n", error.message);
      abort ();
   }

   /* overwrite the buffer we used for upload_big_file */
   iov.iov_base = (void *) context->base.data;
   iov.iov_len = context->base.data_sz;
   read_sz = mongoc_gridfs_file_readv (file, &iov, 1, context->base.data_sz, 0);
   if (read_sz != context->base.data_sz) {
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

#define download_after NULL
#define download_teardown gridfs_teardown

#define GRIDFS_TEST(prefix, name) \
   { sizeof (prefix ## _test_t), #name, NULL, \
     prefix ## _setup, prefix ## _before, prefix ## _task, prefix ## _after, \
     prefix ## _teardown }

void
gridfs_perf (void)
{
   perf_test_t tests[] = {
      GRIDFS_TEST (upload, TestGridFsUpload),
      GRIDFS_TEST (download, TestGridFsDownload),
      { 0 },
   };

   run_perf_tests (tests);
}
