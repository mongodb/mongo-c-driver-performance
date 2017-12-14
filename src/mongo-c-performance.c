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

#include <bson.h>
#include <mongoc.h>
#include <dirent.h>

#include "mongo-c-performance.h"


const int NUM_ITERATIONS = 100;
const int NUM_DOCS = 10000;
const int MIN_TIME_USEC = 1 * 60 * 1000 * 1000;
const int MAX_TIME_USEC = 5 * 60 * 1000 * 1000;

char *g_test_dir;
static int g_num_tests;
static char **g_test_names;


void
prep_tmp_dir (const char *path)
{
   DIR *dirp;
   struct dirent *dp;
   char filepath[PATH_MAX];
   struct stat sb;

   if (mkdir (path, S_IRWXU) < 0 && errno != EEXIST) {
      perror ("mkdir");
      abort ();
   }

   dirp = opendir (path);
   if (!dirp) {
      perror ("opening data path");
      abort ();
   }

   while ((dp = readdir (dirp)) != NULL) {
      bson_snprintf (filepath, PATH_MAX, "%s/%s", path, dp->d_name);
      if (stat (filepath, &sb) == 0 && S_ISREG (sb.st_mode)) {
         if (remove (filepath)) {
            perror ("remove");
            abort ();
         }
      }
   }

   closedir (dirp);
}


void
parse_args (int argc, char **argv)
{
   if (argc < 2) {
      fprintf (stderr, "USAGE: mongo-c-performance TEST_DIR [TEST_NAME ...]\n");
      exit (1);
   }

   g_test_dir = argv[1];
   g_num_tests = argc - 2;
   g_test_names = g_num_tests ? &argv[2] : NULL;
}


const char *
get_ext (const char *filename)
{
   const char *dot;

   dot = strrchr (filename, '.');
   if (!dot || dot == filename) {
      return "";
   }

   return dot + 1;
}


void
read_json_file (const char *data_path, bson_t *bson)
{
   char *path;
   bson_json_reader_t *reader;
   bson_error_t error;
   int r;

   path = bson_strdup_printf ("%s/%s", g_test_dir, data_path);
   reader = bson_json_reader_new_from_file (path, &error);
   if (!reader) {
      MONGOC_ERROR ("%s: %s\n", path, error.message);
      abort ();
   }

   bson_init (bson);
   r = bson_json_reader_read (reader, bson, &error);
   if (r < 0) {
      MONGOC_ERROR ("%s: %s\n", data_path, error.message);
      abort ();
   }

   if (r == 0) {
      MONGOC_ERROR ("%s: no data\n", data_path);
      abort ();
   }

   bson_json_reader_destroy (reader);
   bson_free (path);
}

void
write_one_byte_file (mongoc_gridfs_t *gridfs)
{
   mongoc_gridfs_file_t *file;
   char c = '\0';
   mongoc_iovec_t iov;
   bson_error_t error;

   /* write 1-byte file */
   file = mongoc_gridfs_create_file (gridfs, NULL);
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

void
run_test_as_utility (perf_test_t *test)
{
   test->setup (test);
   test->before (test);
   test->task (test);
   test->after (test);
   test->teardown (test);
}


/* from "man qsort" */
static int
cmp (const void *a, const void *b)
{
   int64_t arg1 = *(const int64_t *) a;
   int64_t arg2 = *(const int64_t *) b;

   if (arg1 < arg2)
      return -1;
   if (arg1 > arg2)
      return 1;
   return 0;
}


static bool
should_run_test (const char *name)
{
   int i;

   if (!g_test_names) {
      /* run all tests */
      return true;
   }

   for (i = 0; i < g_num_tests; i++) {
      if (!strcmp (g_test_names[i], name)) {
         return true;
      }
   }

   return false;
}


void
perf_test_setup (perf_test_t *test)
{
}


void
perf_test_before (perf_test_t *test)
{
}


void
perf_test_task (perf_test_t *test)
{
}


void
perf_test_after (perf_test_t *test)
{
}


void
perf_test_teardown (perf_test_t *test)
{
}


void
perf_test_init (perf_test_t *test,
                const char *name,
                const char *data_path,
                int64_t data_sz)
{
   test->name = name;
   test->data_path = data_path;
   test->data_sz = data_sz;

   test->setup = perf_test_setup;
   test->before = perf_test_before;
   test->task = perf_test_task;
   test->after = perf_test_after;
   test->teardown = perf_test_teardown;
}


static FILE *output;
static bool is_first_test;

void
open_output (void)
{
   printf ("opening results.json\n");
   output = fopen ("results.json", "w");

   if (!output) {
      perror ("results.json");
      abort ();
   }
}


void
close_output (void)
{
   fclose (output);
}


void
print_header (void)
{
   fprintf (output,
            "{\n"
            "  \"results\": [\n");

   is_first_test = true;
}


static void
print_result (const char *name, double ops_per_sec)
{
   if (!is_first_test) {
      fprintf (output, ",\n");
   }

   is_first_test = false;

   fprintf (output,
            "    {\n"
            "      \"name\": \"%s\",\n"
            "      \"results\": {\n"
            "        \"1\": {\n"
            "          \"ops_per_sec\": %f\n"
            "        }\n"
            "      }\n"
            "    }",
            name,
            ops_per_sec);
}


void
print_footer (void)
{
   fprintf (output,
            "\n"
            "  ]\n"
            "}\n");
}


void
run_perf_tests (perf_test_t **tests)
{
   perf_test_t *test;
   int64_t *results;
   size_t results_sz;
   int test_idx;
   size_t i;
   int64_t task_start;
   int64_t total_time;
   int median_idx;
   double median;

   results_sz = NUM_ITERATIONS;
   results = bson_malloc (results_sz * sizeof (int64_t));

   test_idx = 0;
   while (tests[test_idx]) {
      test = tests[test_idx];
      if (should_run_test (test->name)) {
         printf ("%s\n", test->name);
         test->setup (test);

         /* run at least 1 min, stop at 100 loops or 5 mins, whichever first */
         total_time = 0;
         for (i = 0; total_time < MIN_TIME_USEC ||
                     (i < NUM_ITERATIONS && total_time < MAX_TIME_USEC);
              i++) {
            if (i >= results_sz) {
               results_sz *= 2;
               results = realloc (results, results_sz * sizeof (int64_t));
            }

            test->before (test);

            task_start = bson_get_monotonic_time ();
            test->task (test);
            total_time += results[i] = bson_get_monotonic_time () - task_start;

            test->after (test);
         }

         qsort ((void *) results, i, sizeof (int64_t), cmp);
         median_idx = BSON_MAX (BSON_MIN (0, (int) i / 2 - 1), (int) i - 1);
         median = (double) (results[median_idx]) / 1e6;
         print_result (test->name, test->data_sz / median);

         test->teardown (test);
      }

      bson_free (test);
      test_idx++;
   }

   bson_free (results);
}
