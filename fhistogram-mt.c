// Setting _DEFAULT_SOURCE is necessary to activate visibility of
// certain header file contents on GNU/Linux systems.
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>

#include "job_queue.h"

pthread_mutex_t stdout_mutex = PTHREAD_MUTEX_INITIALIZER;

// err.h contains various nonstandard BSD extensions, but they are
// very handy.
#include <err.h>

#include "histogram.h"

#define PRINT_INTERVAL 100000

static struct job_queue queue;
static pthread_mutex_t hist_mutex = PTHREAD_MUTEX_INITIALIZER;

static int global_histogram[8] = {0};
static long long bytes_since_last_print = 0;

static void *worker_func(void *arg) {
  (void)arg;

  while (1) {
    void *item = NULL;
    if (job_queue_pop(&queue, &item) != 0) {
      break;
    }

    char *path = (char *)item;

    int local_hist[8] = {0};
    long long local_bytes = 0;

    FILE *f = fopen(path, "rb");
    if (f != NULL) {
      unsigned char buffer[4096];
      size_t bytes_read;
      while ((bytes_read = fread(buffer, 1, sizeof(buffer), f)) > 0) {
        for (size_t i = 0; i < bytes_read; i++) {
          update_histogram(local_hist, buffer[i]);
        }
        local_bytes += bytes_read;
      }
      fclose(f);
    }

    // Update global state under mutex lock
    pthread_mutex_lock(&hist_mutex);
    merge_histogram(local_hist, global_histogram);
    bytes_since_last_print += local_bytes;

    if (bytes_since_last_print >= PRINT_INTERVAL) {
      print_histogram(global_histogram);
      bytes_since_last_print = 0;
    }
    pthread_mutex_unlock(&hist_mutex);

    // Free the string allocated via strdup() during traversal
    free(path);
  }

  return NULL;
}

int main(int argc, char * const *argv) {
  if (argc < 2) {
    err(1, "usage: paths...");
    exit(1);
  }

  int num_threads = 1;
  char * const *paths = &argv[1];

  if (argc > 3 && strcmp(argv[1], "-n") == 0) {
    // Since atoi() simply returns zero on syntax errors, we cannot
    // distinguish between the user entering a zero, or some
    // non-numeric garbage.  In fact, we cannot even tell whether the
    // given option is suffixed by garbage, i.e. '123foo' returns
    // '123'.  A more robust solution would use strtol(), but its
    // interface is more complicated, so here we are.
    num_threads = atoi(argv[2]);

    if (num_threads < 1) {
      err(1, "invalid thread count: %s", argv[2]);
    }

    paths = &argv[3];
  } else {
    paths = &argv[1];
  }

  // Initialise the job queue and some worker threads here.
  job_queue_init(&queue, 64);

  pthread_t *threads = malloc(num_threads * sizeof(pthread_t));
  if (!threads) {
    err(1, "failed to allocate memory for thread handles");
  }

  for (int i = 0; i < num_threads; i++) {
    if (pthread_create(&threads[i], NULL, worker_func, NULL) != 0) {
      err(1, "failed to create worker thread %d", i);
    }
  }

  // FTS_LOGICAL = follow symbolic links
  // FTS_NOCHDIR = do not change the working directory of the process
  //
  // (These are not particularly important distinctions for our simple
  // uses.)
  int fts_options = FTS_LOGICAL | FTS_NOCHDIR;

  FTS *ftsp;
  if ((ftsp = fts_open(paths, fts_options, NULL)) == NULL) {
    err(1, "fts_open() failed");
    return -1;
  }

  FTSENT *p;
  while ((p = fts_read(ftsp)) != NULL) {
    switch (p->fts_info) {
    case FTS_D:
      break;
    case FTS_F:
      job_queue_push(&queue, strdup(p->fts_path)); // Process the file p->fts_path, somehow.
      break;
    default:
      break;
    }
  }

  fts_close(ftsp);

  // Shut down the job queue and the worker threads here.
  job_queue_destroy(&queue);

  for (int i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
  }
  free(threads);

  pthread_mutex_lock(&hist_mutex);
  print_histogram(global_histogram);
  pthread_mutex_unlock(&hist_mutex);

  pthread_mutex_destroy(&hist_mutex);

  move_lines(9);

  return 0;
}
