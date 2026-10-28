// Setting _DEFAULT_SOURCE is necessary to activate visibility of
// certain header file contents on GNU/Linux systems.
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>

// err.h contains various nonstandard BSD extensions, but they are
// very handy.
#include <err.h>

#include <pthread.h>

#include "job_queue.h"

// size of the job queue. if it is full main just blocks in push until
// a worker has taken something, so it doesn't need to be very big
#define QUEUE_CAPACITY 64

// lock for printing, so lines from different threads don't get mixed up
pthread_mutex_t print_lock = PTHREAD_MUTEX_INITIALIZER;

// stuff each worker needs. all workers share the same queue and needle
struct worker_args {
  struct job_queue *q;
  char const *needle;
};
int fauxgrep_file(char const *needle, char const *path) {
  FILE *f = fopen(path, "r");

  if (f == NULL) {
    warn("failed to open %s", path);
    return -1;
  }

  char *line = NULL;
  size_t linelen = 0;
  int lineno = 1;

  while (getline(&line, &linelen, f) != -1) {
    if (strstr(line, needle) != NULL) {
      // only lock around the print, not the whole file. otherwise the
      // threads would basically run one at a time
      pthread_mutex_lock(&print_lock);
      printf("%s:%d: %s", path, lineno, line);
      pthread_mutex_unlock(&print_lock);
    }

    lineno++;
  }

  free(line);
  fclose(f);

  return 0;
}
void *worker(void *arg) {
  struct worker_args *args = arg;
  void *data;

  // keep taking file names until pop returns -1 (queue is empty and destroyed)
  while (job_queue_pop(args->q, &data) == 0) {
    char *path = data;
    fauxgrep_file(args->needle, path);
    free(path); // was strdup'ed in main
  }

  return NULL;
}

int main(int argc, char * const *argv) {
  if (argc < 2) {
    err(1, "usage: [-n INT] STRING paths...");
    exit(1);
  }

  int num_threads = 1;
  char const *needle = argv[1];
  char * const *paths = &argv[2];


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

    needle = argv[3];
    paths = &argv[4];

  } else {
    needle = argv[1];
    paths = &argv[2];
  }

  // set up the queue
  struct job_queue q;
  if (job_queue_init(&q, QUEUE_CAPACITY) != 0) {
    err(1, "job_queue_init failed");
  }

  // start the workers. they will just wait in pop until we push something
  struct worker_args args;
  args.q = &q;
  args.needle = needle;

  pthread_t *threads = malloc(num_threads * sizeof(pthread_t));
  if (threads == NULL) {
    err(1, "malloc failed");
  }

  for (int i = 0; i < num_threads; i++) {
    if (pthread_create(&threads[i], NULL, worker, &args) != 0) {
      err(1, "pthread_create failed");
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
    case FTS_F: {
      // fts reuses the memory for fts_path, so the worker needs its own copy
      char *path = strdup(p->fts_path);
      if (path == NULL) {
        err(1, "strdup failed");
      }
      if (job_queue_push(&q, path) != 0) {
        free(path);
        errx(1, "job_queue_push failed");
      }
      break;
    }
    default:
      break;
    }
  }

  fts_close(ftsp);

  // destroy waits until the queue is empty and then wakes up the workers
  // so their pop returns -1
  job_queue_destroy(&q);

  // destroy only waits for the queue to be empty, a worker might still be
  // searching its last file, so we have to join all the threads
  for (int i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
  }

  free(threads);
  return 0;
}
