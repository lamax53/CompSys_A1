#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "job_queue.h"


int job_queue_init(struct job_queue *job_queue, int capacity) {
  // initialize the job queue
  job_queue->capacity = capacity; 
  job_queue->front = 0;
  job_queue->rear = -1;
  job_queue->count = 0;
  job_queue->destroyed = 0;
   
  // Allocate memory for the queue
  job_queue->queue = malloc(capacity * sizeof(void*));
  if (job_queue->queue == NULL) {
    return -1;
  }
  
  // Initialize the mutex and condition variables
  pthread_mutex_init(&job_queue->mutex, NULL);
  pthread_cond_init(&job_queue->data, NULL);
  pthread_cond_init(&job_queue->space, NULL);

  return 0;
}


int job_queue_destroy(struct job_queue *job_queue) {
  // Lock the mutex 
  pthread_mutex_lock(&job_queue->mutex);

  // indicate that the queue is being destroyed
  job_queue->destroyed = 1;
  
  // Wake up any threads that are waiting on the condition variables
  pthread_cond_broadcast(&job_queue->data); 
  pthread_cond_broadcast(&job_queue->space); 

  // wait until the queue is empty before destroying it
  while (job_queue->count > 0) {
    pthread_cond_wait(&job_queue->space, &job_queue->mutex);
  }

  // Unlock the mutex
  pthread_mutex_unlock(&job_queue->mutex);

  // Free the memory allocated for the queue
  free(job_queue->queue);

  return 0;
}


 
int job_queue_push(struct job_queue *job_queue, void *data) {
  // Lock the mutex 
  pthread_mutex_lock(&job_queue->mutex); 

  // Wait if the queue is full and not destroyed
  while (job_queue->count == job_queue->capacity && !job_queue->destroyed) { 
    pthread_cond_wait(&job_queue->space, &job_queue->mutex);
  }
   
  // if the queue has been destroyed, return -1
  if (job_queue->destroyed) {
    pthread_mutex_unlock(&job_queue->mutex);
    return -1; 
  }

  // Add a new job to the rear of the queue
  job_queue->rear = (job_queue->rear + 1) % job_queue->capacity;
  job_queue->queue[job_queue->rear] = data;
  job_queue->count++;
  
  // signal to any waiting threads that there is data available
  pthread_cond_signal(&job_queue->data); 
  // Unlock the mutex
  pthread_mutex_unlock(&job_queue->mutex);
  return 0;
}

int job_queue_pop(struct job_queue *job_queue, void **data) {
  // Lock the mutex 
  pthread_mutex_lock(&job_queue->mutex); 

   // Wait if the queue is empty and not destroyed
  while (job_queue->count == 0 && !job_queue->destroyed) { 
    pthread_cond_wait(&job_queue->data, &job_queue->mutex);
  }

  // if the queue has been destroyed and is empty, return -1
  if (job_queue->destroyed && job_queue->count == 0) {
    pthread_mutex_unlock(&job_queue->mutex);
    return -1;
  }
  
  // Remove a job from the front of the queue
  *data = job_queue->queue[job_queue->front]; 
  job_queue->front = (job_queue->front + 1) % job_queue->capacity; 
  job_queue->count--; 
  
  // signal to any waiting threads that there is space available
  pthread_cond_signal(&job_queue->space); 
  // Unlock the mutex
  pthread_mutex_unlock(&job_queue->mutex); 
  return 0;
}
