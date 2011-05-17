/* 
 * File:   defines.h
 * Author: guilhermerodrigues
 *
 * Created on May 17, 2011, 5:49 PM
 */

#ifndef DEFINES_H
#define	DEFINES_H

#include <vector>
#include <list>
#include <algorithm>
#include <map>

#define RULE_SIZE   11
#define INPUT_SIZE  10

#define RULE_NUM    2000000
#define INPUT_NUM   1000000

#define NUM_RANGE   10000

#define WORK_RANGE  100000

#define NUM_CLASS   100

//#define SERIAL
#define PTHREADS
//#define OPENMP

#define NUM_FILES   1

#ifndef SERIAL
#define NUM_THREADS 4
#else
#define NUM_THREADS 1 // DO NOT CHANGE
#endif

#define OUTPUT(file, input, r_class, t_id) \
(file)->output[(t_id)].push_back(pair<cell_array, cell_value>((input), (r_class)))

#define INPUT_STRING "dataset/THE_PROBLEM/trans_day_%d.csv"
#define OUPUT_STRING "dataset/THE_PROBLEM/output_%d.csv"

#define LOCK(mutex)     pthread_mutex_lock(&(mutex))
#define UNLOCK(mutex)   pthread_mutex_unlock(&(mutex))

#define COND_BROADCAST(cond_mutex) pthread_cond_broadcast(&(cond_mutex))
#define COND_SIGNAL(cond_mutex) pthread_cond_signal(&(cond_mutex))

#define COND_WAIT(cond, cond_mutex, mutex) \
while ( cond ) {   \
   pthread_cond_wait(&(cond_mutex), &(mutex));   \
}

using namespace std;

typedef short*  cell_array;
typedef short   cell_value;
typedef vector<short*> cell_vector;

typedef short level;

#endif	/* DEFINES_H */

