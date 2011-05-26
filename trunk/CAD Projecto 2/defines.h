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
#include <mpi.h>

#define DEBUG

#define WRITE_RANK  0

#define RULE_SIZE   11
#define INPUT_SIZE  10

#define RULE_NUM    2000000
#define INPUT_NUM   1000000

#define NUM_RANGE   10000

#define WORK_RANGE  100000

#define NUM_CLASS   100

#define RULE_ACCEPTED_DEPTH -1

#define NUM_FILES   1

#define NEW_STATE_CACHE_SIZE 1000
#define MAX_STATE_CACHE_SIZE 50000

#define MPI

#define OUTPUT(file, input, r_class, id) \
(file)->output[(id)].push_back(OutputPair((input), (r_class)))

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

#ifndef DEBUG
#define ASSERT(x)
#else
#define ASSERT(x) \
        if (! (x)) \
        { \
                cout << "\nERROR!! Assert " << #x << " failed\n"; \
                cout << " on line " << __LINE__  << "\n"; \
                cout << " in file " << __FILE__ << "\n\n";  \
        }
#endif

using namespace std;

typedef short   cell_value;

typedef cell_value Depth;
typedef cell_value* cell_array;
typedef vector<cell_value*> cell_vector;
typedef pair<pthread_t, pthread_t> threadPair;

#endif	/* DEFINES_H */

