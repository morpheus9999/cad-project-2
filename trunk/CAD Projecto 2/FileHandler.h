/* 
 * File:   FileHandler.h
 * Author: guilhermerodrigues
 *
 * Created on March 26, 2011, 9:24 PM
 */

#ifndef FILEHANDLER_H
#define	FILEHANDLER_H

#include <vector>
#include <list>
#include <algorithm>

#define RULE_SIZE   11
#define INPUT_SIZE  10

#define RULE_NUM    2000000
#define INPUT_NUM   1000000

#define NUM_RANGE   10000

#define WORK_RANGE  100000

//#define SERIAL
#define PTHREADS
//#define OPENMP

#define NUM_FILES   1

#ifndef SERIAL
#define NUM_THREADS 4
#else
#define NUM_THREADS 1 // DO NOT CHANGE
#endif

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

typedef int*  cell_array;
typedef int   cell_value;
typedef vector<int*> cell_vector;

struct LoadedFile {
    cell_array memoryBlock;
    cell_vector* workVector;
    unsigned int size;
    
    pthread_mutex_t mutex;
    pthread_cond_t  finished_cond;
    
    int availableWork;
    int finished_work;
    
    list< pair<cell_array, cell_value> > output[NUM_THREADS];
    
    void finished() {
        
        LOCK(mutex);
        
        if (++finished_work == NUM_THREADS)
            COND_SIGNAL(finished_cond);
        
        UNLOCK(mutex);
    }
};

void* BeginReadingThread(void* p);

class FileHandler {
public:
    FileHandler();
    FileHandler(const FileHandler& orig);
    virtual ~FileHandler();

    cell_vector* readRuleFile(const char* FileName);
    cell_vector* readInputFile(const char* FileName);
    
    void start();
    
    void startReadInputs();
    
    LoadedFile* getNextWorkFile(int file_id);
    
    void manageOutputOf(int file_id, const char* fileName);

    void addOutput(cell_array input, cell_value classf, int thread_id=0);

    unsigned long int getMemoryUsed();
    
    void freeRuleSpace();

private:
    cell_value c_nextToken(char delim);
    
    LoadedFile* readFile(const char* FileName, int row_size, int vector_size, int number_size);

    int highest_available;
    
    /* Atributes */
    char*       tokenPtr;

    char        lookupTable[NUM_RANGE+1][7];
    cell_value  lookupSizes[NUM_RANGE+1];

    LoadedFile* ruleHandler;
    vector<LoadedFile*> inputHandler;
    
    pthread_t thread;
    
    /* Access control */
    pthread_mutex_t available_mutex;
    pthread_cond_t  available_cond;
    
    pthread_mutex_t finished_mutex;
    
};

#endif	/* FILEHANDLER_H */

