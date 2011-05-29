/* 
 * File:   FileHandler.h
 * Author: guilhermerodrigues
 *
 * Created on March 26, 2011, 9:24 PM
 */

#ifndef FILEHANDLER_H
#define	FILEHANDLER_H

#include "defines.h"
//#include "mpi.h"

#define MAX_RULES 2000000

//typedef pair<int, cell_value> OutputPair;

void* BeginReadingThread(void* p);
void* BeginWritingThread(void* p);

namespace fileHandler {

enum {
    WRITE_THREAD = 1,
    READ_THREAD = 0
};

struct OutputPair {
    int index;
    short rule;
    
    OutputPair() {
        index = 0;
        rule = -9999;
    }
    
    OutputPair(int n_index, short n_rule) {
        index = n_index;
        rule = n_rule;
    }
};

typedef vector< OutputPair > outputVector;

struct LoadedFile {
    cell_array memoryBlock;
    cell_vector* workVector;
    unsigned int size;
    
    pthread_mutex_t mutex;
    pthread_cond_t  finished_cond;
    
    bool finished_work;
    bool loaded;
    
    int* outputSizes;
    outputVector* output;
    
    void finished() {
        
        LOCK(mutex);
        
        finished_work = true;
        COND_BROADCAST(finished_cond);
        
        UNLOCK(mutex);
    }
};

typedef LoadedFile* file_pointer;

class FileHandler {
public:
    FileHandler();
    FileHandler(const FileHandler& orig);
    virtual ~FileHandler();

    void init(int rank, int num_process, MPI_Status status);
    
    cell_vector* readRuleFile(const char* FileName, int start=0, int size=RULE_NUM);
    cell_vector* readInputFile(const char* FileName, int id);
    
    void start();
    
    void startReadingThread();
    void startMPIreceiveThread();
    
    file_pointer getNextWorkFile(int file_id);
    
    void manageOutputOf(int file_id, const char* fileName);

    unsigned long int getMemoryUsed();
    
    void freeRuleSpace();

    pthread_t getThreadObject(int objId);
    
private:
    cell_value c_nextToken(char delim);
    
    file_pointer readFile(const char* FileName, int row_size, int vector_size, file_pointer file, int start);
    
    int highest_available;
    
    /* Atributes */

    int rank;
    int num_process;
    MPI_Status status;
    int* num_ficheiro;
    char*       tokenPtr;

    char        lookupTable[NUM_RANGE+1][7];
    cell_value  lookupSizes[NUM_RANGE+1];

    file_pointer ruleHandler;
    file_pointer inputHandler[NUM_FILES];
    //LoadedFile* m;
    pthread_t read_thread;
    pthread_t write_thread;
    
    /* Access control */
    pthread_mutex_t available_mutex;
    pthread_cond_t  available_cond;
    
    pthread_mutex_t finished_mutex;
    
};

} //namespace fileHandler

#endif	/* FILEHANDLER_H */

