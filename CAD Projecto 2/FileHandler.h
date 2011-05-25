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

struct OutputPair {
    int index;
    short rule;
    
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
    
    outputVector* output;
    
    void finished() {
        
        LOCK(mutex);
        
        finished_work = true;
        COND_BROADCAST(finished_cond);
        
        UNLOCK(mutex);
    }
};

void* BeginReadingThread(void* p);
void* BeginWritingThread(void* p);

class FileHandler {
public:
    FileHandler();
    FileHandler(const FileHandler& orig);
    virtual ~FileHandler();

    cell_vector* readRuleFile(const char* FileName);
    cell_vector* readRuleFileMPI(const char* FileName, int start, int end);
    cell_vector* readInputFile(const char* FileName);
    
    threadPair start();
    
    void startFileHandlerThread();
    void startMPIreceiveThread();
    
    LoadedFile* getNextWorkFile(int file_id);
    
    void manageOutputOf(int file_id, const char* fileName);

    unsigned long int getMemoryUsed();
    
    void setRank(int rank);
    
    void setNumProcess(int num_process);
    void setStat(MPI_Status status);
    
    void freeRuleSpace();

private:
    cell_value c_nextToken(char delim);
    
    LoadedFile* readFile(const char* FileName, int row_size, int vector_size, int number_size);
    LoadedFile* readFileMPI(const char* FileName, int row_size, int vector_size, int number_size,int start,int end);
    int highest_available;
    
    /* Atributes */
    
   
    int x;
    

    int rank;
    int num_process;
    MPI_Status status;
    int* num_ficheiro;
    char*       tokenPtr;

    char        lookupTable[NUM_RANGE+1][7];
    cell_value  lookupSizes[NUM_RANGE+1];

    LoadedFile* ruleHandler;
    vector<LoadedFile*> inputHandler;
    
    //vector <vector <int> > seila;
    
    
    pthread_t read_thread;
    pthread_t write_thread;
    
    /* Access control */
    pthread_mutex_t available_mutex;
    pthread_cond_t  available_cond;
    
    pthread_mutex_t finished_mutex;
    
};

#endif	/* FILEHANDLER_H */

