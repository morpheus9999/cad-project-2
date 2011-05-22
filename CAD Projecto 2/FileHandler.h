/* 
 * File:   FileHandler.h
 * Author: guilhermerodrigues
 *
 * Created on March 26, 2011, 9:24 PM
 */

#ifndef FILEHANDLER_H
#define	FILEHANDLER_H

#include "defines.h"

#define MAX_RULES 2000000

typedef pair<cell_array, cell_value> OutputPair;

struct LoadedFile {
    cell_array memoryBlock;
    cell_vector* workVector;
    unsigned int size;
    
    pthread_mutex_t mutex;
    pthread_cond_t  finished_cond;
    
    bool finished_work;
    
    list< OutputPair > output;
    
    void finished() {
        
        LOCK(mutex);
        
        finished_work = true;
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
    cell_vector* readRuleFileMPI(const char* FileName, int start, int end);
    cell_vector* readInputFile(const char* FileName);
    
    void start();
    
    void startFileHandlerThread();
    
    LoadedFile* getNextWorkFile(int file_id);
    
    void manageOutputOf(int file_id, const char* fileName);

    void addOutput(cell_array input, cell_value classf);

    unsigned long int getMemoryUsed();
    
    void freeRuleSpace();

private:
    cell_value c_nextToken(char delim);
    
    LoadedFile* readFile(const char* FileName, int row_size, int vector_size, int number_size);
    LoadedFile* readFileMPI(const char* FileName, int row_size, int vector_size, int number_size,int start,int end);
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

