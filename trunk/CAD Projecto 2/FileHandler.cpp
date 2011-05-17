/* 
 * File:   FileHandler.cpp
 * Author: guilhermerodrigues
 * 
 * Created on March 26, 2011, 9:24 PM
 */

#include "FileHandler.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <fstream>
#include <fcntl.h>

// TODO : remove useless libraries <iostream>
#include <iostream>

void* BeginReadingThread(void* p) {
    FileHandler *fh = (FileHandler *) p;

    fh->startReadInputs();

    pthread_exit(NULL);
}

FileHandler::FileHandler() {
    ruleHandler = NULL;
    for (int i = 0; i < NUM_RANGE; i += 5) {
        lookupSizes[i] = sprintf(lookupTable[i], "%d", i);
        lookupSizes[i + 1] = sprintf(lookupTable[i + 1], "%d", i + 1);
        lookupSizes[i + 2] = sprintf(lookupTable[i + 2], "%d", i + 2);
        lookupSizes[i + 3] = sprintf(lookupTable[i + 3], "%d", i + 3);
        lookupSizes[i + 4] = sprintf(lookupTable[i + 4], "%d", i + 4);
    }

    lookupSizes[NUM_RANGE] = sprintf(lookupTable[NUM_RANGE], "%d", NUM_RANGE);
    
    highest_available = -1;
}

FileHandler::FileHandler(const FileHandler& orig) {
}

void FileHandler::addOutput(cell_array input, cell_value classf, int thread_id) {
    inputHandler[0]->output[thread_id].push_back(pair<cell_array, cell_value>(input, classf));
}

cell_vector* FileHandler::readRuleFile(const char* FileName) {
    LoadedFile* newRule = readFile(FileName, RULE_SIZE, RULE_NUM, RULE_NUM*RULE_SIZE);
    ruleHandler = newRule;

    return newRule->workVector;
}

cell_vector* FileHandler::readInputFile(const char* FileName) {
    LoadedFile* newInput = readFile(FileName, INPUT_SIZE, INPUT_NUM, INPUT_NUM*INPUT_SIZE);
    
    newInput->finished_work = 0;
    newInput->availableWork = INPUT_NUM;
    pthread_mutex_init(&(newInput->mutex), NULL);
    pthread_cond_init (&(newInput->finished_cond), NULL);
    
    inputHandler.push_back(newInput);

    return newInput->workVector;
}

void FileHandler::start() {
    int rc = pthread_create(&thread, NULL, BeginReadingThread, (void *) this);
    if (rc) {
        printf("ERROR in FileHandler; return code from pthread_create() is % d\n", rc);
        exit(-1);
    }
}

void FileHandler::startReadInputs() {
    
    int files_read = 0;
    int files_proc = 0;
    char inputFileName[80];
    char outputFileName[80];
    LoadedFile* lf;
    
    do {
        
        if( files_read - files_proc > 1 ) {
            
            lf = inputHandler[files_proc];
            
            // Check to see if all threads have finished processing this input file
            LOCK(lf->mutex);
            COND_WAIT((lf->finished_work<NUM_THREADS), lf->finished_cond, lf->mutex)
            UNLOCK(lf->mutex);
            
            sprintf(outputFileName, OUPUT_STRING, files_proc);
            
            // Condition verified - begin output print
            manageOutputOf( files_proc, outputFileName);
            
            // Cleaup resources used
            pthread_mutex_destroy(&lf->mutex);
            pthread_cond_destroy(&lf->finished_cond);
            
            delete lf->workVector;
            delete lf->memoryBlock;
            delete inputHandler[files_proc];
            
            inputHandler[files_proc] = NULL;
            
            files_proc++;
        }
        
        sprintf(inputFileName, INPUT_STRING, files_read);
        
        readInputFile(inputFileName);
        
        files_read++;
        
        LOCK(available_mutex);
        
        highest_available++;
        
        COND_BROADCAST(available_cond);
        UNLOCK(available_mutex);
        
    }while(files_read < NUM_FILES);

    // All files have been read, process remaining files
    while (files_proc < files_read) {
        lf = inputHandler[files_proc];

        // Check to see if all threads have finished processing this input file
        LOCK(lf->mutex);
        COND_WAIT((lf->finished_work < NUM_THREADS), lf->finished_cond, lf->mutex)
        UNLOCK(lf->mutex);

        sprintf(outputFileName, OUPUT_STRING, files_proc);
        
        // Condition verified - begin output print
        manageOutputOf(files_proc, outputFileName);

        // Cleaup resources used
        pthread_mutex_destroy(&lf->mutex);
        pthread_cond_destroy(&lf->finished_cond);

        delete lf->workVector;
        delete lf->memoryBlock;
        delete inputHandler[files_proc];

        inputHandler[files_proc] = NULL;

        files_proc++;
    }
    
    pthread_exit(NULL);
}

LoadedFile* FileHandler::getNextWorkFile(int file_id) {

    if (file_id < NUM_FILES) {

        LOCK(available_mutex);

        COND_WAIT((file_id > highest_available), available_cond, available_mutex)

        UNLOCK(available_mutex);

        return inputHandler[file_id];
    } else {
        
        return NULL;
    }
}

void FileHandler::freeRuleSpace() {
    if (ruleHandler != NULL) {
        delete ruleHandler->memoryBlock;
        delete ruleHandler->workVector;

        delete ruleHandler;
        
        ruleHandler = NULL;
    }
}

unsigned long int FileHandler::getMemoryUsed() {
    vector<LoadedFile*>::iterator it = inputHandler.begin();
    
    unsigned long int totalSize = 0;

    while(it < inputHandler.end()) {
        totalSize += (*it)->size * sizeof(cell_value);
        totalSize += (*it)->workVector->size() * sizeof(cell_value*);
        totalSize += sizeof(unsigned int);

        for(int i=0; i<NUM_THREADS; i++)
            totalSize += (*it)->output[i].size() * sizeof( pair<cell_array, cell_value> );

        it++;
    }

    if (ruleHandler != NULL) {
        totalSize += ruleHandler->size * sizeof (cell_value);
        totalSize += ruleHandler->workVector->size() * sizeof (cell_array);
        totalSize += sizeof (unsigned int);
    }

    return totalSize;
}

void FileHandler::manageOutputOf(int file_id, const char* fileName) {

    clock_t s = clock();

    LoadedFile* file = inputHandler[file_id];

    int fd, result;
    char *map;

    int fileSize = 0;

#ifdef SERIAL
    fileSize = file->output[0].size();
    printf("Found %d outputs\n", fileSize);
#else
    cout << endl;
    for (int i = 0; i < NUM_THREADS; i++) {
        fileSize += file->output[i].size();
//        printf("Thread %d added %d outputs\n", i, (int) file->output[i].size());
    }
    cout << endl;
#endif

    fileSize *= RULE_SIZE * 6 * sizeof (char);

    fd = open(fileName, O_RDWR | O_CREAT, (mode_t) 0600);

    if (fd == -1) {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }

    result = lseek(fd, fileSize - 1, SEEK_SET);
    if (result == -1) {
        close(fd);
        perror("Error calling lseek() to 'stretch' the file");
        exit(EXIT_FAILURE);
    }

    result = write(fd, "", 1);
    if (result != 1) {
        close(fd);
        perror("Error writing last byte of the file");
        exit(EXIT_FAILURE);
    }

    map = static_cast<char*> (mmap(NULL, fileSize, PROT_WRITE, MAP_SHARED, fd, 0));

    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    int idx = 0;
    cell_array t_id;
    list< pair<cell_array, cell_value> >::iterator it;

    cout << "Extra memory reserved: " << fileSize / (float) (1048576) << " MB\n";

#ifdef SERIAL
    int i = 0;
#else
    for (int i = 0; i < NUM_THREADS; i++) {
#endif
        for (it = file->output[i].begin(); it != file->output[i].end(); it++) {
            t_id = &((*it).first[0]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[1]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[2]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[3]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[4]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[5]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[6]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[7]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[8]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id = &((*it).first[9]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            memcpy(&map[idx], lookupTable[(*it).second], lookupSizes[(*it).second]);
            idx += lookupSizes[(*it).second];
            map[idx++] = '\n';
        }
        
#ifndef SERIAL
    }
#endif

    map[idx] = '\0';
    close(fd);
    munmap(map, sizeof (map));

    cout << "\nWrite took: " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;

}

cell_value FileHandler::c_nextToken(char delim) {

    cell_value result=0;
    char* lptr = tokenPtr;

    for(; *lptr != '\0'; lptr++) {

        if(*lptr == delim) {
            *lptr = '\0';
            tokenPtr = ++lptr;
            
            return result;
        }

        result *= 10;
        result += *lptr - '0';
    }

    if (result>0) {
        tokenPtr = lptr;
        return result;
    }
    
    return -1;
}

LoadedFile* FileHandler::readFile(const char* FileName, int row_size, int vector_size, int number_size) {

    clock_t s = clock();

    cell_vector* rules = new cell_vector();
    rules->reserve(vector_size);

    int fd;
    char *map;
    struct stat buffer;
    int status;

    fd = open(FileName, O_RDONLY);
    status = fstat(fd, &buffer);

    if (fd == -1) {
	perror("Error opening file for reading");
	exit(EXIT_FAILURE);
    }

    map = static_cast<char*>(mmap(NULL, buffer.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0));

    if (map == MAP_FAILED) {
	close(fd);
	perror("Error mmapping the file");
	exit(EXIT_FAILURE);
    }
    
    tokenPtr = map;
    cell_value value = c_nextToken(',');
    
    cell_array array = new cell_value[number_size];
    int ruleIndex = 0, ruleStart;

    while (value>=0) {
        ruleStart = ruleIndex;

        array[ruleIndex++] = value;
        array[ruleIndex++] = c_nextToken(',');
        array[ruleIndex++] = c_nextToken(',');
        array[ruleIndex++] = c_nextToken(',');
        array[ruleIndex++] = c_nextToken(',');
        array[ruleIndex++] = c_nextToken(',');
        array[ruleIndex++] = c_nextToken(',');
        array[ruleIndex++] = c_nextToken(',');
        array[ruleIndex++] = c_nextToken(',');

        if(row_size == RULE_SIZE) {
            array[ruleIndex++] = c_nextToken(',');
            array[ruleIndex++] = c_nextToken('\n');
        }else {
            array[ruleIndex++] = c_nextToken('\n');
        }

        rules->push_back(&array[ruleStart]);

        value = c_nextToken(',');
    }

    close(fd);
    munmap(map, sizeof(map));

    LoadedFile *newFile  = new LoadedFile;

    newFile->size        = number_size;
    newFile->memoryBlock = array;
    newFile->workVector  = rules;

    cout << "\nRead took: " << (((double)clock() - s) / CLOCKS_PER_SEC) << endl;

    return newFile;
}

FileHandler::~FileHandler() {
    vector<LoadedFile*>::iterator it = inputHandler.begin();

    while (it < inputHandler.end()) {
        if (*it != NULL) {

            pthread_mutex_destroy(&((*it)->mutex));
            pthread_cond_destroy(&((*it)->finished_cond));

            delete (*it)->memoryBlock;
            delete (*it)->workVector;
            delete (*it);
        }
        
        it++;
    }

    inputHandler.clear();

    if (ruleHandler != NULL) {
        delete ruleHandler->memoryBlock;
        delete ruleHandler->workVector;

        delete ruleHandler;
    }
}

