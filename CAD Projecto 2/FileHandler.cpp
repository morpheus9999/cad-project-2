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

using namespace fileHandler;

void* BeginReadingThread(void* p) {
    FileHandler *fh = (FileHandler *) p;

    fh->startReadingThread();

    pthread_exit(NULL);
}

void* BeginWritingThread(void* p) {
    FileHandler *fh = (FileHandler *) p;

    fh->startMPIreceiveThread();

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

pthread_t FileHandler::getThreadObject(int objId) {
    if(objId == READ_THREAD) {
        return read_thread;
    } else if(objId == WRITE_THREAD) {
        return write_thread;
    }
    
    return NULL;
}

cell_vector* FileHandler::readRuleFile(const char* FileName, int start, int size) {
    ruleHandler = new LoadedFile;
    
    readFile(FileName, RULE_SIZE, size, ruleHandler, start);

    return ruleHandler->workVector;
}

cell_vector* FileHandler::readInputFile(const char* FileName, int id) {
    
    file_pointer newInput = readFile(FileName, INPUT_SIZE, INPUT_NUM, inputHandler[id], 0);

    newInput->finished_work = false;
    pthread_mutex_init(&(newInput->mutex), NULL);
    pthread_cond_init(&(newInput->finished_cond), NULL);
    
    inputHandler[id] = newInput;
    
    return newInput->workVector;
}

void FileHandler::init(int rank, int num_process, MPI_Status status) {
    this->rank=rank;
 
    this->num_process=num_process;
    
    num_ficheiro = new int[num_process];
    memset(num_ficheiro, 0, sizeof(int)*num_process);
    
    this->status=status;
    
    file_pointer* ptr = inputHandler;
    for(int i=0; i<NUM_FILES; i++, ptr++) {
        (*ptr) = new LoadedFile;
        (*ptr)->outputSizes = new int[num_process];
        (*ptr)->output = new outputVector[num_process];
        (*ptr)->loaded = false;
    }
}


void FileHandler::start() {
    
    //cout << "RANK :::::" << rank << endl;
    
    int rc = pthread_create(&read_thread, NULL, BeginReadingThread, (void *) this);
    if (rc) {
        printf("ERROR in FileHandler; return code from pthread_create() is % d\n", rc);
        exit(-1);
    }
    
    // Writing is only done by the master process
    if(rank==WRITE_RANK){
        rc = pthread_create(&write_thread, NULL, BeginWritingThread, (void *) this);
        if (rc) {
            printf("ERROR in FileHandler; return code from pthread_create() is % d\n", rc);
            exit(-1);
        }
    
    }
}

void FileHandler::startMPIreceiveThread() {

    MPI_Datatype OutputPairtype;
    char outputFileName[80];
    MPI_Datatype type[2] = {MPI_INT, MPI_SHORT};
    int blocklength[2] = {1, 1};
    MPI_Aint disp[2] = {0, sizeof (int)}; //PODE NAO FUNKAR
    MPI_Type_struct(2, blocklength, disp, type, &OutputPairtype);
    MPI_Type_commit(&OutputPairtype);
    
    int numberOfOutputs;
    outputVector* outputPointer;
    file_pointer currentFile;
    //OutputPair k;
    
    //cout << "ENTRA Thread receber!!!!" << endl;
    
    for (int currentFileId = 0; currentFileId < NUM_FILES; currentFileId++) {
        
        currentFile = inputHandler[currentFileId];
        //cout << "ENTRA Thread receber3!!!!" << endl;
        for (int i = 0; i < num_process - 1; i++) {
            // First receive the number of outputs generated from sender
            //cout << "vai receber(rank=" << rank << ") :: " << endl;
            MPI_Recv(&numberOfOutputs, 1, MPI_INT, MPI_ANY_SOURCE, currentFileId, MPI_COMM_WORLD, &status);
           // printf("-- received from rank %d, %d outputs\n", status.MPI_SOURCE, numberOfOutputs);
            if (numberOfOutputs > 0) {
                // Then reserve memory to contain the new values in the appropriate structure
                outputPointer = &(currentFile->output[status.MPI_SOURCE]);
                ASSERT(outputPointer!=NULL);
                outputPointer->reserve(numberOfOutputs);
                
                currentFile->outputSizes[status.MPI_SOURCE] = numberOfOutputs;
                
                // Next instruct MPI_Recv to wait for a message from the same sender & tag with
                //the new outputs and directly copy them to the assigned structure
               // cout << "vai receber (do rank=" << rank << ") :: recebe do " << status.MPI_SOURCE << " :" << numberOfOutputs << endl;
                
                MPI_Recv(&(*outputPointer)[0], numberOfOutputs, OutputPairtype,
                        status.MPI_SOURCE, currentFileId, MPI_COMM_WORLD, &status);
                
                //printf("recebeu indice %d, regra %d\n", (*outputPointer)[0].index, (*outputPointer)[0].rule);
                //printf("recebeu indice %d, regra %d\n", (*outputPointer)[1].index, (*outputPointer)[1].rule);
                
            }
            

            //cout << "num_ficheiro :::::" << num_ficheiro[currentFileId] << endl;
        }
        
        // All outputs from other clients have been received so we begin the final wrap up
        // Again, not sure why the condition is relevant...
        //if (num_ficheiro[currentFileId] > num_process) {

            //cout << "----------- IMPRIME ----------- " << rank << endl;
            sprintf(outputFileName, OUPUT_STRING, currentFileId);

            // Check if this client has finished processing the file
            currentFile = inputHandler[currentFileId];
            
            // Check to see if all threads have finished processing this input file
            LOCK(currentFile->mutex);
            COND_WAIT(!currentFile->finished_work, currentFile->finished_cond, currentFile->mutex)
            UNLOCK(currentFile->mutex);

            currentFile->outputSizes[rank] = currentFile->output[rank].size();
            
            // Condition verified - begin output print
            manageOutputOf(currentFileId, outputFileName);

            // Cleaup resources used
            pthread_mutex_destroy(&currentFile->mutex);
            pthread_cond_destroy(&currentFile->finished_cond);

            delete currentFile->workVector;
            delete currentFile->memoryBlock;
            delete currentFile;

            currentFile = NULL;
        //}

        //cout << " ACABOU RECEBER:::::" << endl;
    }
}

void FileHandler::startReadingThread() {

    int files_read = 0;
    int files_processed = 0;
    char inputFileName[80];
    char outputFileName[80];
    file_pointer loadedFile;

    int numberOfOutputs;
    
    do {

        if (files_read - files_processed > 1) {

            loadedFile = inputHandler[files_processed];

            // Check to see if all threads have finished processing this input file
            LOCK(loadedFile->mutex);
            COND_WAIT(!loadedFile->finished_work, loadedFile->finished_cond, loadedFile->mutex)
            UNLOCK(loadedFile->mutex);
            
            
            if(rank!=WRITE_RANK)
            {
                //cout<< "ENTRA PARA ENVIAR rank= "<< rank << endl;
                
                MPI_Datatype OutputPairtype;
                MPI_Datatype type[2]= { MPI_INT, MPI_SHORT};
                int blocklength[2]={1,1};
                MPI_Aint disp[2]={0,sizeof (int)};//vai dar merda
                MPI_Type_struct(2,blocklength,disp,type,&OutputPairtype);
                MPI_Type_commit(&OutputPairtype);
                
                // Send a first message containing the number of elements
                numberOfOutputs = loadedFile->output[rank].size();
                //cout << "vai enviar(rank="<<rank <<") :: "<< numberOfOutputs << endl;
                MPI_Send(&numberOfOutputs, 1, MPI_INT, WRITE_RANK, files_processed,MPI_COMM_WORLD);

                if (numberOfOutputs > 0) {
                    // And a second with the elements themselves
                   // cout << "vai enviar 2 (rank=" << rank << ") :: " << numberOfOutputs << endl;
                    MPI_Send(&(loadedFile->output[rank]), loadedFile->output[rank].size(),
                            OutputPairtype, WRITE_RANK, files_processed, MPI_COMM_WORLD);
                }
                // Finally cleanup resources used by this file
                pthread_mutex_destroy(&loadedFile->mutex);
                pthread_cond_destroy(&loadedFile->finished_cond);

                delete loadedFile->workVector;
                delete loadedFile->memoryBlock;
                delete loadedFile;

                loadedFile = NULL;
                
            }else{
                numberOfOutputs = loadedFile->output[rank].size();
                //cout << "tenho (rank="<<rank <<") :: "<< numberOfOutputs << endl; 
            }
            
            files_processed++;
            
        }

        sprintf(inputFileName, INPUT_STRING, files_read);

        readInputFile(inputFileName, files_read);

        files_read++;

        LOCK(available_mutex);

        highest_available++;
        COND_SIGNAL(available_cond);

        UNLOCK(available_mutex);

    } while (files_read < NUM_FILES);

    // All files have been read, process remaining files
    while (files_processed < files_read) {
        loadedFile = inputHandler[files_processed];

        // Check to see if all threads have finished processing this input file
        LOCK(loadedFile->mutex);
        COND_WAIT(!loadedFile->finished_work, loadedFile->finished_cond, loadedFile->mutex)
        UNLOCK(loadedFile->mutex);

        if (rank != WRITE_RANK) {
            //cout << "ENTRA PARA ENVIAR rank= " << rank << endl;

            MPI_Datatype OutputPairtype;
            MPI_Datatype type[2] = {MPI_INT, MPI_SHORT};
            int blocklength[2] = {1, 1};
            MPI_Aint disp[2] = {0, sizeof (int)};
            MPI_Type_struct(2, blocklength, disp, type, &OutputPairtype);
            MPI_Type_commit(&OutputPairtype);

            // Send a first message containing the number of elements
            numberOfOutputs = loadedFile->output[rank].size();
            //cout << "vai enviar(rank=" << rank << ") :: " << numberOfOutputs << endl;
            MPI_Send(&numberOfOutputs, 1, MPI_INT, WRITE_RANK, files_processed, MPI_COMM_WORLD);

            if (numberOfOutputs > 0) {
                // And a second with the elements themselves
                //cout << "vai enviar 2 (rank=" << rank << ") :: " << numberOfOutputs << endl;
                //printf("env: indice %d, regra %d\n", (loadedFile->output[rank])[0].index, (loadedFile->output[rank])[0].rule);
                //printf("env: indice %d, regra %d\n", (loadedFile->output[rank])[1].index, (loadedFile->output[rank])[1].rule);
                
                
                
                MPI_Send(&(loadedFile->output[rank])[0], loadedFile->output[rank].size(),
                        OutputPairtype, WRITE_RANK, files_processed, MPI_COMM_WORLD);
            }
            // Finally cleanup resources used by this file
            pthread_mutex_destroy(&loadedFile->mutex);
            pthread_cond_destroy(&loadedFile->finished_cond);

            delete loadedFile->workVector;
            delete loadedFile->memoryBlock;
            delete loadedFile;

            loadedFile = NULL;

        }else{
                numberOfOutputs = loadedFile->output[rank].size();
                //cout << "tenho (rank="<<rank <<") :: "<< numberOfOutputs << endl; 
            }

        files_processed++;
    }
    
    //cout << " acabou :S .......... "<< rank << endl;
}

file_pointer FileHandler::getNextWorkFile(int file_id) {

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
    file_pointer* file_iterator = inputHandler;

    unsigned long int totalSize = 0;

    for(int i=0; i<NUM_FILES; i++) {
        if ((*file_iterator)->loaded) {
            totalSize += (*file_iterator)->size * sizeof (cell_value);
            totalSize += (*file_iterator)->workVector->size() * sizeof (cell_value*);
            totalSize += sizeof (unsigned int);

            for (int i = 0; i < num_process; i++) {
                totalSize += (*file_iterator)->outputSizes[i] * sizeof ( OutputPair );
            }

            file_iterator++;
        }
    }

    if (ruleHandler != NULL) {
        totalSize += ruleHandler->size * sizeof (cell_value);
        totalSize += ruleHandler->workVector->size() * sizeof (cell_array);
        totalSize += sizeof (unsigned int);
    }

    return totalSize;
}

void FileHandler::manageOutputOf(int file_id, const char* fileName) {

    //clock_t s = clock();
    
    file_pointer file = inputHandler[file_id];

    int fd, result;
    char *map;

    int fileSize = 0;
    for (int i = 0; i < num_process; i++) {
        fileSize += file->outputSizes[i];
    
    }
    printf("Found %d outputs\n", fileSize);

    //cout << "contador::" << fileSize << endl;
    fileSize *= RULE_SIZE * 6 * sizeof (char);

    fd = open(fileName, O_RDWR | O_CREAT, (mode_t) 0600);

    if (fd == -1) {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }
    if (fileSize == 0)
        fileSize = 2;

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

    //cout << "Encontrados: " << fileSize << endl;
    cout << "Extra memory reserved: " << fileSize / (float) (1048576) << " MB\n";
    
    int j;
    outputVector* it;
    
    for (int i = 0; i < num_process; i++) {
        
        it = &(file->output[i]);
        
        for (j=0; j < file->outputSizes[i]; j++) {
            
            t_id = (*file->workVector)[(*it)[j].index];

            //t_id = &((*it).first[0]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[1]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[2]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[3]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[4]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[5]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[6]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[7]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[8]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            //t_id &((*it).first[9]);
            memcpy(&map[idx], lookupTable[*t_id], lookupSizes[*t_id]);
            idx += lookupSizes[*t_id];
            map[idx++] = ',';
            t_id++;
            memcpy(&map[idx], lookupTable[(*it)[j].rule], lookupSizes[(*it)[j].rule]);
            idx += lookupSizes[(*it)[j].rule];
            map[idx++] = '\n';

        }
    }

    map[idx] = '\0';
    close(fd);
    munmap(map, sizeof (map));

   // cout << "\nWrite took: " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;

}

cell_value FileHandler::c_nextToken(char delim) {

    cell_value result = 0;
    char* lptr = tokenPtr;

    for (; *lptr != '\0'; lptr++) {

        if (*lptr == delim) {
            *lptr = '\0';
            tokenPtr = ++lptr;

            return result;
        }

        result *= 10;
        result += *lptr - '0';
    }

    if (result > 0) {
        tokenPtr = lptr;
        return result;
    }

    return -1;
}

file_pointer FileHandler::readFile(const char* FileName, int row_size, int vector_size, file_pointer file, int start) {

    clock_t s = clock();

    cell_vector* rules = new cell_vector();
    
    rules->reserve(vector_size);

    int number_size = row_size * vector_size;
    
    int fd;
    char *map;
    struct stat buffer;
    int status;

    fd = open(FileName, O_RDONLY);
    status = fstat(fd, &buffer);

    if (fd == -1) {
        perror("Error opening file for reading");
        printf("do rank==%d ",rank);
        exit(EXIT_FAILURE);
    }

    map = static_cast<char*> (mmap(NULL, buffer.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0));

    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }
    
    tokenPtr = map;
    cell_value value;
    
    cell_array array = new cell_value[number_size];
    int ruleIndex = 0, ruleStart;
    
    while (ruleIndex < start) {
        array[ruleIndex++] = c_nextToken('\n');
    }
    
    value = c_nextToken(',');
    ruleIndex = 0;
    int k = 0;
    while (k < vector_size) {

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

        if (row_size == RULE_SIZE) {
            array[ruleIndex++] = c_nextToken(',');
            array[ruleIndex++] = c_nextToken('\n');
        } else {
            array[ruleIndex++] = c_nextToken('\n');
        }

        rules->push_back(&array[ruleStart]);

        value = c_nextToken(',');
        k++;
    }
    close(fd);
    munmap(map, sizeof (map));

    file->size = number_size;
    file->memoryBlock = array;
    file->workVector = rules;
    file->loaded = true;

    //cout << "\nRead took: " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;

    return file;
}

FileHandler::~FileHandler() {
//    
//    file_pointer* inputFileIterator = inputHandler;
//
//    for(int i=0; i<NUM_FILES; i++) {
//        if (inputFileIterator != NULL) {
//
//            pthread_mutex_destroy(&((*inputFileIterator)->mutex));
//            pthread_cond_destroy(&((*inputFileIterator)->finished_cond));
//
//            delete (*inputFileIterator)->output;
//            
//            delete (*inputFileIterator)->memoryBlock;
//            delete (*inputFileIterator)->workVector;
//            delete (*inputFileIterator);
//        }
//
//        inputFileIterator++;
//    }
//
//    delete inputHandler;
//
//    if (ruleHandler != NULL) {
//        delete ruleHandler->memoryBlock;
//        delete ruleHandler->workVector;
//
//        delete ruleHandler;
//    }
}

