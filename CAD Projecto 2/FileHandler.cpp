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

    fh->startFileHandlerThread();

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

cell_vector* FileHandler::readRuleFile(const char* FileName) {
    LoadedFile* newRule = readFile(FileName, RULE_SIZE, RULE_NUM, RULE_NUM * RULE_SIZE);
    ruleHandler = newRule;

    return newRule->workVector;
}

cell_vector* FileHandler::readRuleFileMPI(const char* FileName, int start, int end) {
    LoadedFile* newRule = readFileMPI(FileName, RULE_SIZE, RULE_NUM - end, (RULE_NUM - end) * RULE_SIZE, start, end);
    ruleHandler = newRule;

    return newRule->workVector;
}

cell_vector* FileHandler::readInputFile(const char* FileName) {
    LoadedFile* newInput = readFile(FileName, INPUT_SIZE, INPUT_NUM, INPUT_NUM * INPUT_SIZE);

    newInput->finished_work = false;
    pthread_mutex_init(&(newInput->mutex), NULL);
    pthread_cond_init(&(newInput->finished_cond), NULL);

    inputHandler.push_back(newInput);

    return newInput->workVector;
}

void FileHandler::init(int rank, int num_process, MPI_Status status) {
    this->rank=rank;
 
    this->num_process=num_process;
    
    num_ficheiro = new int[num_process];
    memset(num_ficheiro, 0, sizeof(int)*num_process);
    
    this->status=status;
}


threadPair FileHandler::start() {
    
    cout << "RANK :::::" << rank << endl;
    
    int rc = pthread_create(&read_thread, NULL, BeginReadingThread, (void *) this);
    if (rc) {
        printf("ERROR in FileHandler; return code from pthread_create() is % d\n", rc);
        exit(-1);
    }
    
    // Writing is only done by the master process
    if(rank==0){
        rc = pthread_create(&write_thread, NULL, BeginWritingThread, (void *) this);
        if (rc) {
            printf("ERROR in FileHandler; return code from pthread_create() is % d\n", rc);
            exit(-1);
        }
    
    }
    
    return threadPair(read_thread, write_thread);
}

void FileHandler::startMPIreceiveThread() {

    MPI_Datatype OutputPairtype;
    char outputFileName[80];
    MPI_Datatype type[2] = {MPI_INT, MPI_SHORT};
    int blocklength[2] = {1, 1};
    MPI_Aint disp[2] = {sizeof (int), sizeof (short)}; //vai dar merda
    MPI_Type_struct(2, blocklength, disp, type, &OutputPairtype);
    MPI_Type_commit(&OutputPairtype);
    
    int numberOfOutputs;
    outputVector* outputPointer;
    LoadedFile* currentFile;
    
    for (int currentFileId = 0; currentFileId < NUM_FILES; currentFileId++) {

        currentFile = inputHandler[currentFileId];
        
        for (int i = 0; i < num_process - 1; i++) {
            // First receive the number of outputs generated from sender
            cout << "vai receber(rank=" << rank << ") :: " << endl;
            MPI_Recv(&numberOfOutputs, 1, MPI_INT, MPI_ANY_SOURCE, currentFileId, MPI_COMM_WORLD, &status);

            // Then reserve memory to contain the new values in the appropriate structure
            outputPointer = &(currentFile->output[status.MPI_SOURCE]);
            outputPointer->reserve(numberOfOutputs);

            // Next instruct MPI_Recv to wait for a message from the same sender & tag with
            //the new outputs and directly copy them to the assigned structure
            cout << "vai receber (do rank=" << rank << ") :: recebe do " << status.MPI_SOURCE << " :" << numberOfOutputs << endl;
            MPI_Recv(outputPointer, numberOfOutputs, OutputPairtype,
                    status.MPI_SOURCE, currentFileId, MPI_COMM_WORLD, &status);
            cout << "vai receber 2 (rank=" << rank << ") :: " << numberOfOutputs << endl;

            // Not sure why this is relevant...
            num_ficheiro[currentFileId]++;

            cout << "num_ficheiro :::::" << num_ficheiro[currentFileId] << endl;
        }

        // All outputs from other clients have been received so we begin the final wrap up
        // Again, not sure why the condition is relevant...
        if (num_ficheiro[currentFileId] > num_process) {

            cout << "----------- IMPRIME ----------- " << rank << endl;
            sprintf(outputFileName, OUPUT_STRING, currentFileId);

            // Check if this client has finished processing the file
            currentFile = inputHandler[currentFileId];

            // Check to see if all threads have finished processing this input file
            LOCK(currentFile->mutex);
            COND_WAIT(!currentFile->finished_work, currentFile->finished_cond, currentFile->mutex)
            UNLOCK(currentFile->mutex);

            // Condition verified - begin output print
            manageOutputOf(currentFileId, outputFileName);

            // Cleaup resources used
            pthread_mutex_destroy(&currentFile->mutex);
            pthread_cond_destroy(&currentFile->finished_cond);

            delete currentFile->workVector;
            delete currentFile->memoryBlock;
            delete currentFile;

            currentFile = NULL;
        }

        cout << " ACABOU RECEBER:::::" << endl;
    }
}

void FileHandler::startFileHandlerThread() {

    int files_read = 0;
    int files_processed = 0;
    char inputFileName[80];
    char outputFileName[80];
    LoadedFile* loadedFile;

    int numberOfOutputs;
    
    do {

        if (files_read - files_processed > 1) {

            loadedFile = inputHandler[files_processed];

            // Check to see if all threads have finished processing this input file
            LOCK(loadedFile->mutex);
            COND_WAIT(!loadedFile->finished_work, loadedFile->finished_cond, loadedFile->mutex)
            UNLOCK(loadedFile->mutex);
            
            
            if(rank==0){
                // Cleanup is handled by the receiver thread
                num_ficheiro[files_processed]++;

            }else{
                cout<< "ENTRA PARA ENVIAR rank= "<< rank << endl;
                
                MPI_Datatype OutputPairtype;
                MPI_Datatype type[2]= { MPI_INT, MPI_SHORT};
                int blocklength[2]={1,1};
                MPI_Aint disp[2]={sizeof(int),sizeof(short)};//vai dar merda
                MPI_Type_struct(2,blocklength,disp,type,&OutputPairtype);
                MPI_Type_commit(&OutputPairtype);
                
                // Send a first message containing the number of elements
                numberOfOutputs = loadedFile->output[rank].size();
                cout << "vai enviar(rank="<<rank <<") :: "<< numberOfOutputs << endl;
                MPI_Send(&numberOfOutputs, 1, MPI_INT, 0, files_processed,MPI_COMM_WORLD);
                
                // And a second with the elements themselves
                cout << "vai enviar 2 (rank="<<rank <<") :: "<< numberOfOutputs << endl;
                MPI_Send(&(loadedFile->output[rank]), loadedFile->output[rank].size(),
                        OutputPairtype, 0, files_processed, MPI_COMM_WORLD);
                
                // Finally cleanup resources used by this file
                pthread_mutex_destroy(&loadedFile->mutex);
                pthread_cond_destroy(&loadedFile->finished_cond);

                delete loadedFile->workVector;
                delete loadedFile->memoryBlock;
                delete loadedFile;

                loadedFile = NULL;
                
            }
            
            files_processed++;
            
        }

        sprintf(inputFileName, INPUT_STRING, files_read);

        readInputFile(inputFileName);

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


        if (rank == 0) {
            // Cleanup is handled by the receiver thread
            num_ficheiro[files_processed]++;

        } else {
            cout << "ENTRA PARA ENVIAR rank= " << rank << endl;

            MPI_Datatype OutputPairtype;
            MPI_Datatype type[2] = {MPI_INT, MPI_SHORT};
            int blocklength[2] = {1, 1};
            MPI_Aint disp[2] = {sizeof (int), sizeof (short)}; //vai dar merda
            MPI_Type_struct(2, blocklength, disp, type, &OutputPairtype);
            MPI_Type_commit(&OutputPairtype);

            // Send a first message containing the number of elements
            numberOfOutputs = loadedFile->output[rank].size();
            cout << "vai enviar(rank=" << rank << ") :: " << numberOfOutputs << endl;
            MPI_Send(&numberOfOutputs, 1, MPI_INT, 0, files_processed, MPI_COMM_WORLD);

            // And a second with the elements themselves
            cout << "vai enviar 2 (rank=" << rank << ") :: " << numberOfOutputs << endl;
            MPI_Send(&(loadedFile->output[rank]), loadedFile->output[rank].size(),
                    OutputPairtype, 0, files_processed, MPI_COMM_WORLD);

            // Finally cleanup resources used by this file
            pthread_mutex_destroy(&loadedFile->mutex);
            pthread_cond_destroy(&loadedFile->finished_cond);

            delete loadedFile->workVector;
            delete loadedFile->memoryBlock;
            delete loadedFile;

            loadedFile = NULL;

        }

        files_processed++;
    }
    
    cout << " acabou :S .......... "<< rank << endl;
    
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

    while (it < inputHandler.end()) {
        totalSize += (*it)->size * sizeof (cell_value);
        totalSize += (*it)->workVector->size() * sizeof (cell_value*);
        totalSize += sizeof (unsigned int);

        for(int i=0; i<num_process; i++){
            totalSize += (*it)->output[i].size() * sizeof ( OutputPair );
        }       

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
    for (int i = 0; i < num_process; i++) {
        fileSize += file->output[i].size();
    
    }
    printf("Found %d outputs\n", fileSize);

    cout << "contador::" << fileSize << endl;
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
    vector< OutputPair >::iterator it;

    cout << "Encontrados: " << fileSize << endl;
    cout << "Extra memory reserved: " << fileSize / (float) (1048576) << " MB\n";

    for (int i = 0; i < num_process; i++) {
        for (it = file->output[i].begin(); it < file->output[i].end(); it++) {

            t_id = (*file->workVector)[((*it).index)];

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
            memcpy(&map[idx], lookupTable[(*it).rule], lookupSizes[(*it).rule]);
            idx += lookupSizes[(*it).rule];
            map[idx++] = '\n';

        }
    }

    map[idx] = '\0';
    close(fd);
    munmap(map, sizeof (map));

    cout << "\nWrite took: " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;

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

    map = static_cast<char*> (mmap(NULL, buffer.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0));

    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    tokenPtr = map;
    cell_value value = c_nextToken(',');

    cell_array array = new cell_value[number_size];
    int ruleIndex = 0, ruleStart;

    while (value >= 0) {
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
    }

    close(fd);
    munmap(map, sizeof (map));

    LoadedFile *newFile = new LoadedFile;

    newFile->size = number_size;
    newFile->memoryBlock = array;
    newFile->workVector = rules;
    newFile->output = new outputVector[num_process];

    cout << "\nRead took: " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;

    return newFile;
}

LoadedFile* FileHandler::readFileMPI(const char* FileName, int row_size, int vector_size, int number_size, int start, int end) {

    clock_t s = clock();

    cell_vector* rules = new cell_vector();
    rules->reserve(vector_size);

    int fd;
    char *map;
    struct stat buffer;
    int status;
    cout << "file name: " << FileName << endl;
    fd = open(FileName, O_RDONLY);


    status = fstat(fd, &buffer);


    if (fd == -1) {
        perror("Error opening file for reading ......");
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
    while (k < end) {


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
    cout << "k:::: " << k << endl;
    close(fd);
    munmap(map, sizeof (map));

    LoadedFile *newFile = new LoadedFile;

    newFile->size = number_size;
    newFile->memoryBlock = array;
    newFile->workVector = rules;
    newFile->output = new outputVector[num_process];
    
    cout << "\nRead took (MPI): " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;

    return newFile;
}

FileHandler::~FileHandler() {
    vector<LoadedFile*>::iterator it = inputHandler.begin();

    while (it < inputHandler.end()) {
        if (*it != NULL) {

            pthread_mutex_destroy(&((*it)->mutex));
            pthread_cond_destroy(&((*it)->finished_cond));

            delete (*it)->output;
            
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

