/* 
 * File:   main.cpp
 * Author: guilhermerodrigues
 *
 * Created on May 17, 2011, 2:30 PM
 */

#include <cstdlib>
#include <iostream>
#include <pthread.h>
#include <omp.h>
#include <cmath>
#include <time.h>


#include "mpi.h"

#include "FileHandler.h"

using namespace std;

/* 
 * 
 * STRUCTS
 *
 */

// We use this structure to replace having an array of maps of INPUT_SIZE
// <memory optimization: instead of having 2M nodes with an map array of INPUT_SIZE
//  now the size depends on the number of levels with existing states, during input
//  matchmaking all levels have to be searched so order is irrelevant>

// Main node structure for the state tree

struct StateNode {
    cell_value index;
    cell_value value;

    StateNode* next;
};

struct StateCompare {
    bool operator() (const StateNode* lhs, const StateNode* rhs) const {
        return lhs->value < rhs->value;
    }
};

typedef vector<StateNode*> StateVector;
typedef map< Depth, StateVector > LevelMap;

struct ContainFirst {
    LevelMap* next;
};

/* 
 * 
 * FUNCTION PROTOTYPES
 *
 */

void thread_work(int rank);
void addZeroRuleOutput(cell_array input);

void buildStateMachine(cell_vector* ruleSet);

/* 
 * 
 * GLOBAL VARIABLES
 *
 */

int work_ID;
pthread_mutex_t mutex_ID;

bool hasZeroRule;
cell_value zeroClass;
StateCompare compareObj;

cell_vector* inputSet;

FileHandler fileHandler;
StateNode finalState[NUM_CLASS];
vector<StateNode*> startIndex[INPUT_SIZE];

ContainFirst mappedIndexes[INPUT_SIZE][NUM_RANGE];

/* 
 * 
 * FUNCTIONS
 *
 */

int main(int argc, char** argv) {
    //264345972
    cell_vector* ruleSet;
    time_t start_time= 0;
    time_t end_time= 0;
    cout<< "MPI" << endl;
    
    
    
#ifdef MPI
    
    MPI_Status status;
    MPI_Comm comm;
    comm = MPI_COMM_WORLD;
    
    int numprocs=1, rank=0, namelen;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    double wtime;
    
    wtime = MPI_Wtime();
    
    //MPI_Status stat;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name, &namelen);
    
    //numprocs=1;
    cout <<"NUM processes:" <<numprocs << endl;
    cout <<"NUM rank:" <<rank << endl;
    cout << "NOME DO PC:::" << processor_name << endl;
    fileHandler.setRank(rank);
    fileHandler.setStat(status);
    fileHandler.setNumProcess(numprocs);
    

        cout  << rank*RULE_NUM << " " << RULE_NUM/numprocs << " " << endl;
        ///Users/jojo/Documents/DEI/CAD/CAD2/trunk/CAD Projecto 2/         
        ruleSet = fileHandler.readRuleFileMPI("dataset/THE_PROBLEM/rules2M.csv",rank*RULE_NUM,RULE_NUM/numprocs);
        
#else
    
    //    ruleSet = fileHandler.readRuleFile("dataset/sm_rules.csv");
    ruleSet = fileHandler.readRuleFile("dataset/THE_PROBLEM/rules2M.csv");
    //    ruleSet = fileHandler.readRuleFile("dataset/xs_rules.csv");
    
#endif
    
    threadPair tp = fileHandler.start();
    
//    inputSet = fileHandler.readInputFile("dataset/THE_PROBLEM/trans_day_1.csv");
    //        inputSet = fileHandler.readInputFile("dataset/sm_input.csv");
    //    inputSet = fileHandler.readInputFile("dataset/xs_input.csv");
    //cout << "entra" << endl;
    start_time=time(NULL);
    buildStateMachine(ruleSet);
    end_time =time(NULL);
    cout << "Tempo makina estados::  "<< end_time-start_time << endl;
    //cout << "Printing tree\n";
    //printSM(root, 0);
    
   // return 0;
    
    thread_work(rank);
    
    
  
#ifdef MPI
    
    if(rank==0){
        pthread_join(tp.first,NULL);
        pthread_join(tp.second,NULL);
        
        
        wtime = MPI_Wtime() - wtime;
        cout << "TEMPO::: " << wtime << endl; 
        
    }else{
        pthread_join(tp.first,NULL);
    }
    cout << "::::ACABA:::: " <<rank  << endl;
    
    MPI_Finalize();
    
 #endif   

    
    
    pthread_exit((void*) 0);    
    return 0;
}

void thread_work(int rank) {

    int fileId = 0;
    
    // Get a work item - Current Work File (CWF)
    
    
    
    LoadedFile* currentWorkFile = fileHandler.getNextWorkFile(fileId);
    cell_vector::iterator input_it;

    LevelMap::iterator depth_iterator;

    StateVector::iterator stateMachine_iterator;

    StateNode valueToFind;
    StateNode *state;
    ContainFirst *first_state;
    
    clock_t s = clock();
    
    int input_index=0;
    
    do {

        input_it = currentWorkFile->workVector->begin();

        // While there is work in this work block
        while (input_it < currentWorkFile->workVector->end()) {

            // For each input value inside the selected input
            for (int i = 0; i < INPUT_SIZE; i++) {
                valueToFind.value = (*input_it)[i];
                
                // Check if there is a rule that begins with the same value

                first_state = &(mappedIndexes[i][valueToFind.value]);
                
                if (first_state->next != NULL) {

                    // If so perform the next step - search for one that matches the second value
                    depth_iterator = first_state->next->begin();

                    if (depth_iterator->first == i) {
                        // If, for this rule there are no more distinct values required,
                        //signal the fileHandler thread of a new matching pair (input, rule)
                        for (stateMachine_iterator = depth_iterator->second.begin()
                                ; stateMachine_iterator < depth_iterator->second.end()
                                ; stateMachine_iterator++) {
                            OUTPUT(currentWorkFile, input_index, (*stateMachine_iterator)->value,rank);
                        }

                        depth_iterator++;
                    }

                    // Now we need to 
                    while (depth_iterator != first_state->next->end()) {

                        valueToFind.value = (*input_it)[depth_iterator->first];

                        stateMachine_iterator = lower_bound(depth_iterator->second.begin()
                                , depth_iterator->second.end()
                                , &valueToFind
                                , compareObj);

                        for (; stateMachine_iterator != depth_iterator->second.end()
                                && (*stateMachine_iterator)->value == valueToFind.value
                                ; stateMachine_iterator++) {

                            state = (*stateMachine_iterator)->next;

                            while (true) {

                                if (state->next != NULL) {
                                    if (state->value != (*input_it)[state->index]) {

                                        break;
                                    } else {
                                        state = state->next;
                                    }
                                } else {
                                    OUTPUT(currentWorkFile, input_index, state->value,rank);
                                    break;
                                }
                            }
                        }

                        depth_iterator++;
                    }
                }
            }

            if (hasZeroRule)
                OUTPUT(currentWorkFile, input_index, zeroClass,rank);

            input_it++;
            input_index++;
        }

        fileId++;
        
        currentWorkFile->finished();

        currentWorkFile = fileHandler.getNextWorkFile(fileId);

    } while (currentWorkFile != NULL);

    cout << "\nExecution took: " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;
    
    
}

void buildStateMachine(cell_vector* ruleSet) {

    cell_vector::iterator rule_it = ruleSet->begin();

    clock_t s = clock();

    StateNode *ptr;
    
    LevelMap::iterator map_it;

    StateVector* stateVector;
    
    StateNode* newState;
    cell_value currentValue;
    
    int state_free = MAX_STATE_CACHE_SIZE;
    StateNode* state_cache = new StateNode[MAX_STATE_CACHE_SIZE];
    
    Depth         cache_depth;
    cell_value    cache_value;
    ContainFirst* cache_firstState;
    
    Depth depth;
    
    int contador=0;
    while (rule_it < ruleSet->end()) {
        
        ptr = NULL;
        depth = 0;
        
        for (int i = 0; i < INPUT_SIZE; i++) {
            if ((*rule_it)[i] != 0) {
                
                depth++;

                currentValue = (*rule_it)[i];
                
                if (depth > 1) {
                    
                    newState = &state_cache[state_free--];
                    
                    newState->index = i;
                    newState->value = currentValue;
                    
                    if (depth > 2) {
                        
                        ptr->next = newState;
                        
                    } else {
                        
                        stateVector = &((*cache_firstState->next)[i]);
                        stateVector->push_back(newState); // Consuming 30% of the total time.
                        
                    }
                    
                    ptr = newState;
                    
                    if (state_free < 0) {
                        
                        state_free = NEW_STATE_CACHE_SIZE;
                        state_cache = new StateNode[NEW_STATE_CACHE_SIZE];
                        
                    }

                } else {
                    
                    if (cache_value != currentValue || cache_depth != i) {
                        
                        cache_depth = i;
                        cache_value = currentValue;
                        cache_firstState = &(mappedIndexes[i][currentValue]);
                        
                        if(cache_firstState->next == NULL) {
                            cache_firstState->next = new LevelMap();
                        }
                        
                    }
                }
            }
        }

        currentValue = (*rule_it)[INPUT_SIZE];
        
        if (depth != 0) {
            contador++;
            
            newState = &state_cache[state_free--];
            
            newState->index = RULE_ACCEPTED_DEPTH;
            newState->value = currentValue;

            if (depth > 1) {
                
                newState->next = NULL;
                ptr->next = newState;
                
            } else {
                
                stateVector = &((*cache_firstState->next)[cache_depth]);
                stateVector->push_back(newState);
                
            }
            
            if (state_free < 0) {         
               state_free = NEW_STATE_CACHE_SIZE;
               state_cache = new StateNode[NEW_STATE_CACHE_SIZE];      
            }
            
        } else {
            hasZeroRule = true;
            zeroClass = currentValue;
        }

        rule_it++;
    }
    
    
    
    cout << "contador dentro state::"<<contador << endl; 
    //    for (int i = 0; i < INPUT_SIZE; i++)
    //        cout << countIdx[i] << " ";
    //
    //    cout << endl << " average size of rules: " << sizes / (float) 2000000 << endl;
    //    cout << endl << " number of empty rules: " << nulls << endl;

    for (int i = 0; i < INPUT_SIZE; i++) {
        for (int j = 0; j < NUM_RANGE; j++) {
            if (mappedIndexes[i][j].next != NULL) {
                for (map_it = mappedIndexes[i][j].next->begin(); map_it != mappedIndexes[i][j].next->end(); map_it++) {
                    sort(map_it->second.begin(), map_it->second.end(), compareObj);
                }
            }
        }
    }
    
    // Release space reserved by file handler for rules
    fileHandler.freeRuleSpace();

    cout << "\nState machine build took: " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;
}
