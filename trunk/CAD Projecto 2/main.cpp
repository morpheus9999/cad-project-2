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

using namespace MPI;

/* 
 * 
 * STRUCTS
 *
 */

// We use this structure to replace having an array of maps of INPUT_SIZE
// <memory optimization: instead of having 2M nodes with an map array of INPUT_SIZE
//  now the size depends on the number of levels with existing states, during input
//  matchmaking all levels have to be searched so order is irrelevant>

struct TreeNode;

struct MapLevel {
    short depth;
    map< cell_value, TreeNode* > level;
};

// Main node structure for the state tree

struct TreeNode {
    vector<MapLevel*> mappedNodes;
};

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

/* 
 * 
 * FUNCTION PROTOTYPES
 *
 */

void* thread_work(void * id);
void addZeroRuleOutput(cell_array input);

void printSM(StateNode* state, int d);
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

TreeNode* root;

/* 
 * 
 * FUNCTIONS
 *
 */

int main(int argc, char** argv) {
    int numprocs, rank, namelen, i;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    
    MPI_Status stat;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(processor_name, &namelen);
    
    cout << numprocs << endl;
    hasZeroRule = false;
    zeroClass = -1;
    

    cell_vector* ruleSet;
    //    ruleSet = fileHandler.readRuleFile("dataset/sm_rules.csv");
    ruleSet = fileHandler.readRuleFile("dataset/THE_PROBLEM/rules2M.csv");
    //    ruleSet = fileHandler.readRuleFile("dataset/xs_rules.csv");

    fileHandler.start();
    
//    inputSet = fileHandler.readInputFile("dataset/THE_PROBLEM/trans_day_1.csv");
    //        inputSet = fileHandler.readInputFile("dataset/sm_input.csv");
    //    inputSet = fileHandler.readInputFile("dataset/xs_input.csv");

    buildStateMachine(ruleSet);
    
#ifdef PTHREADS
    pthread_t thread[NUM_THREADS];
    int threads[NUM_THREADS];
    pthread_attr_t attr;
    int rc;
    long t;
    void *status;

    work_ID = 0;

    // Create mutex for work ID access
    pthread_mutex_init(&mutex_ID, NULL);

    /* Initialize and set thread detached attribute */
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    for (t = 0; t < NUM_THREADS; t++) {
        threads[t]=t;
        rc = pthread_create(&thread[t], &attr, thread_work, (void*) threads[t]);
        if (rc) {
            printf("ERROR; return code from pthread_create() is % d\n", rc);
            exit(-1);
        }
    }

    /* Free attribute and wait for the other threads */
    pthread_attr_destroy(&attr);
    for (t = 0; t < NUM_THREADS; t++) {
        rc = pthread_join(thread[t], &status);
        if (rc) {
            printf("ERROR; return code from pthread_join() is % d\n", rc);
            exit(-1);
        }
    }
#endif
    
    pthread_mutex_destroy(&mutex_ID);
    pthread_exit(NULL);

    return 0;
}

void* thread_work(void* id) {
#ifdef PTHREADS
//    
//    int my_id;
//    int num_worked, startIndex, fileId = 0;
//
//    LOCK(mutex_ID);
//    my_id = work_ID++;
//    cout << "Thread " << my_id << " reporting in" << endl;
//    UNLOCK(mutex_ID);
//
//    // Get a work item - Current Work File (CWF)
//    LoadedFile* cwf = fileHandler.getNextWorkFile(fileId);
//    cell_vector::iterator input_it;
//    
//    do {
//    
//    while (true) {
//        
//        LOCK(cwf->mutex);
//        startIndex = cwf->availableWork - 1;
//        cwf->availableWork -= WORK_RANGE;
//        UNLOCK(cwf->mutex);
//
//        if (startIndex < 0) {
//            break;
//        }
//        
//        num_worked = 0;
//        input_it = cwf->workVector->begin() + startIndex;
//
//        map< cell_value, ContainFirst* >::iterator mit;
//        map< level, vector<StateNode*>* >::iterator l_it;
//
//        vector<StateNode*>::iterator it;
//
//        StateNode cmp;
//        StateNode *state;
//        ContainFirst *fs;
//
//        while (num_worked < WORK_RANGE) {
//            
//            for (int i = 0; i < INPUT_SIZE; i++) {
//                cmp.value = (*input_it)[i];
//
//                mit = mappedIndexes[i].find(cmp.value);
//
//                if (mit != mappedIndexes[i].end()) {
//                    fs = (*mit).second;
//
//                    l_it = fs->next.begin();
//
//                    if (l_it->first == i) {
//                        for (it = l_it->second->begin(); it < l_it->second->end(); it++) {
//                            OUTPUT(cwf, *input_it, (*it)->value, my_id);
//                        }
//
//                        l_it++;
//                    }
//
//                    while (l_it != fs->next.end()) {
//
//                        cmp.value = (*input_it)[l_it->first];
//
//                        it = lower_bound(l_it->second->begin(), l_it->second->end(), &cmp, compareObj);
//
//                        for (; it != l_it->second->end() && (*it)->value == cmp.value; it++) {
//
//                            state = (*it)->next;
//
//                            while (true) {
//
//                                if (state->next != NULL) {
//                                    if (state->value != (*input_it)[state->index]) {
//
//                                        break;
//                                    } else {
//                                        state = state->next;
//                                    }
//                                } else {
//                                    OUTPUT(cwf, *input_it, state->value, my_id);
//                                    break;
//                                }
//                            }
//                        }
//
//                        l_it++;
//                    }
//                }
//            }
//
//            if (hasZeroRule)
//                OUTPUT(cwf, *input_it, zeroClass, my_id);
//
//            num_worked++;
//            input_it--;
//        }
//    }
//    
//    fileId++;
//    
//    cwf->finished();
//    
//    cwf = fileHandler.getNextWorkFile(fileId);
//         * 
//    } while(cwf != NULL);
//    
//    LOCK(mutex_ID);
//    cout << "Thread " << my_id << " handled " << num_worked << " exited\n";
//    UNLOCK(mutex_ID);

    pthread_exit((void*) 0);
#endif
}

//void printSM(StateNode* sm, int d) {
//
//    for (int i = 0; i < d; i++) {
//        cout << "  ";
//    }
//
//    cout << sm->index << " " << sm->value << endl;
//
//    if (sm->next != NULL)
//        printSM(sm->next, d + 1);
//
//}

void buildStateMachine(cell_vector* ruleSet) {

    cell_vector::iterator rule_it = ruleSet->begin();

    clock_t s = clock();

    TreeNode* ptr_node;

    root = new TreeNode;
    root->mappedNodes.push_back(NULL);
    
    short depth;

    while (rule_it < ruleSet->end()) {
        
        ptr_node = root;
        depth = 0;
        
        for (int i = 0; i < INPUT_SIZE; i++) {
            if ((*rule_it)[i] != 0) {
                
                TreeNode* newState = new TreeNode;
                
                newState->index = i;
                newState->value = (*rule_it)[i];

                ;

//                        map_it = last->next.lower_bound(i);
//
//                        if (map_it != last->next.end() && map_it->first == i) {
//                            //                            printf("added in %d value %d\n", i, (*rule_it)[i]);
//                            map_it->second->push_back(newState);
//
//                        } else {
//                            //                            printf("created new level %d with %d\n", i, (*rule_it)[i]);
//                            map_it = last->next.insert(map_it, pair<level, vector<StateNode*>*>(i, new vector<StateNode*>));
//                            map_it->second->push_back(newState);
//                        }

                ptr_node = newState;
            }
        }

        if (ptr_node != NULL) {

            StateNode *newState = new StateNode;
            newState->index = -1;
            newState->value = (*rule_it)[INPUT_SIZE];

        } else {
            hasZeroRule = true;
            zeroClass = (*rule_it)[INPUT_SIZE];
        }

        rule_it++;
    }

    // Release space reserved by file handler for rules
    fileHandler.freeRuleSpace();

    cout << "\nState machine build took: " << (((double) clock() - s) / CLOCKS_PER_SEC) << endl;
}
