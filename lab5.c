#include <pthread.h>
#include <mpi.h>
#include <stdio.h>
#include <math.h>
#include <stdbool.h>
#include <stdlib.h>
 
#define TaskList struct _TaskList
 
TaskList {
    int repeatNum;
};
 
const int LIST_SIZE = 1000;
const int LIST_COUNT = 20;
const int K = 10000000;
 
pthread_mutex_t mutex;
 
TaskList* taskPart;
 
int taskNum;
int iterCounter;
bool hasNextTask;
bool tasksLeft;
 
int procCount;
int askingProc;
int rank;
 
void generateTasks(int iter) {  
    printf("generating tasks (%d), rank = %d\n", iter, rank);
    for (int i = 0; i < LIST_SIZE / procCount ; ++i ) {
        taskPart[i].repeatNum = K / (rank + 1) + i;
    } 
}
 
void* requestsHandle() {
    int outOfTasks = -1;

    while (true) {
        MPI_Recv(&askingProc, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
        if (askingProc == - 1)      // out of lists
            break;

        pthread_mutex_lock(&mutex);
        int currentTaskNum = taskNum;
        pthread_mutex_unlock(&mutex);

        if (currentTaskNum + 1 >= LIST_SIZE / procCount ) {
            MPI_Send(&outOfTasks, 1, MPI_INT, askingProc, 2, MPI_COMM_WORLD);
        }
        else {
            
            pthread_mutex_lock(&mutex);

            int task = taskPart[taskNum].repeatNum;
            printf("proc #%d sends task #%d = %d to proc #%d\n", rank, currentTaskNum, task, askingProc);
            taskNum++;
            
            pthread_mutex_unlock(&mutex);

            MPI_Send(&task, 1, MPI_INT, askingProc, 2, MPI_COMM_WORLD);
        }
    }
    printf("requestsHandle() has finished, rank = %d\n", rank);
}
 
 
void* execute() {
    int currentTask;
    int currentTaskNum;
    while (hasNextTask) {

        pthread_mutex_lock(&mutex);
        if (LIST_SIZE / procCount <= taskNum) {
            tasksLeft = false;
        }
        else {
            tasksLeft = true;
        }
        
        if (tasksLeft) {
            currentTask = taskPart[taskNum].repeatNum;
            currentTaskNum = taskNum;
            taskNum++;
        }
        pthread_mutex_unlock(&mutex);

        if (!tasksLeft) {
            for (int procToAsk = 0; procToAsk < procCount; procToAsk++) {
                if (procToAsk != rank) {

                    MPI_Send(&rank, 1, MPI_INT, procToAsk, 1, MPI_COMM_WORLD);
                    MPI_Recv(&currentTask, 1, MPI_INT, procToAsk, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    
                    if (currentTask == -2)              // if no lists left
                        continue;
                    if (currentTask != -1) {            // if proc has tasks
                        tasksLeft = true;

                        pthread_mutex_lock(&mutex);
                        currentTaskNum = taskNum;
                        taskNum++;
                        pthread_mutex_unlock(&mutex);

                        break;
                    }
                }
            }
        }
     
        if (tasksLeft) {
            double result = 0.0;
         
            printf("proc with rank = %d starts task #%d = %d\n", rank, currentTaskNum, currentTask);
            for (int i = 0; i < currentTask; ++i)
                result += sqrt(i);
            printf("proc with rank = %d finished task #%d = %d\n", rank, currentTaskNum, currentTask);
 
        }
        else {
            int outOfLists = -1;

            MPI_Barrier(MPI_COMM_WORLD);

            iterCounter++;

            if (iterCounter >= LIST_COUNT) {
                hasNextTask = false;
                MPI_Send(&outOfLists, 1, MPI_INT, rank, 1, MPI_COMM_WORLD); 
            }
            else {
                pthread_mutex_lock(&mutex);
                taskNum = 0;
                pthread_mutex_unlock(&mutex);
 
                tasksLeft = true;
                
                generateTasks(iterCounter);
            }
        }
    }
    printf("execute() has been finished, rank = %d\n", rank);
}
 
int main(int argc, char* argv[]) {
    pthread_t executor;
    pthread_t requestsHandler;
 
    int* provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, provided);
 
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &procCount);
 
    taskPart = (TaskList*)malloc(sizeof(TaskList) * LIST_SIZE / procCount);
    
    generateTasks(0);
    
    hasNextTask = true;
    tasksLeft = true;
    taskNum = 0;
    iterCounter = 0;
    
    pthread_mutex_init(&mutex, NULL);
 
    MPI_Barrier(MPI_COMM_WORLD);
    
    pthread_create(&executor, NULL, (void *(*)(void *))execute, NULL);
    pthread_create(&requestsHandler, NULL, (void *(*)(void *))requestsHandle, NULL);
    
    pthread_join(executor, NULL);
    pthread_join(requestsHandler, NULL);
 
    pthread_mutex_destroy(&mutex);
    free(taskPart);
 
    MPI_Finalize();
 
    return 0;
}
