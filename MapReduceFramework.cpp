//
// Created by rubi1993 on 5/13/17.
//
#include "MapReduceFramework.h"
#include <fstream>
#include <iostream>  // std::cout
#include <deque>          // std::deque
#include <list>           // std::list
#include <queue>          // std::queue
#include <pthread.h>
#include <map>
#include <mutex>
#include <pthread.h>
#include <semaphore.h>
#include <algorithm>
#include <math.h>
#include <stdlib.h>
#include <iomanip>
#include <sys/time.h>

#define SEC_TO_NANO 10000000000.0
#define MICRO_TO_NANO 1000.0
#define FAIL -1

template<typename T>
/**
 * comperator for k2.
 */
class cmpForK2 : public std::binary_function<T, T, bool>{
public:
    bool operator()(T k1, T k2) const {
        return *k1 < *k2;
    }
};


typedef std::vector<std::pair<k2Base*,std::vector<v2Base*>>> TO_REDUCE;
typedef std::map<k2Base*,std::vector<v2Base*>, cmpForK2<const k2Base*>> MID_ITEMS_VEC;
typedef std::vector<std::pair<k2Base*, v2Base*>> VEC_2;
typedef std::vector<std::pair<k3Base*, v3Base*>> VEC_3;
typedef std::map<pthread_t,VEC_2> CONT_ITEM;
typedef std::map<pthread_t,VEC_3> CONT_REDUCE;

bool myFunction(std::pair<k3Base*, v3Base*> p1, std::pair<k3Base*, v3Base*> p2){
    return *(p1.first) < *(p2.first);
};


TO_REDUCE fromMapToVector;//the data for the reduce function in a vector.
CONT_ITEM containersFromMap;// the containers that produced by the map threads.
MID_ITEMS_VEC dataForReduce;// what the shuffle did after arranging the map output.
IN_ITEMS_VEC dataForMap;// data for the map function.
OUT_ITEMS_VEC returnVec;// the return vector.
CONT_REDUCE containersFromReduce;// // the containers that produced by the reduce threads.
bool deleteK2V2;
int positionInMapVec;
unsigned long sizeOfMapVec;
int positionInReduceVec;
unsigned long sizeOfReduceVec;
int numOfThreads;
int numOfTerminatedThreads;
pthread_t *listOfThreads; // list of threads.
std::map <pthread_t,std::mutex*> pthreadAndMutexMap;// list of mutexes for each map thread.
std::map <pthread_t,int> pthreadAndIntMap;// list of number of inserted k2v2 pairs from each thread.
sem_t shuffleSemaphore;// semaphore for the shuffle.
std::mutex mutexForContainer;// mutex for the big container before map threads starting to work.
std::mutex mutexForLogFile;// mutex for log file!
MapReduceBase *myMapReduce;
std::ofstream logFile;
time_t rawtime;
struct tm * timeinfo;

/**
 * start time for measuring.
 */
static struct timeval startTime;

/**
 * end time for measuring.
 */
static struct timeval endTime;
/**


/**
 * the function exit the profram in a case of error.
 * @param indic
 */
void errorHandling(int indic){
    if(indic < 0){
        printf("error\n");
        exit(-1);
    }
}

/**
 * the function that the execMap threads execute,
 * running the map functions.
 * @return
 */
void *mapForThread(void*){
    mutexForContainer.lock();
    mutexForContainer.unlock();
    int curPosition;
    mutexForContainer.lock();
    int numOfRuns = std::min((int)(sizeOfMapVec - positionInMapVec),10);
    curPosition = positionInMapVec;
    positionInMapVec += numOfRuns;
    mutexForContainer.unlock();
    while (numOfRuns > 0){
        for(int i = 0; i < numOfRuns; i++){
            myMapReduce->Map(dataForMap[curPosition + i].first,dataForMap[curPosition + i].second);
        }
        mutexForContainer.lock();
        numOfRuns = std::min((int)(sizeOfMapVec - positionInMapVec),10);
        curPosition = positionInMapVec;
        positionInMapVec += numOfRuns;
        mutexForContainer.unlock();
    }
    numOfTerminatedThreads++;
    if(numOfTerminatedThreads == numOfThreads){
        errorHandling(sem_post(&shuffleSemaphore));
    }
    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    mutexForLogFile.lock();
    logFile.open (".MapReduceFramework.log", std::ios::app);
    logFile << "Thread ExecMap terminated "<<asctime (timeinfo) << "\n";
    logFile.close();
    mutexForLogFile.unlock();
    pthread_exit(NULL);
}


void *reduceForThread(void*){
    mutexForContainer.lock();
    mutexForContainer.unlock();
    int curPosition;
    mutexForContainer.lock();
    int numOfRuns = std::min((int)(sizeOfReduceVec - positionInReduceVec),10);
    curPosition = positionInReduceVec;
    positionInReduceVec += numOfRuns;
    mutexForContainer.unlock();
//    std::cout << "numofruns in reduce " << numOfRuns << "\n";
    while (numOfRuns > 0){
        for(int i = 0; i < numOfRuns; i++){
            myMapReduce->Reduce(fromMapToVector[curPosition + i].first,fromMapToVector[curPosition + i].second);
        }
        mutexForContainer.lock();
        numOfRuns = std::min((int)(sizeOfReduceVec - positionInReduceVec),10);
        curPosition = positionInReduceVec;
        positionInReduceVec += numOfRuns;
        mutexForContainer.unlock();
    }
    numOfTerminatedThreads++;
    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    mutexForLogFile.lock();
    logFile.open (".MapReduceFramework.log", std::ios::app);
    logFile << "Thread ExecReduce terminated "<<asctime (timeinfo) << "\n";
    logFile.close();
    mutexForLogFile.unlock();
    pthread_exit(NULL);
}

void *shuffleFunc(void*){
    std::map<k2Base*,std::vector<v2Base*>>::iterator key2fromMidItems;
    while (true) {
//        std::cout << "im before sema"<<"\n" ;
        errorHandling(sem_wait(&shuffleSemaphore));//tries to start shuffeling

//         std::cout << "numofterminated" <<numOfTerminatedThreads<<"\n" ;
//          std::cout << "numofthreads"<< numOfThreads <<"\n" ;
        if(numOfTerminatedThreads == numOfThreads)
            break;
        //succeded now finds the avilable continer
        for (auto it = pthreadAndIntMap.begin();it != pthreadAndIntMap.end(); ++it) { // going over all pthreads and their
            // avilable numOf K2 to be taken!
            pthreadAndMutexMap[(*it).first]->lock();//locking the mutex of this specifc thread container.
//            std::cout << "num of files loop 1 "<< (*it).second <<"\n" ;
            if ((*it).second != 0) {
                (*it).second--;
                std::pair<k2Base *, v2Base *> k2v2 = containersFromMap[(*it).first].back();
                containersFromMap[(*it).first].pop_back();
                pthreadAndMutexMap[(*it).first]->unlock();
                if(!(dataForReduce.find(k2v2.first) == dataForReduce.end())){
                    dataForReduce[k2v2.first].push_back(k2v2.second);
                    if(deleteK2V2)
                        delete k2v2.first;
                }
                else{
                    dataForReduce[k2v2.first].push_back(k2v2.second);
                }
            } else{
                pthreadAndMutexMap[(*it).first]->unlock();
            }
        }
    }
    for (auto it = pthreadAndIntMap.begin();it != pthreadAndIntMap.end(); ++it) { // going over all pthreads and their
        // avilable numOf K2 to be taken!
//        std::cout << "num of files loop 2 "<< (*it).second <<"\n" ;
        for (int j = 0; j < (*it).second;j++) {
            //(*it).second--; seems unneeded
            std::pair<k2Base *, v2Base *> k2v2 = containersFromMap[(*it).first].back();
            containersFromMap[(*it).first].pop_back();
            if(!(dataForReduce.find(k2v2.first) == dataForReduce.end())){
                dataForReduce[k2v2.first].push_back(k2v2.second);
                delete k2v2.first;
            }
            else{
                dataForReduce[k2v2.first].push_back(k2v2.second);
            }
        }
    }
//    std::cout << "data for reduce size" << dataForReduce.size() << "\n";
    //todo: shuffle is done, time to clear the container "containersFromMap" and add the dead shuffle thread to our counter
    for(auto it = containersFromMap.begin(); it != containersFromMap.end(); it++){
        (*it).second.clear();
    }
    containersFromMap.clear();
    numOfTerminatedThreads++;
    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    mutexForLogFile.lock();
    logFile.open (".MapReduceFramework.log", std::ios::app);
    logFile << "Thread Shuffle terminated "<<asctime (timeinfo) << "\n";
    logFile.close();
    mutexForLogFile.unlock();
    pthread_exit(NULL);
}


/**
 * the function creates the threads and the container.
 */

void createMapAndShuffleThreads(){
    errorHandling(sem_init(&shuffleSemaphore,0,0));
    int indicator;
    for (int i = 0; i < numOfThreads; ++i) {
        time ( &rawtime );
        timeinfo = localtime ( &rawtime );
        mutexForLogFile.lock();
        logFile.open (".MapReduceFramework.log", std::ios::app);
        logFile << "Thread ExecMap created "<<asctime (timeinfo) << "\n";
        logFile.close();
        mutexForLogFile.unlock();
        indicator= pthread_create(&listOfThreads[i], NULL, mapForThread,NULL);
        if(indicator){
            printf("problem with creating thread\n");
        }
    }
    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    mutexForLogFile.lock();
    logFile.open (".MapReduceFramework.log", std::ios::app);
    logFile << "Thread Shuffle created "<<asctime (timeinfo) << "\n";
    logFile.close();
    mutexForLogFile.unlock();
    indicator= pthread_create(&listOfThreads[numOfThreads], NULL, shuffleFunc,NULL);
    if(indicator){
        printf("problem with creating shuffle thread\n");
    }
    for (int i = 0; i < numOfThreads; ++i){
        std::vector<std::pair<k2Base*, v2Base*>> tempv;
        containersFromMap[listOfThreads[i]]=tempv;
        std::mutex *tempMutex = new std::mutex();
        pthreadAndMutexMap[listOfThreads[i]] = tempMutex;
        pthreadAndIntMap[listOfThreads[i]] = 0;
    }
    //initilaize the semaphore
//    printf("I did sem_init\n");
}


void createReduceThreads(){
    int indicator;

    for (int i = 0; i < numOfThreads; ++i) {
        time ( &rawtime );
        timeinfo = localtime ( &rawtime );
        mutexForLogFile.lock();
        logFile.open (".MapReduceFramework.log", std::ios::app);
        logFile << "Thread ExecReduce created "<<asctime (timeinfo) << "\n";
        logFile.close();
        mutexForLogFile.unlock();
        indicator= pthread_create(&listOfThreads[i], NULL, reduceForThread,NULL);
        if(indicator){
            printf("problem with creating thread\n");
        }
    }
    for (int i = 0; i < numOfThreads; ++i){
        std::vector<std::pair<k3Base*, v3Base*>> tempv;
        containersFromReduce[listOfThreads[i]]=tempv;
    }
}

void Emit2(k2Base* k2, v2Base* v2){
    pthread_t curThread =pthread_self();
    pthreadAndMutexMap[curThread]->lock();
    pthreadAndIntMap[curThread]++;
    containersFromMap[curThread].push_back(std::pair<k2Base*,v2Base*>(k2,v2));
    errorHandling(sem_post(&shuffleSemaphore));
    pthreadAndMutexMap[curThread]->unlock();
}

void Emit3 (k3Base* k3, v3Base* v3){
    pthread_t curThread =pthread_self();
    containersFromReduce[curThread].push_back(std::pair<k3Base*,v3Base*>(k3,v3));
}
void prepareforMap(){

//    std::cout << "size of map vec "<< dataForMap.size() <<"\n" ;
    mutexForContainer.lock();
    createMapAndShuffleThreads();
    mutexForContainer.unlock();

}

void prepareforReduce(){
    std::map<k2Base*,std::vector<v2Base*>>::iterator fromMapToVecIt;
    for (auto fromMapToVecIt = dataForReduce.begin();fromMapToVecIt!= dataForReduce.end(); ++fromMapToVecIt) {
//        printf("loop of prepare reduce\n");
        fromMapToVector.push_back(std::pair<k2Base*,std::vector<v2Base*>>((*fromMapToVecIt).first,(*fromMapToVecIt).second));
    }
    mutexForContainer.lock();
    sizeOfReduceVec = fromMapToVector.size();
//    std::cout << "size of reduce vec "<< sizeOfReduceVec <<"\n" ;
    createReduceThreads();
    mutexForContainer.unlock();



}

OUT_ITEMS_VEC outPut(){
    std::map<k2Base*,std::vector<v2Base*>>::iterator lastIterator;
    for (auto lastIterator = containersFromReduce.begin();lastIterator != containersFromReduce.end(); ++lastIterator) {
        returnVec.insert(returnVec.end(),(*lastIterator).second.begin(),(*lastIterator).second.end());
    }
    std::sort (returnVec.begin(), returnVec.end(),myFunction);
    for(auto it = containersFromReduce.begin(); it != containersFromReduce.end(); it++){
        (*it).second.clear();
    }
    containersFromReduce.clear();
    return returnVec;
}

/*
 * the function measure the time between start and end timevals.
 * @param iter number of iterations.
 * @return

 */
double timer(){
    return ((endTime.tv_sec  - startTime.tv_sec)*SEC_TO_NANO + (endTime.tv_usec - startTime.tv_usec)*MICRO_TO_NANO);

};

OUT_ITEMS_VEC RunMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_VEC& itemsVec,
                                    int multiThreadLevel, bool autoDeleteV2K2){
//    std::cout << "size of items vec " << itemsVec.size() <<"\n";
    mutexForLogFile.lock();
    logFile.open (".MapReduceFramework.log", std::ios::app);
    logFile << "RunMapReduceFramework started with "<< multiThreadLevel <<" threads\n";
    logFile.close();
    mutexForLogFile.unlock();
    deleteK2V2 = autoDeleteV2K2;
    numOfTerminatedThreads = 0;
    dataForMap = itemsVec;
    numOfThreads = multiThreadLevel;
    listOfThreads = new pthread_t[numOfThreads + 1];
    positionInMapVec = 0;
    positionInReduceVec = 0;
    myMapReduce = &mapReduce;
    sizeOfMapVec = itemsVec.size();
    if(gettimeofday(&startTime,NULL)==FAIL){
        printf("problem with time measuring");

    };
    prepareforMap();
    while (numOfTerminatedThreads < numOfThreads+1){
    }

    if(gettimeofday(&endTime,NULL)==FAIL){
        printf("problem with time measuring");
    };
    mutexForLogFile.lock();
    logFile.open (".MapReduceFramework.log", std::ios::app);
    logFile << "Map and Shuffle took "<< timer()<<" ns \n";
    logFile.close();
    mutexForLogFile.unlock();
    // reduce part
    for(auto it = dataForMap.begin();it != dataForMap.end();it++){
        delete (*it).first;
        delete (*it).second;
    }
    dataForMap.clear();
    pthreadAndIntMap.clear();
    delete[](listOfThreads);
    listOfThreads = new pthread_t[numOfThreads];
    numOfTerminatedThreads = 0;
    if(gettimeofday(&startTime,NULL)==FAIL){
        printf("problem with time measuring");

    };
    prepareforReduce();
    while (numOfTerminatedThreads < numOfThreads){}
    if(gettimeofday(&endTime,NULL)==FAIL){
        printf("problem with time measuring");

    };
    mutexForLogFile.lock();
    logFile.open (".MapReduceFramework.log", std::ios::app);
    logFile << "Reduce took "<< timer()<<" ns \n";
    logFile.close();
    mutexForLogFile.unlock();
    if(deleteK2V2){
        for(auto it = dataForReduce.begin();it != dataForReduce.end();it++){
            (*it).second.clear();
        }
        dataForReduce.clear();
        for(auto it = fromMapToVector.begin(); it != fromMapToVector.end(); it++){
            delete (*it).first;
            for(auto it2 = (*it).second.begin(); it2 != (*it).second.end(); it2++){
                delete *it2;
            }
            (*it).second.clear();
        }
        fromMapToVector.clear();
    }
    for(auto it = pthreadAndMutexMap.begin(); it != pthreadAndMutexMap.end(); it++) {
        delete (*it).second;
    }
    pthreadAndMutexMap.clear();
    delete [](listOfThreads);
    mutexForLogFile.lock();
    logFile.open (".MapReduceFramework.log", std::ios::app);
    logFile << "RunMapReduceFramework finished\n";
    logFile.close();
    mutexForLogFile.unlock();
    return outPut();
}