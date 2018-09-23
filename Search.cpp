//
// Created by rubi1993 on 5/14/17.
//
#include "MapReduceClient.h"
#include "MapReduceFramework.h"
#include <string>
#include <iostream>
#include <fstream>
#include <dirent.h>

using namespace std;
class fileName:public k1Base, public k2Base, public k3Base
{
public:
    std::string fileNameStr;
    fileName(std::string s){
        fileNameStr = s;
    }
    bool operator<(const k1Base &other) const {
        fileName *f = (fileName*)&other;
        return ((fileName*)this)->fileNameStr < f->fileNameStr;
    }
    bool operator<(const k2Base &other) const {
        fileName *f = (fileName*)&other;
        return ((fileName*)this)->fileNameStr < f->fileNameStr;
    }
    bool operator<(const k3Base &other) const {
        fileName *f = (fileName*)&other;
        return ((fileName*)this)->fileNameStr < f->fileNameStr;
    }
    ~fileName(){
        fileNameStr.clear();
    }
};

class subString:public v1Base
{
public:
    std::string searchWord;
    subString(std::string s){
        searchWord = s;
    }
    ~subString(){
        searchWord.clear();
    }
};

class appNum:public v2Base,public v3Base
{
public:
    unsigned long numOfApperance;
    appNum(unsigned long n){
        numOfApperance = n;
    };
    ~appNum(){

    }
};

class MyMapImp:public MapReduceBase {
    void Map(const k1Base *const key, const v1Base *const val) const {
        fileName *f = (fileName *) key;
        subString *s = (subString *) val;

        if (f->fileNameStr.find(s->searchWord) != std::string::npos) { //todo to make sure its not colaps
            fileName *f2 = new fileName(f->fileNameStr);
            appNum *a = new appNum(1);
            Emit2(f2, a);
        }
    }

    void Reduce(const k2Base *const key, const V2_VEC &vals) const {
        fileName *f = (fileName *) key;
        appNum *a = new appNum(vals.size());
        fileName *f2 = new fileName(f->fileNameStr);
//        std::cout << a->numOfApperance << " num of apperance\n";
        Emit3(f2, a);
//        std::cout << "reduce\n";
//        std::cout << f->fileNameStr << "\n";
//        std::cout << a->numOfApperance << "\n";
    }
};

int main (int argc, char *argv[]) {
    ofstream myfile;
    subString* curSubString;
    fileName* curFile;
    IN_ITEMS_VEC itemsVec;
    MyMapImp myMapReduce;
    OUT_ITEMS_VEC outItem;
    if(argc > 1){
        std::string wordToSearch = argv[1];
    }
    else{
        printf("wrong number of parameters");
        return -1;
    }
    for (int i = 2; i < argc; ++i) {
        DIR *dir;
        struct dirent *ent;
        if ((dir = opendir (argv[i])) != NULL) {
            while ((ent = readdir (dir)) != NULL) {
                curFile = new fileName(ent->d_name);
                curSubString = new subString(argv[1]);
                itemsVec.push_back(std::pair<k1Base*,v1Base*>(curFile,curSubString));
            }
            closedir (dir);
        } else {
            printf("not directory "+(*argv[i]));
        }
    }
    outItem = RunMapReduceFramework(myMapReduce,itemsVec,5, true);
    for (auto it = outItem.begin();it != outItem.end(); ++it) { // going over all pthreads and their
        for (int i = 0; i < ((appNum*)((*it).second))->numOfApperance; ++i) {
            std::cout << ((fileName*)((*it).first))->fileNameStr << ' ';
        }
    }
    itemsVec.clear();
    for(auto it = outItem.begin();it != outItem.end();it++){
        delete (*it).first;
        delete (*it).second;
    }
    outItem.clear();

}

