#include "kvstore.h"
#include <iostream>
#include <fstream>
#include <windows.h>
using namespace std;

int main(){
    KVStore s("./data");
    fstream out;
    out.open(".\\file1.txt",ios::out);
    int num = 0;
    double time = 0;
    double interval = 0;

    LARGE_INTEGER cpuFreq;
    LARGE_INTEGER startTime;
    LARGE_INTEGER endTime;
    LARGE_INTEGER midTime;
    double rumTime=0.0;
    QueryPerformanceFrequency(&cpuFreq);
	//time start
    QueryPerformanceCounter(&startTime);
    out << "time used per ms" << '\n';
    for (int j = 0; j < 6000;++j){
        QueryPerformanceCounter(&midTime);
        for (uint64_t i = 0; i < 10; ++i)
        {
            s.put(num,string(1000,'s'));
            ++num;
        }
        //time end
	    QueryPerformanceCounter(&endTime);
        interval=(((endTime.QuadPart - midTime.QuadPart) * 1000.0f) / cpuFreq.QuadPart);
        time = (((endTime.QuadPart - startTime.QuadPart) * 1000.0f) / cpuFreq.QuadPart);
        out << time << '\t' << (double)10 / interval << '\n';
    }
    out.close();
}