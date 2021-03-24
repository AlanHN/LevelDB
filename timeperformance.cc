#include "kvstore.h"
#include <iostream>
#include <fstream>
#include <windows.h>
#include <cstdint>
#include <string>

using namespace std;

const int num = 100;
const uint64_t Test1 = 1 * num;
const uint64_t Test2 = 2 * num;
const uint64_t Test3 = 3 * num;
const uint64_t Test4 = 4 * num;
const uint64_t Test5 = 5 * num;
const uint64_t Test6 = 6 * num;
const uint64_t Test7 = 7 * num;
const uint64_t Test8 = 8 * num;
const uint64_t Test9 = 9 * num;

KVStore KV("./data");

void time(uint64_t max, ofstream *File)
{
    uint64_t i;
    *File << max << "\t";
    double run_time=0,run_time1=0,run_time2=0,run_time3=0;
    vector<double> run1,run2,run3;
    _LARGE_INTEGER time_start; //开始时间
    _LARGE_INTEGER time_over;  //结束时间
    double dqFreq;             //计时器频率
    LARGE_INTEGER f;           //计时器频率
    QueryPerformanceFrequency(&f);
    dqFreq = (double)f.QuadPart;
    for(int j = 0;j<1;j++)
    {
    QueryPerformanceCounter(&time_start); //计时开始
    for (i = 0; i < max; ++i)
    {
        KV.put(i, std::string(i + 1, 's'));
    }
    QueryPerformanceCounter(&time_over);                                      //计时结束
    run_time = 1000000 * (time_over.QuadPart - time_start.QuadPart) / dqFreq; //乘以1000000把单位由秒化为微秒，精度为1000 000/（cpu主频）微秒
    run1.push_back(run_time);

    QueryPerformanceCounter(&time_start); //计时开始
    for (i = 0; i < max; ++i)
    {
        KV.get(i);
    }
    QueryPerformanceCounter(&time_over);                                      //计时结束
    run_time = 1000000 * (time_over.QuadPart - time_start.QuadPart) / dqFreq; //乘以1000000把单位由秒化为微秒，精度为1000 000/（cpu主频）微秒
    run2.push_back(run_time);

    QueryPerformanceCounter(&time_start); //计时开始
    for (i = 0; i < max; ++i)
    {
        KV.del(i);
    }
    QueryPerformanceCounter(&time_over);                                      //计时结束
    run_time = 1000000 * (time_over.QuadPart - time_start.QuadPart) / dqFreq; //乘以1000000把单位由秒化为微秒，精度为1000 000/（cpu主频）微秒
    run3.push_back(run_time);
    }

    for(int i = 0 ; i <run1.size(); i ++)
    {
        run_time1+=run1[i];
    }
    run_time1 = run_time1/run1.size();
    run_time1 = run_time1/max;

    for(int i = 0 ; i <run2.size(); i ++)
    {
        run_time2+=run2[i];
    }
    run_time2 = run_time2/run2.size();
    run_time2 = run_time2/max;

    for(int i = 0 ; i <run3.size(); i ++)
    {
        run_time3+=run3[i];
    }
    run_time3 = run_time3/run3.size();
    run_time3 = run_time3/max;

    *File <<run_time1<<"\t"<<run_time2<<"\t"<<run_time3<<"\n";
}

int main(int argc, char *argv[])
{
    std::cout << "Start test" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    ofstream *File = new ofstream();
    File->open("output.txt");

    // for(int i = 1 ;i <20 ; i++)
    // {
    //     int testnum = i*100;
    //     cout<<"test:"<<testnum<<endl;
    //     time(testnum,File);
    // }

    int testnum = 2028;
    cout<<"test:"<<testnum<<endl;
    time(testnum,File);

    File->close();
    //delete File;
    return 0;
}
