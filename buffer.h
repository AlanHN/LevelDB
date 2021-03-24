#include <iostream>
#include <vector>
#include <time.h>

using namespace std;

//键值对
struct dataNode
{
    uint64_t key;
    std::string value;
    dataNode(uint64_t k, std::string v)
        : key(k), value(v) {}
};

//目录对
struct offsetNode
{
    uint64_t key;
    int offset;
    offsetNode(uint64_t k, int o)
        : key(k), offset(o) {}
};

//数据表
struct dataList
{
    vector<dataNode> *data; //存数据
    time_t timestamp;       //时间戳
    dataList(time_t t)
        : timestamp(t)
    {
        data = new vector<dataNode>();
    }
    dataList(vector<dataNode> *d, time_t t)
        : data(d), timestamp(t) {}
};

class Buffer
{
    friend class KVStore;

private:
    vector<dataList *> files; //存输入文件的数据
    vector<dataNode> outdata; //存输出文件的数据
public:
    Buffer();
    ~Buffer();
    void addDataList(vector<dataNode> *d, time_t t); //向files中增加data
    void read(fstream *in);                          //从in中读取数据
    void write(fstream *out, int byte);              //向out中写入byte位数据
    void compactor();                                //合并files中的数据到outdata
    void reset();                                    //重置buffer
    time_t genTime();                                //生成时间戳
    uint64_t MaxKey();//得到outdata中最大的key
    uint64_t MinKey();//得到outdata中最小的key
};