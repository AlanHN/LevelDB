#include <cstdint>
#include <string>
#include <fstream>
#include <vector>

using namespace std;

//目录节点
struct indexNode
{
    uint64_t key;
    int offset;
    indexNode(uint64_t k, int o)
        : key(k), offset(o) {}
};

//SSTable节点，记录层数，目录号，时间戳
struct SSTable
{
    int level = 0;        //记录层数
    int num = 0;          //记录文件编号
    time_t timestamp = 0; //记录时间戳

    vector<indexNode> *index = nullptr; //记录key和对应offset的表

    SSTable(int l, int n, time_t t, vector<indexNode> *i)
        : level(l), num(n), timestamp(t), index(i) {}
    //返回SSTable的节点数
    int size()
    {
        return index->size();
    }
    //返回最小key
    int minKey()
    {
        return index->front().key;
    }
    //返回最大key
    int maxKey()
    {
        return index->back().key;
    }
    //二分法找key,找不到就返回-1
    int getKeyOffset(uint64_t key)
    {
        int minId = 0;
        int maxId = index->size();
        while (minId <= maxId)
        {
            int midId = (minId + maxId) / 2;
            if (key == (*index)[midId].key)
            {
                return (*index)[midId].offset;
            }
            else if (key > (*index)[midId].key)
            {
                minId = midId + 1;
            }
            else if (key < (*index)[midId].key)
            {
                maxId = midId - 1;
            }
        }
        return -1;
    }
};

class Index
{
    friend class KVStore;

private:
    vector<SSTable *> index;
    SSTable *getSSTableIndex(int l, int n); //找到对应sstable
public:
    Index();
    ~Index();
    void read(int l, int n, fstream *in);                               //从in中读取sstable
    bool delSSTableIndex(int l, int n);                                 //删除Index中的sstable
    void addSSTableIndex(int l, int n, time_t t, vector<indexNode> *i); //增加Index中的sstable
    int getOffset(int l, int n, uint64_t key);                          //从l，n表中读key的offset，找不到返回-1
    int searchKey(int &l, int &n, uint64_t key);                        //从Index中搜索key，返回offset，l,n，找不到返回-1
    uint64_t getMaxKey(int l, int n);                                   //从l，n表中读最大key
    uint64_t getMinKey(int l, int n);                                   //从l，n表中读最小key
    void reset();                                                       //重置index
};