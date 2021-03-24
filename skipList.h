#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <iostream>
#include <cstdint>
#include <string>
#include <vector>
#include <ctime>

#define MAX_LEVEL 20 //跳表最高20层，可以存8M以上的数据

struct skipListNode
{
    uint64_t key;
    std::string value;
    skipListNode *right;
    skipListNode *down;
    skipListNode(uint64_t k, std::string v, skipListNode *r = nullptr, skipListNode *d = nullptr)
        : key(k), value(v), right(r), down(d) {}
};

class skipList
{
    friend class KVStore;

public:
    skipList();
    ~skipList();
    std::string getValue(uint64_t k);       //返回跳表中key=k的value，若不存在记录，返回“”。
    void insert(uint64_t k, std::string v); //插入键值对
    bool remove(uint64_t k);                //删除k记录，成功返回true，失败返回false
    int getsize();                          //返回跳表元素数目
    int getSIZE();                          //返回跳表数据量
    void reset();                           //重置跳表

private:
    skipListNode *header[MAX_LEVEL];                                             //头节点
    int max_level;                                                               //跳表中现有的最高层数
    int size;                                                                    //跳表中元素数目
    int getLevel();                                                              //返回以p=1/2的概率增长得到的节点高度
    skipListNode *findNode(uint64_t k);                                          //返回key=k的节点，找不到返回null
    skipListNode *insertIntoList(skipListNode *head, uint64_t k, std::string v); //在head行插入节点，返回插入节点位置
    bool removeFromList(skipListNode *head, uint64_t k);                         //在head行删除节点k，成功返回true，失败返回false
    void removeAll();                                                            //删除所有节点
};

#endif