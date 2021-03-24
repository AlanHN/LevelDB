#include "index.h"

Index::Index()
{
}

Index::~Index()
{
    for (int i = 0; i < (int)index.size(); ++i)
    {
        delete index[i];
    }
}

SSTable *Index::getSSTableIndex(int l, int n)
{
    for (int i = 0; i < (int)index.size(); ++i)
    {
        if (index[i]->level == l && index[i]->num == n)
        {
            return index[i];
        }
    }
    return nullptr;
}

uint64_t Index::getMaxKey(int l, int n)
{
    SSTable *tmp = getSSTableIndex(l,n);
    return tmp->maxKey();
}

uint64_t Index::getMinKey(int l, int n)
{
    SSTable *tmp = getSSTableIndex(l,n);
    return tmp->minKey();
}

bool Index::delSSTableIndex(int l, int n)
{
    int i = 0;
    for (; i < (int)index.size(); ++i)
    {
        if (index[i]->level == l && index[i]->num == n)
        {
            delete index[i];
            index.erase(index.begin() + i);
            return true;
        }
    }
    return false;
}

void Index::addSSTableIndex(int l, int n, time_t t, vector<indexNode> *in)
{
    int i = 0;
    //若已有记录
    for (; i < (int)index.size(); ++i)
    {
        if (index[i]->level == l && index[i]->num == n)
        {
            //更新记录
            delSSTableIndex(l,n);
        }
    }  
    SSTable *tmp = new SSTable(l, n, t,in);
    index.push_back(tmp);
}



void Index::read(int l, int n,fstream *in)
{
    int offsetEnd = 0;//从后开始的offset
    
    //读取时间戳
    char timetmp[8] = {0};
    offsetEnd += sizeof(timetmp);
    in->seekg(-offsetEnd, ios::end);
    in->read(timetmp, sizeof(timetmp));
    time_t time = *((time_t *)timetmp);

    //检查是否已在内存中
    SSTable* lastTime = getSSTableIndex(l,n);
    if(lastTime && time == lastTime->timestamp)
    {
        return;
    }

    //读取数据量    
    char sizetmp[4] = {0};
    offsetEnd += sizeof(sizetmp);
    in->seekg(-offsetEnd,ios::end);
    in->read(sizetmp,sizeof(sizetmp)); 
    int size= *((int *)sizetmp);  

    //读取indexoffset
    char indexostmp[4] = {0};
    offsetEnd += sizeof(indexostmp);
    in->seekg(-offsetEnd,ios::end);
    in->read((char*)(&indexostmp),sizeof(indexostmp));    
    int indexos = *((int *)indexostmp); 

    //读取数据
    vector<indexNode> *data = new vector<indexNode>();
    uint64_t key = 0;
    int offset=0;
    in->seekg(indexos,ios::beg);
    for(int i = 0 ;i < size; i++ )
    {
        in->read((char*)&key,sizeof(key));
        in->read((char*)&offset,sizeof(offset));
        data->push_back(indexNode(key,offset));
    }
    addSSTableIndex(l, n, time, data);
}

//找到key的offset，找不到就返回-1
int Index::getOffset(int l, int n, uint64_t key)
{
    SSTable *find = getSSTableIndex(l,n);
    if(find == nullptr)
    {
        return -1;
    }
    else
    {
        uint64_t minKey = find->minKey();
        uint64_t maxKey = find ->maxKey();
        if(key<minKey||key>maxKey)
        {
            return -1;
        }
        else
        {
            for(int i = 0;i<find->size();i++)
            {
                if(key == (*find->index)[i].key)
                return (*find->index)[i].offset;
            }
        }
    }
    return -1;
}

int Index::searchKey(int &l, int &n, uint64_t key)
{
    int os;
    vector<int> ltmp;//记录层数
    vector<int> filetmp;
    for (int i = index.size()-1; i >=0 ; --i)
    {
        os = index[i]->getKeyOffset(key);
        //找到,先存起来
        if (os!=-1)
        {
            ltmp.push_back(index[i]->level);
            filetmp.push_back(i);
        }
    }
    //没有
    if(filetmp.empty())
    {
        return -1;
    }
    //找最高记录
    else
    {
        int lmin = ltmp[0] , count = filetmp[0];
        for(int i = 0;i<(int)filetmp.size();i++)
        {
            if(lmin>ltmp[i])
            {
                count = filetmp[i];
            }
        }
        l=index[count]->level;
        n=index[count]->num;
        os = index[count]->getKeyOffset(key);
        return os;
    }
    //没找到返回-1
    return -1;
}

void Index::reset()
{
    for (int i = 0; i < (int)index.size(); ++i)
    {
        delete index[i]->index;
        delete index[i];
    }
    index.clear();
}