#include "buffer.h"
#include <fstream>
#include <chrono>
using namespace std;

Buffer::Buffer()
{
}

Buffer::~Buffer()
{
    //释放内存
    for (int i = 0; i < (int)files.size(); ++i)
    {
        delete files[i]->data;
        delete files[i];
    }
}

void Buffer::addDataList(vector<dataNode> *d, time_t t)
{
    dataList *tmp = new dataList(d, t);
    files.push_back(tmp);
}

void Buffer::read(fstream *in)
{

    int offsetEnd = 0; //从后开始的offset

    //读取时间戳
    time_t time = 0;
    offsetEnd += sizeof(time);
    in->seekg(-offsetEnd, ios::end);
    in->read((char *)(&time), sizeof(time));

    //读取数据量
    int size = 0;
    offsetEnd += sizeof(size);
    in->seekg(-offsetEnd, ios::end);
    in->read((char *)(&size), sizeof(size));

    //读取数据
    vector<dataNode> *data = new vector<dataNode>();
    uint64_t key = 0;
    char tmp[100000] = {0};
    std::string value = "";
    in->seekg(0, ios::beg);
    for (int i = 0; i < size; i++)
    {
        in->read((char *)&key, sizeof(key));
        in->get(tmp, 100000, '\0');
        value = tmp;
        in->seekg(1, ios::cur); //移动一个\0
        data->push_back(dataNode(key, value));
    }
    addDataList(data, time);
}

void Buffer::write(fstream *out, int byte)
{
    //先确认输出数据数
    int SIZE = sizeof(int) + sizeof(int) + sizeof(time_t); //数据量
    int count = 0;                                         //元素量
    int offset = 0;
    vector<offsetNode> outoffset; //目录

    for (int i = 0; i < (int)outdata.size() && SIZE < byte; i++)
    {
        count++;
        uint64_t key = outdata[i].key;
        std::string value = outdata[i].value;
        SIZE += sizeof(key);
        SIZE += value.length() + 1;
        SIZE += sizeof(uint64_t);
        SIZE += sizeof(uint64_t);
    }

    out->seekg(0, ios::beg);
    //输出数据
    for (int i = 0; i < count && !outdata.empty(); i++)
    {
        uint64_t key = outdata.begin()->key;
        std::string value = outdata.begin()->value;
        
        //记录offest
        outoffset.push_back(offsetNode(key, offset));

        //删除node
        vector<dataNode>::iterator k = outdata.begin();
        outdata.erase(k);

        //更新offset
        offset += sizeof(key);
        offset += value.length() + 1;

        //写入数据
        out->write((char *)(&key), sizeof(key));
        out->write((char *)value.c_str(), value.length() + 1);
    }

    //输出index
    for (int i = 0; i < count && !outoffset.empty(); i++)
    {
        uint64_t key = outoffset.begin()->key;
        int os = outoffset.begin()->offset;

        //删除node
        vector<offsetNode>::iterator k = outoffset.begin();
        outoffset.erase(k);

        //写入数据
        out->write((char *)(&key), sizeof(key));
        out->write((char *)(&os), sizeof(os));
    }

    //输出indexoffset
    out->write((char *)(&offset), sizeof(offset));

    //输出size
    out->write((char *)(&count), sizeof(count));

    //输出timestamp
    time_t timenow;
    timenow = genTime();
    out->write((char *)(&timenow), sizeof(timenow));
}

void Buffer::compactor()
{
    //检查是否有需要合并文件
    if (files.empty())
    {
        cout<<"no file to compactor"<<endl;
        return;
    }
    if(!outdata.empty())
    {
        cout<<"outdata is not empty"<<endl;
        return;
    }
    uint64_t min = ~0;//记录当前最小key
    int count = 0;//记录属于哪个文件list
    while (!files.empty())
    {
        min = ~0;//初始化最小key
        count = 0;//初始化文件位置
        //确认最小元素以及其位置
        for (int i = 0; i < (int)files.size(); i++)
        {
            //若key相同
            if (files[i]->data->front().key == min)
            {            
                //取出二者时间
                time_t time1 = files[count]->timestamp;
                time_t time2 = files[i]->timestamp;   
                
                //若原有min比较旧，更新
                if (time1 < time2)
                {
                    //删除原有node
                    vector<dataNode>::iterator k = files[count]->data->begin();
                    files[count]->data->erase(k);
                    //确认是否为空
                    if (files[count]->data->empty())
                    {
                        vector<dataList *>::iterator it = files.begin() + count;
                        files.erase(it);
                    }
                    count = i; //更新文件num
                }
                //若原有min比较新，删除新发现的
                else
                {
                    //删除新node
                    vector<dataNode>::iterator k = files[i]->data->begin();
                    files[i]->data->erase(k);
                    //确认是否为空
                    if (files[i]->data->empty())
                    {
                        vector<dataList *>::iterator it = files.begin() + i;
                        files.erase(it);
                    }
                }
            }
            //不同key
            else
            {
                uint64_t key = files[i]->data->front().key;
                //若找到比较小的key,更新count，min
                if (key < min)
                {
                    count = i;
                    min = key;
                }
            }
        }
        //找到了最小的node,加入outdata
        dataNode tmp = files[count]->data->front();
        outdata.push_back(tmp);
        //删除node
        vector<dataNode>::iterator k = files[count]->data->begin();
        files[count]->data->erase(k);
        //确认是否为空
        if (files[count]->data->empty())
        {
            vector<dataList *>::iterator it = files.begin() + count;
            files.erase(it);
        }
        //进入下一次循环
    }
}

void Buffer::reset()
{
    for (int i = 0; i < (int)files.size(); ++i)
    {
        delete files[i]->data;
        delete files[i];
    }
    files.clear();
    outdata.clear();
}

time_t Buffer::genTime()
{
	chrono::time_point<chrono::system_clock, chrono::milliseconds> tp = chrono::time_point_cast<chrono::milliseconds>(chrono::system_clock::now());
    auto tmp = chrono::duration_cast<chrono::milliseconds>(tp.time_since_epoch());
    return (time_t)tmp.count();
}

uint64_t Buffer::MaxKey()
{   
    uint64_t maxKey = 0;//最大的key
    uint64_t testKey = 0;//测试key
    //当files不为空的情况下进入
    if(!files.empty())
    {
        for(int i = 0;i < (int)files.size();i++)
        {
            testKey = files[i]->data->back().key;
            if(maxKey < testKey)
            {
                maxKey = testKey;
            }
        }
    } 
    return maxKey;
}

uint64_t Buffer::MinKey()
{   
    uint64_t minKey = ~0;//最大的key
    uint64_t testKey = 0;//测试key
    //当files不为空的情况下进入
    if(!files.empty())
    {
        for(int i = 0;i < (int)files.size();i++)
        {
            testKey = files[i]->data->front().key;
            if(minKey > testKey)
            {
                minKey = testKey;
            }
        }
    } 
    return minKey;
}