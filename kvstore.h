#pragma once
#include "kvstore_api.h"
#include "skipList.h"
#include "index.h"
#include "buffer.h"
#include <vector>
#include <chrono>

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
	std::string root = ""; // 根目录

	void init(); //初始化

	//内存部分
	//memtable
	skipList *memtable = nullptr;

	//index
	Index *Dindex = nullptr;				//内存目录
	std::string findInDindex(uint64_t key); //在内存中找值
	void getIndex(int level, int num);		//把(level,num)中的文件读到内存

	//磁盘部分
	vector<int> levelFiles; //记录每层文件数

	//buffer，辅助输出
	Buffer *Dbuffer = nullptr;
	std::string findInSSTable(uint64_t key);						  //在磁盘中找值
	void writeToDisk();												  //写入磁盘
	std::string readValueFromFile(int offset, std::string &filePath); //从file中取值

	//合并函数
	void checkCompactor();	   //检查是否需要合并
	void compactor(int level); //合并level层的文件

	//删除函数
	void deleteAllFile();				 //删除所有文件
	void deleteFile(int level, int num); //删除（level，num）的文件
	void deleteFolder(int level);		 //散出level的文件夹

	//辅助函数
	void genFolder(int level);					 //生成文件夹
	std::string genFolderPath(int level);		 //返回level层的文件夹路径
	std::string genFilePath(int level, int num); //返回（level，num）的文件路径
	time_t genTime();
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;
};
