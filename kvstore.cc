#include "kvstore.h"
#include <string>
#include <io.h>
#include <stdio.h>
#include <fstream>
#include <chrono>

using namespace std;

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
	//提取路径
	root = dir;
	root = root.substr(2);

	//初始化
	memtable = new skipList();
	Dindex = new Index();
	Dbuffer = new Buffer();
	//初始化levelnum

	//根目录
	std::string folderPath = genFolderPath(-1);

	//若不存在data目录就创建目录
	if (_access(folderPath.c_str(), 0) == -1)
	{
		genFolder(-1);
	}

	//初始化fileNum
	init();

    //调试用
	//reset();
}

void KVStore::init()
{
	//从level0.file0开始
	int level = 0;
	int num = 0;

	std::string folderPath = genFolderPath(level);
	std::string filePath = genFilePath(level, num);

	while (_access(folderPath.c_str(), 0) != -1)
	{
		levelFiles.push_back(0); //增加一个level
		while (_access(filePath.c_str(), 0) != -1)
		{
			levelFiles[level]++; //更新level
			fstream *in = new fstream();
			in->open(filePath.c_str() ,ios::binary|ios::in);
			Dindex->read(level,num,in);
			in->close();
			delete in;
			//下一个文件
			num++;
			filePath = folderPath + "\\SSTable" + to_string(num);
		}
		//读取当前目录结束，继续下一个
		level++;
		num = 0; //从第0个开始
		folderPath = genFolderPath(level);
		filePath = genFilePath(level, num);
	}
}

KVStore::~KVStore()
{
	delete memtable;
	delete Dindex;
	delete Dbuffer;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	//直接向memtable中插入
	memtable->insert(key, s);
	//数据大小
	int dataSIZE = memtable->getSIZE();
	//index大小
	int indexSIZE = memtable->getsize() * (sizeof(uint64_t) + sizeof(int));
	//剩余信息大小
	int infoSIZE = 2 * sizeof(int) + sizeof(time_t);
	int SIZE = dataSIZE + indexSIZE + infoSIZE;
	//若超出2M
	if (SIZE >= 2 * 1024 * 1024)
	{
		writeToDisk();	  //写入level0
		checkCompactor(); //检查合并
	}
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string tmp = "";

	//先到memtable中找
	tmp = memtable->getValue(key);

	//若memtable中删除了,直接返回not found
	if (tmp == "delete")
	{
		return "";
	}
	//若在memtable中找到了，直接返回memtable中的值
	else if (tmp != "")
	{
		return tmp;
	}
	//memtable中找不到的情况
	else
	{
		//先到index中找
		int level = 0, num = 0;
		int os = Dindex->searchKey(level, num, key);
		//找到了，直接去对应SSTable中找
		if (os != -1)
		{
			std::string filePath = genFilePath(level, num);
			tmp = readValueFromFile(os, filePath); //内存读值
			//如果是delete
			if (tmp == "delete")
			{
				tmp = "";
			}
			return tmp;
		}
		//没找到，去SSTable中遍历寻找
		else
		{
			tmp = findInSSTable(key);
			if (tmp == "delete")
			{
				tmp = "";
			}
			return tmp;
		}
	}
	return tmp;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	//先到memtable里面找
	std::string tmp = memtable->getValue(key);
	//如果是delete记录，返回false
	if (tmp == "delete")
	{
		return false;
	}
	else if (tmp != "")
	{
		return memtable->remove(key);
	}
	//若找不到，到index中找
	else
	{
		//到index中找
		int level, num;
		int os = Dindex->searchKey(level, num, key);
		//找到记录
		if (os != -1)
		{
			std::string folderPath = ".\\" + root + "\\level" + to_string(level);
			std::string filePath = folderPath + "\\SSTable" + to_string(num);
			//取出看看
			tmp = readValueFromFile(os, filePath);
			//已经delete
			if (tmp == "delete")
			{
				return false;
			}
			//有记录且还没delete
			else if (tmp != "")
			{
				memtable->insert(key, "delete");
				return true;
			}
			//没有记录
			else
			{
				return false;
			}
		}
		//index中找不到
		else
		{
			//到SSTable中找
			std::string tmp = findInSSTable(key);
			//找不到或者已经被删除
			if (tmp == "delete" || tmp == "")
			{
				return false;
			}
			//找到了
			else
			{
				memtable->insert(key, "delete");
				return true;
			}
		}
	}
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	//内存重置
	memtable->reset();
	Dindex->reset();
	Dbuffer->reset();

	//磁盘重置
	deleteAllFile();
	levelFiles.clear();
}

void KVStore::writeToDisk()
{
	std::string folderPath = genFolderPath(0);

	//若找不到folder0,生成目录
	if (_access(folderPath.c_str(), 0) == -1)
	{
		genFolder(0);
	}

	//更新目录
	int index = levelFiles[0];
	levelFiles[0]++;

	std::string filePath = genFilePath(0, index);
	fstream *out = new fstream(filePath.c_str(), ios::binary | ios::out);
	if (!out)
	{
		cout << "Can't open the file " << filePath << endl;
		cout << 242;
	}

	skipListNode *p = memtable->header[0]->right;
	int count = memtable->getsize();
	int offset = 0;
	vector<offsetNode> outoffset;

	out->seekp(0, ios::beg);
	//输出数据
	for (int i = 0; i < count; i++)
	{
		uint64_t key = p->key;
		std::string value = p->value;

		outoffset.push_back(offsetNode(key, offset)); //记录offest

		//后移p
		p = p->right;

		//更新offset
		offset += sizeof(key);
		offset += value.length() + 1;

		//写入数据
		out->write((char *)(&key), sizeof(key));
		out->write((char *)value.c_str(), value.length() + 1);
	}

	//输出index
	for (int i = 0; i < count; i++)
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

	//关闭文件流
	out->close();
	delete out;

	//更新内部index
	fstream *in = new fstream();
	in->open(filePath.c_str(), ios::binary | ios::in);
	Dindex->read(0, index, in);
	in->close();
	delete in;

	//重置memtable
	memtable->reset();
}

std::string KVStore::readValueFromFile(int offset, std::string &filePath)
{
	const char *path = filePath.c_str();
	fstream *in = new fstream();
	in->open(path, ios::in | ios::binary);
	if (!*in)
	{
		cout << "Can't open the file " << filePath << endl;
		cout << 320;
	}
	char tmp[100000] = {0};
	std::string ret = "";
	//从offset后面8个开始
	in->seekg(offset + sizeof(uint64_t), ios::beg);
	in->get(tmp, 100000, '\0');
	delete in;

	ret = tmp;
	return ret;
}

void KVStore::checkCompactor()
{
	//只需检查第0层
	if (levelFiles[0] > 2)
	{
		compactor(0);
	}
}

void KVStore::compactor(int levelNum)
{

	//最大文件数
	int limit = 1 << (levelNum + 1);
	// level0的时候要求从第一个开始读，特殊处理
	if (levelNum == 0)
	{
		limit = 0;
	}
    
	
	std::string folderPath = genFolderPath(levelNum);//当前目录路径
    std::string filePath = genFilePath(levelNum, limit);//要读取文件路径
	fstream *in = new fstream();//输入文件流
	fstream *out = new fstream();//输出文件流
	
	//先处理本层文件
	for (int i = limit; i < levelFiles[levelNum]; ++i)
	{	
		filePath = genFilePath(levelNum, i);//文件位置
		//打开文件
		in->open(filePath.c_str(), ios::binary | ios::in);
		if (!*in)
		{
			cout << "Can't open SSTable" << i << " in level" << levelNum << endl;
			cout << 368;
		}
		//读入到Dbuffer里面
		Dbuffer->read(in);
		in->close();

		Dindex->delSSTableIndex(levelNum,i);

		//删掉本层多余文件
		if (remove(filePath.c_str()) == -1)
		{
			cout << "Can't delete SSTable" << i << " in level" << levelNum << endl;
			cout << 569;
		}
		//更新路径		
	}
    //更新文件数
	levelFiles[levelNum] = limit;

	//进入下一层
	int nextLevel = levelNum + 1 ;
	folderPath = genFolderPath(nextLevel);

	//下层没有文件
	if (_access(folderPath.c_str(), 0) == -1)
	{
		//生成文件夹
		genFolder(nextLevel);

		//合并Dbuffer中的数据
		Dbuffer->compactor();

		int leftData = Dbuffer->outdata.size(); //Dbuffer中的数据量
		int filecounter = 0;					//输入文件量

		//当buffer中还有数据，就全部输入
		while (leftData > 0)
		{
			//文件路径
			filePath = folderPath + "\\SSTable" + to_string(filecounter);

            //输出2M数据
			out->open(filePath.c_str(), ios::binary | ios::out);
			Dbuffer->write(out, 2 * 1024 * 1024);
			out->close();

			//读入到Dindex里面
			in->open(filePath.c_str(), ios::binary | ios::in);
			Dindex->read(nextLevel, filecounter, in);
			in->close();

			//更新剩余数据量
			leftData = Dbuffer->outdata.size();

			//更新本层文件数
			levelFiles[nextLevel]++;
            
			//更新索引
			filecounter++;
		}
	}
	//下层有文件
	else
	{
		uint64_t min = Dbuffer->MinKey();//记录本层超出文件key最小值
	    uint64_t max = Dbuffer->MaxKey();;//记录本层超出文件key最大值
        vector<int> comFiles; //记录下层要合并的文件id
		//从第一个文件开始扫描
		int i = 0;
		filePath = folderPath + "\\SSTable" + to_string(i);
		while (_access(filePath.c_str(), 4) != -1)
		{
			//打开文件
			in->open(filePath.c_str(), ios::binary | ios::in);
			if (!*in)
			{
				cout << "Can't open SSTable" << i << " in level" << levelNum + 1 << endl;
				cout << 455;
			}
			//读入数据
			Dindex->read(nextLevel, i, in);
			uint64_t maxKey = Dindex->getMaxKey(nextLevel, i);
			uint64_t minKey = Dindex->getMinKey(nextLevel, i);
			//确定是否有交集
			if (!(maxKey < min || minKey > max))
			{
				//读入到Dbuffer
				Dbuffer->read(in);

				//确定文件更新目录
				comFiles.push_back(i);

				//删除原有目录
				Dindex->delSSTableIndex(levelNum + 1, i);

                //删掉原文件
				in->close();				
				if (remove(filePath.c_str()) == -1)
				{
					cout << "Can't delete SSTable" << i << " in level" << levelNum << endl;
					cout << 485;
				}
				levelFiles[levelNum + 1]--;
			}
			else
			{
				in->close();
			} 
			//更新文件目录	
			++i;
			filePath = folderPath + "\\SSTable" + to_string(i);
		}
		//合并Dbuffer中的数据
		Dbuffer->compactor();
		//确定剩余数据量
		int leftData = Dbuffer->outdata.size();
		//本层被更新文件
		int filecounter = 0;  //标志文件号
		bool comflag = false; //是否有被更新的文件
		//先更新到原有被合并文件中
		while (leftData > 0 && !comFiles.empty())
		{
			comflag = true; //有文件被更新
			//取出文件路径
			filecounter = comFiles[0];
			//打开文件
			filePath = folderPath + "\\SSTable" + to_string(filecounter);

			//写入2M数据
			out->open(filePath.c_str(), ios::binary | ios::out);
			Dbuffer->write(out, 2 * 1024 * 1024);
			out->close();


			//读入到Dindex里面，保险一点
			in->open(filePath.c_str(), ios::binary | ios::in);
			if (!*in)
			{
				cout << "Can't open SSTable" << i << " in level" << levelNum << endl;
				cout << 386;
			}
			Dindex->read(nextLevel, filecounter, in);
			in->close();
			
			//删除comFiles头节点
			vector<int>::iterator it = comFiles.begin();
			comFiles.erase(it);

            //更新数据量
			leftData = Dbuffer->outdata.size();

            //更新文件量
			levelFiles[nextLevel]++;
		}

		//分情况讨论	
		//若被更新过，那么filecounter到下一个
		if (comflag == true)
		{
			filecounter++;
		}
		//若没被更新
		else
		{
			//其实不需要，因为没有文件有交集，所以min和max不变
			min = Dbuffer->outdata.front().key; //buffer中最小key
			max = Dbuffer->outdata.back().key;  //buffer中最大key
			uint64_t minKey = Dindex->getMinKey(nextLevel,0);//层中最小key
			//1)buffer内的maxKey<min,从头开始写
            if(max<minKey)
			{
				filecounter = 0;
			}
			//2)buffer内的minKey>max，从尾开始写
			else
			{
				filecounter=levelFiles[nextLevel];
			}	
		}

		//全部输入
		while (leftData > 0)
		{
			filePath = folderPath + "\\SSTable" + to_string(filecounter); //目标文件路径

			//若已有文件
			if (_access(filePath.c_str(), 4) != -1)
			{
				//后面所有文件向后挪一个，从最大文件号开始
				for (int tmpcount = levelFiles[levelNum + 1]; tmpcount > filecounter; tmpcount--)
				{
					std::string file1 = folderPath + "\\SSTable" + to_string(tmpcount - 1); //原文件名
					std::string file2 = folderPath + "\\SSTable" + to_string(tmpcount);		//目标文件名
					rename(file1.c_str(), file2.c_str());
				}
			}

			//打开文件
			out->open(filePath.c_str(), ios::binary | ios::out);
			//写入2M数据
			Dbuffer->write(out, 2 * 1024 * 1024);
			out->close();

			//读入到Dindex里面，保险一点
			in->open(filePath.c_str(), ios::binary | ios::in);
			if (!*in)
			{
				cout << "Can't open SSTable" << i << " in level" << levelNum << endl;
				cout << 545;
			}
			Dindex->read(levelNum + 1, filecounter, in);
			in->close();

			//更新数据量
			leftData = Dbuffer->outdata.size();
			//取出下一个
			filecounter++;
			//更新下一层文件量
			levelFiles[nextLevel]++;
		}
	}

	int nextlimit = 1 << (nextLevel + 1); //本层最大文件数
	//确定是否有数据剩余，若有继续合并下一层
	if (levelFiles[nextLevel] > nextlimit)
	{
		compactor(nextLevel);
	}
}

std::string KVStore::findInDindex(uint64_t key)
{
	int os = 0;
	int i, j;
	os = Dindex->searchKey(i, j, key);
	if (os != -1)
	{
		std::string folderPath = ".\\" + root + "\\level" + to_string(i);
		std::string filePath = folderPath + "\\SSTable" + to_string(j);
		return readValueFromFile(os, filePath);
	}
	return "";
}

std::string KVStore::findInSSTable(uint64_t key)
{
	int os = 0;
	//遍历每层
	for (int i = 0; i < (int)levelFiles.size(); i++)
	{
		//遍历层里每个文件
		for (int j = 0; j < (int)levelFiles[i]; j++)
		{
			std::string folderPath = ".\\" + root + "\\level" + to_string(i);
			std::string filePath = folderPath + "\\SSTable" + to_string(j);
			fstream *in = new fstream();
			in->open(filePath.c_str(), ios::binary | ios::in);
			Dindex->read(i, j, in);
			in->close();
			delete in;
			os = Dindex->getOffset(i, j, key);
			if (os != -1)
			{
				return readValueFromFile(os, filePath);
			}
		}
	}
	return "";
}

void KVStore::getIndex(int level, int num)
{
	std::string folderPath = ".\\" + root + "\\level" + to_string(level);
	std::string filePath = folderPath + "\\SSTable" + to_string(num);
	fstream *in = new fstream();
	in->open(filePath.c_str(), ios::in | ios::binary);
	Dindex->read(level, num, in);
	in->close();
	delete in;
}

void KVStore::deleteFile(int level, int num)
{
	std::string folderPath = ".\\" + root + "\\level" + to_string(level);
	std::string filePath = folderPath + "\\SSTable" + to_string(num);
	remove(filePath.c_str());
}

void KVStore::deleteAllFile()
{
	int level = 0;
	int num = 0;
	//从(0,0)开始
	string folderPath = genFolderPath(level);
	string filePath = genFilePath(level, num);
	while (_access(folderPath.c_str(), 0) != -1)
	{
		while (_access(filePath.c_str(), 0) != -1)
		{
			if (remove(filePath.c_str()) == -1)
			{
				cout << "Can't delete SSTable" << num << " in level" << level << endl;
				cout << 661;
			}
			++num;
			filePath = genFilePath(level, num);
		}
		if (rmdir(folderPath.c_str()) == -1)
		{
			cout << "Can't delete level" << level << endl;
			cout << 669;
		}
		num = 0;
		++level;
		folderPath = genFolderPath(level);
		filePath = genFilePath(level, num);
	}
}

std::string KVStore::genFolderPath(int level)
{
	std::string folderPath = "";
	//若根目录
	if (level == -1)
	{
		folderPath = ".\\" + root;
	}
	//非根目录
	else
	{
		folderPath = ".\\" + root + "\\level" + to_string(level);
	}
	return folderPath;
}

std::string KVStore::genFilePath(int level, int num)
{
	std::string folderPath = genFolderPath(level);
	std::string filePath = folderPath + "\\SSTable" + to_string(num);
	return filePath;
}

void KVStore::genFolder(int level)
{
	string folderPath = genFolderPath(level);
	string cmd = "mkdir" + folderPath;
	system(cmd.c_str());
	//根目录
	if (level == -1)
	{
		return;
	}
	//其它
	else
	{
		levelFiles.push_back(0);
	}
}

void KVStore::deleteFolder(int level)
{
	string folderPath = genFolderPath(level);
	string cmd = "rmdir" + folderPath;
	system(cmd.c_str());
	//根目录
	if (level == -1)
	{
		return;
	}
	//其它
	else
	{
		vector<int>::iterator it = levelFiles.begin() + level;
		levelFiles.erase(it);
	}
}

time_t KVStore::genTime(){
	chrono::time_point<chrono::system_clock, chrono::milliseconds> tp = chrono::time_point_cast<chrono::milliseconds>(chrono::system_clock::now());
    auto tmp = chrono::duration_cast<chrono::milliseconds>(tp.time_since_epoch());
    return (time_t)tmp.count();
}