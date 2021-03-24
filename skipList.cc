#include "skipList.h"

skipList::skipList()
{
    max_level = size = 0;

    //初始化头节点
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        skipListNode *tmp = new skipListNode(0, "header");
        header[i] = tmp;
    }

    //头节点相连
    for (int i = MAX_LEVEL - 1; i > 0; --i)
    {
        header[i]->down = header[i - 1];
    }
}

skipList::~skipList()
{
    //删除所有元素
    removeAll();
    //释放空间
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        delete header[i];
    }
}

std::string skipList::getValue(uint64_t k)
{
    //先找到这个点
    skipListNode *tmp = findNode(k);
    //若找到
    if (tmp)
    {
        return tmp->value;
    }
    //若找不到，返回“”
    else
    {
        return "";
    }
}

void skipList::insert(uint64_t k, std::string v)
{
    //得到插入点的高度
    int level = getLevel();

    //更新最高高度
    if (level > max_level)
    {
        max_level = level;
    }

    //如果已经存在，先删除
    //这边有两种处理1）直接改值 2）重新插入
    if (findNode(k))
    {
        remove(k);
    }

    skipListNode *height[MAX_LEVEL]; //辅助插入序列

    //从最高行开始插入
    for (int i = level; i >= 0; --i)
    {
        height[i] = insertIntoList(header[i], k, v);
    }

    //把每行插入的节点相连
    for (int i = level; i > 0 ; --i)
    {
        height[i]->down = height[i - 1];
    }

    //更新size
    ++size;
}

int skipList::getLevel()
{
    int k = 0;
    while (rand() % 2)
        ++k;
    return k;
}

skipListNode *skipList::findNode(uint64_t k)
{
    //从最高层的header开始找
    skipListNode *tmp = header[max_level];

    //当tmp存在时
    while (tmp)
    {
        //若找到且不为头节点，之间返回
        if (tmp->key == k && tmp->value != "header")
            return tmp;
        else
        {
            //先向右看，若右边有节点，且右边节点key比目标k小,向右走一格
            if (tmp->right && tmp->right->key <= k)
            {
                tmp = tmp->right;
            }
            //向右走不了，向下走
            else
            {
                tmp = tmp->down;
            }
        }
    }
    //走到底了，tmp为nullptr
    return tmp;
}

skipListNode *skipList::insertIntoList(skipListNode *header, uint64_t k, std::string v)
{

    skipListNode *prev = header;              //记录前面顶点
    skipListNode *n = new skipListNode(k, v); //被插入节点

    skipListNode *tmp = header->right;        //从header开始向后寻找插入点

    //若header后面无节点，直接把在header后面加
    if (tmp == nullptr)
    {
        prev->right = n;
    }
    //若header后面有节点，向后面找
    else
    {
        //向后寻找插入位置
        while (tmp && tmp->key < k)
        {
            prev = tmp;
            tmp = tmp->right;
        }
        prev->right = n;
        n->right = tmp;
    }
    //放回插入节点位置
    return n;
}

//PS：其实不需要有返回值
bool skipList::removeFromList(skipListNode *header, uint64_t k)
{
    skipListNode *prev = header;
    skipListNode *cur = header;
    while (cur)
    {
        //找到目标，要避免header
        if (cur->key == k && cur->value != "header")
        {
            prev->right = cur->right;
            delete cur;
            return true;
        }

        //向后寻找
        else if (cur->right && cur->right->key <= k)
        {
            prev = cur;
            cur = cur->right;
        }

        //找不到退出，返回false
        else
        {
            return false;
        }
    }
    return false;
}

bool skipList::remove(uint64_t k)
{
    //寻找k是否存在
    skipListNode *tmp = findNode(k);
    //k存在
    if (tmp)
    {
        //逐行删除
        for (int i = 0; i <= max_level; ++i)
        {
            removeFromList(header[i], k);
        }
        //更新size
        size--;
        //更新max_level
        for (int i = max_level; i > 0; i--)
        {
            if (header[i]->right == nullptr)
                max_level--;
            else
            {
                return true;
            }
        }
        return true;
    }
    else
    {
        return false;
    }
}

void skipList::removeAll()
{
    skipListNode *n = header[0]->right;
    while (n)
    {
        uint64_t key = n->key;
        n = n->right;
        remove(key);
    }
}

//其实可以储存在一个整形变量中
int skipList::getSIZE()
{
    int SIZE = 0;
    skipListNode *current = header[0]->right;
    while (current)
    {
        //计算key
        SIZE += sizeof(current->key);
        //计算value
        SIZE += current->value.length() * sizeof(char);
        //下一个
        current = current->right;
    }
    return SIZE;
}

void skipList::reset()
{
    removeAll();
}

int skipList::getsize()
{
    return size;
}