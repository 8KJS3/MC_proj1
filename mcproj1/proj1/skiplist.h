#include <iostream>
#include <sstream>
#include <cstdlib>
#include <ctime>
#include <pthread.h>
#include <queue>
#include <vector>

#define BILLION  1000000000L

using namespace std;

extern int thread_sz;

struct Work{
        long key;
        char action;
};

//Define the Worker Queue & Skiplist
vector<queue<Work>> WorkQueue;


template<class K,class V,int MAXLEVEL>
class skiplist_node
{
public:
    skiplist_node()
    {
	mark = false;
	valid = false;
        for (int i = 1; i <= MAXLEVEL; i++) {
            forwards[i] = nullptr;
        }
    }

    skiplist_node(K searchKey):key(searchKey)
    {
	mark = false;
	valid = false;
        for (int i = 1; i <= MAXLEVEL; i++) {
            forwards[i] = nullptr;
        }
    }

    skiplist_node(K searchKey,V val):key(searchKey),value(val)
    {
	mark = false;
	valid = false;
        for (int i = 1; i <= MAXLEVEL; i++) {
            forwards[i] = nullptr;
        }
    }

    virtual ~skiplist_node()
    {
    }

    K key;
    V value;
    skiplist_node<K,V,MAXLEVEL>* forwards[MAXLEVEL+1];

    bool mark;
    int toplevel;
    bool valid;
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

};

///////////////////////////////////////////////////////////////////////////////

template<class K, class V, int MAXLEVEL = 16>
class skiplist
{
public:
    typedef K KeyType;
    typedef V ValueType;
    typedef skiplist_node<K,V,MAXLEVEL> NodeType;

    skiplist(K minKey,K maxKey):m_pHeader(nullptr),m_pTail(nullptr),
                                max_curr_level(1),max_level(MAXLEVEL),
                                m_minKey(minKey),m_maxKey(maxKey)
    {
        m_pHeader = new NodeType(m_minKey);
        m_pTail   = new NodeType(m_maxKey);
	m_pHeader->valid = true;
	m_pTail->valid = true;
        for (int i = 1; i <= MAXLEVEL; i++) {
            m_pHeader->forwards[i] = m_pTail;
        }
    }

    virtual ~skiplist()
    {
        NodeType* currNode = m_pHeader->forwards[1];
        while (currNode != m_pTail) {
            NodeType* tempNode = currNode;
            currNode = currNode->forwards[1];
            delete tempNode;
        }
        delete m_pHeader;
        delete m_pTail;
    }

    void insert(K searchKey,V newValue)
    {
        skiplist_node<K,V,MAXLEVEL>* update[MAXLEVEL+1];
        NodeType* currNode = m_pHeader;

        // find predecessor 
        for (int level = max_curr_level; level >= 1; level--) {
            while (currNode->forwards[level]->key < searchKey && currNode->forwards[level]->valid) {
                currNode = currNode->forwards[level];
            }
            update[level] = currNode;
        }
        currNode = currNode->forwards[1];

        if (currNode->key == searchKey) {
	    //pthread_mutex_lock(&currNode->lock);

            currNode->value = newValue;
	    //pthread_mutex_unlock(&currNode->lock);
        } else {
            int newlevel = randomLevel();
            if (newlevel > max_curr_level) {
		pthread_mutex_lock(&lock);
                for (int level = max_curr_level+1; level <= newlevel; level++) {
                    update[level] = m_pHeader;
                }
                max_curr_level = newlevel;
		pthread_mutex_unlock(&lock);
            }
            
	    currNode = new NodeType(searchKey,newValue);
	    currNode->toplevel = newlevel;
	    pthread_mutex_lock(&currNode->lock);

            for (int lv = 1; lv <= newlevel; lv++) {
		if(lv == 1) {
		    pthread_mutex_lock(&update[lv]->lock);
	    	} else {
		    if( update[lv] != update[lv-1] ){
		        pthread_mutex_unlock(&update[lv-1]->lock);
			pthread_mutex_lock(&update[lv]->lock);
		    }
		}

		if( update[lv]->mark ){
		    //update[lv] is deleted
		    pthread_mutex_unlock(&update[lv]->lock);
		    NodeType* tempNode = m_pHeader;
		    for (int level = max_curr_level; level >= lv; level--){
			while (tempNode->forwards[level]->key < searchKey) {
			    tempNode = tempNode->forwards[level];
			}
			update[level] = tempNode;
		    }
		    pthread_mutex_lock(&update[lv]->lock);
		    lv--;
		    continue;
		} else if( update[lv]->forwards[lv]->key > searchKey ){
		    currNode->forwards[lv] = update[lv]->forwards[lv];
		    update[lv]->forwards[lv] = currNode;
		} else {
		    //Newnode was inserted between update[lv] and currNode
		    NodeType* nextNode = update[lv]->forwards[lv];
			
		    pthread_mutex_lock(&nextNode->lock);
		    pthread_mutex_unlock(&update[lv]->lock);
			
		    update[lv] = nextNode;
			

		    while( nextNode->forwards[lv] != m_pTail && nextNode->forwards[lv]->key < searchKey ){
			nextNode = update[lv]->forwards[lv];
			    
			pthread_mutex_lock(&nextNode->lock);
			pthread_mutex_unlock(&update[lv]->lock);

			update[lv] = nextNode;
		    }

		    //Re-Searching is done
		    currNode->forwards[lv] = update[lv]->forwards[lv];
		    update[lv]->forwards[lv] = currNode;
		
		}
	    }
	    currNode->valid = true;

	    pthread_mutex_unlock(&update[newlevel]->lock);
	    pthread_mutex_unlock(&currNode->lock);
	}
    }

    /*
    void erase(K searchKey)
    {
        skiplist_node<K,V,MAXLEVEL>* update[MAXLEVEL+1];
        NodeType* currNode = m_pHeader;

        // find predecessor
        for (int level = max_curr_level; level >= 1; level--) {
            while (currNode->forwards[level]->key < searchKey) {
                currNode = currNode->forwards[level];
            }
            update[level] = currNode;
        }

        currNode = currNode->forwards[1];

        if (currNode->key == searchKey) {
            for (int lv = 1; lv <= max_curr_level; lv++) {
                if (update[lv]->forwards[lv] == currNode) {
                    update[lv]->forwards[lv] = currNode->forwards[lv];
                }
            }
            delete currNode;

            while (max_curr_level > 1 && m_pHeader->forwards[max_curr_level] == m_pTail) {
                max_curr_level--;
            }
        }
    }
    */


    ///* 
    void erase(K searchKey)
    {
        skiplist_node<K,V,MAXLEVEL>* update[MAXLEVEL+1];
        NodeType* currNode = m_pHeader;

        // find predecessor 
        for (int level = max_curr_level; level >= 1; level--) {
            while (currNode->forwards[level]->valid && currNode->forwards[level]->key < searchKey ) {
                currNode = currNode->forwards[level];
            }
            update[level] = currNode;
        }

        currNode = currNode->forwards[1];
	int toplevel = currNode->toplevel;

        if (currNode->key == searchKey) {
	    currNode->mark = true;

	    //pthread_mutex_lock(&currNode->lock);
            for (int lv = 1; lv <= toplevel; lv++) {
		if(lv == 1) {
                    pthread_mutex_lock(&update[lv]->lock);
                } else {
                    if( update[lv] != update[lv-1] ){
                        pthread_mutex_unlock(&update[lv-1]->lock);
                        pthread_mutex_lock(&update[lv]->lock);
                    }
                }
		pthread_mutex_lock(&currNode->lock);
		if( update[lv]->mark ){
                    //update[lv] is deleted
                    pthread_mutex_unlock(&update[lv]->lock);
                    NodeType* tempNode = m_pHeader;
                    for (int level = max_curr_level; level >= lv; level--){
                        while (tempNode->forwards[level]->key < searchKey) {
                            tempNode = tempNode->forwards[level];
                        }
                        update[level] = tempNode;
                    }
		    pthread_mutex_lock(&update[lv]->lock);
                    lv--;
		    pthread_mutex_unlock(&currNode->lock);
                    continue;
                } else if( update[lv]->forwards[lv] == currNode ){
                    update[lv]->forwards[lv] = currNode->forwards[lv];
                } else{
                    //Newnode was inserted between update[lv] and currNode
                    NodeType* nextNode = update[lv]->forwards[lv];

                    pthread_mutex_lock(&nextNode->lock);
                    pthread_mutex_unlock(&update[lv]->lock);

                    update[lv] = nextNode;


                    while( nextNode->forwards[lv] != currNode ){
                        nextNode = update[lv]->forwards[lv];

                        pthread_mutex_lock(&nextNode->lock);
                        pthread_mutex_unlock(&update[lv]->lock);

                        update[lv] = nextNode;
                    }

                    //Re-Searching is done
                    update[lv]->forwards[lv] = currNode->forwards[lv];

                }
		pthread_mutex_unlock(&currNode->lock);
                
            }
	    pthread_mutex_unlock(&update[toplevel]->lock);
            //pthread_mutex_unlock(&currNode->lock);
	    
	    long h = currNode->key % thread_sz;
	    if( h < 0 ) h += thread_sz;
	    TrashQueue[h].push(currNode);
	    
	    pthread_mutex_lock(&lock);
	    pthread_mutex_lock(&m_pHeader->lock);
            while (max_curr_level > 1 && m_pHeader->forwards[max_curr_level] == m_pTail) {
		max_curr_level--;
            }
	    pthread_mutex_unlock(&m_pHeader->lock);
	    pthread_mutex_unlock(&lock);
        }
    }
    //

    bool find(K searchKey, V& outValue)
    {
        NodeType* currNode = m_pHeader;
        for (int level = max_curr_level; level >= 1; level--) {
            while (currNode->forwards[level]->key < searchKey) {
                currNode = currNode->forwards[level];
            }
        }
        currNode = currNode->forwards[1];
        if (currNode->key == searchKey) {
            outValue = currNode->value;
            return true;
        }
        return false;
    }

    bool empty() const
    {
        return (m_pHeader->forwards[1] == m_pTail);
    }

    std::string printList()
    {
        int i = 0;
        std::stringstream sstr;
        NodeType* currNode = m_pHeader->forwards[1];
        while (currNode != m_pTail) {
            sstr << currNode->key << " ";
            currNode = currNode->forwards[1];
            i++;
            if (i > 200) break;
        }
        return sstr.str();
    }

    void TrashSet()
    {
	TrashQueue.resize(thread_sz);	
    }

    void TrashEmpty()
    {
	int sz = TrashQueue.size();
	for(int i = 0; i < sz; i++)
	{
	    while(!TrashQueue[i].empty())
	    {
		delete TrashQueue[i].front();
		TrashQueue[i].pop();
	    }
	}
    }

    const int max_level;

protected:
    double uniformRandom()
    {
        return rand() / double(RAND_MAX);
    }

    int randomLevel() {
        int level = 1;
        double p = 0.5;
        while (uniformRandom() < p && level < MAXLEVEL) {
            level++;
        }
        return level;
    }

    K m_minKey;
    K m_maxKey;
    int max_curr_level;
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    skiplist_node<K,V,MAXLEVEL>* m_pHeader;
    skiplist_node<K,V,MAXLEVEL>* m_pTail;
    vector<queue<skiplist_node<K,V,MAXLEVEL>*>> TrashQueue;
};


