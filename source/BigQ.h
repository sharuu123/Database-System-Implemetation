#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include <vector>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;
typedef struct{
	Pipe *in;
	Pipe *out;
}BigQ_input;

typedef enum {source, destination} merger_type;
typedef enum {initial, merge_done, done, finish,  first_done , second_done , compare, read_first, read_second} merger_state;
// merger_state block_state = initial;


class BigQ {
	int Create();
	int savelist(vector<Record*>);
	int GetNext(Record & );
public:
	pthread_t thread;
	BigQ_input *t;
	OrderMaker *so;
	int rlen;
	ComparisonEngine G_comp;
	int G_curSizeInBytes;
	vector<Record *> overflow;
	vector<Record *> excess;
	merger_state block_state;
	static void *TPMMS_helper(void *context){
        return ((BigQ *)context)->TPMMS();
    }
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

	class block
	{	
	public:
		block(){
			count = 0;
			curpage = 0;
		}
		Record rec;
		Page p;
		int curpage;
		int count;
		int getRecord(){
			if(!p.GetFirst(&rec)){
				return 0;
			}
			return 1;
		}
	};

	class merger
	{	
	public:
		vector<Record *> ex;
		merger(){
			curpage = 0;
			totalpages = 1;
		}
		~merger(){
			delete first;
			delete second;
		}
		File f;
		Page p;
		int curpage;
		int totalpages;
		char * name;
		block *first;
		block *second;
		merger_type mtype;
		int loadpage(int flag){
			if(flag == 0){
				first->p.EmptyItOut();
				if(first->curpage < (totalpages-1) && first->count > 0){
					f.GetPage(&first->p,first->curpage);
					first->count--;
					return 1;
				}
				return 0;
			} else {
				second->p.EmptyItOut();
				if(second->curpage < (totalpages-1) && second->count > 0){
					f.GetPage(&second->p,second->curpage);
					second->count--;
					return 1;
				}
				return 0;
			}
			
		}
		int AddRec(Record &rec,int max_page){
			if(curpage >= max_page){
				// Overflow when merging blocks
				// Store to extra vector list and send them at the end
				Record * curr = new Record();
				curr->Consume(&rec);
				ex.push_back(curr);
			}
			else{
				if(!p.Append(&rec)){
					if(curpage >= max_page-1){
						Record * curr = new Record();
						curr->Consume(&rec);
						// Overflow when merging blocks
						// Store to extra vector list and send them at the end
						ex.push_back(curr);
					}
					else{
						f.AddPage(&p, curpage);
						p.EmptyItOut();
						curpage++;totalpages++;
						p.Append(&rec);
					}
				}
			}
		}
	};

	class Compare{
		public:
		OrderMaker *s;
		// Compare(OrderMaker *sort) {
		// 	s = sort;
		// }
		bool operator()(Record * R1, Record *R2)
		{
			ComparisonEngine comp;
		    if((comp).Compare(R1,R2,s) == -1){
		    	return true;
		    }
		    else{
		    	return false;
		    }
		}
	}mysorter;


	void* TPMMS ();
	int Create (char* s, merger *merge_file);
	int GetNext (Record &fetchme, merger *merge_file);
	int SendAll(merger *merge_file, BigQ_input *t);
	int SendSocket(merger *merge_file, BigQ_input *t);
	int check_file(merger *merge_file, int runlen);
	int savelist (vector<Record *> v, merger *merge_file);


};

#endif
