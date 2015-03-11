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

class BigQ {
	int Create();
	int savelist(vector<Record*>);
	int GetNext(Record & );
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
