#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <sstream>
#include <iomanip>

struct sp_input{
	Pipe *in;
	Pipe *out;
	CNF *selop;
	Record *literal;
};

struct sf_input{
	DBFile *inFile;
	Pipe *out;
	CNF *selop;
	Record *literal;
};

struct p_input{
	Pipe *in;
	Pipe *out;
	int *keep;
	int num_in;
	int num_out;
};

struct s_input{
	Pipe *in;
	Pipe *out;
	Function *func;
};

struct d_input{
	Pipe *in;
	Pipe *out;
	Schema *sch;
};

struct j_input{
	Pipe *inL;
	Pipe *inR;
	Pipe *out;
	CNF *cnf;
	Record *literal;
};

struct w_input{
	Pipe *in;
	FILE *f;
	Schema *sch;
};

struct g_input{
	Pipe *in;
	Pipe *out;
	OrderMaker *ord;
	Function *func;
};

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 
	private:
	pthread_t thread;
	Record *buffer;
	int rlen;
	sf_input *sf_in;
	// void * Runit (void * arg);
	public:
	static void *SelectFile_helper(void *context){
        return ((SelectFile *)context)->sf_Runit();
    }
    void* sf_Runit ();
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
	private:
	pthread_t thread;
	int rlen;
	sp_input *sp_in;
	public:
	SelectPipe(){ rlen = 2;}
	static void *SelectPipe_helper(void *context){
        return ((SelectPipe *)context)->sp_Runit();
    }
    void* sp_Runit ();
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Project : public RelationalOp { 
	private:
	pthread_t thread;
	int rlen;
	p_input *p_in;
	public:
	Project(){ rlen = 2;}
	static void *Project_helper(void *context){
        return ((Project *)context)->p_Runit();
    }
    void* p_Runit ();	
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Join : public RelationalOp { 
	private:
	pthread_t thread;
	int rlen;
	j_input *j_in;
	public:
	Join(){ rlen = 2;}
	static void *Join_helper(void *context){
        return ((Join *)context)->j_Runit();
    }
    void* j_Runit ();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class DuplicateRemoval : public RelationalOp {
	private:
	pthread_t thread;
	int rlen;
	d_input *d_in;
	public:
	DuplicateRemoval(){ rlen = 2;}
	static void *Duplicate_helper(void *context){
        return ((DuplicateRemoval *)context)->d_Runit();
    }
    void* d_Runit ();
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Sum : public RelationalOp {
	private:
	pthread_t thread;
	int rlen;
	s_input *s_in;
	public:
	Sum(){ rlen = 2;}
	static void *Sum_helper(void *context){
        return ((Sum *)context)->s_Runit();
    }
    void* s_Runit ();
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class GroupBy : public RelationalOp {
	private:
	pthread_t thread;
	int rlen;
	g_input *g_in;
	public:
	GroupBy(){ rlen = 2;}
	static void *GroupBy_helper(void *context){
        return ((GroupBy *)context)->g_Runit();
    }
    void* g_Runit ();
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class WriteOut : public RelationalOp {
	private:
	pthread_t thread;
	int rlen;
	w_input *w_in;
	public:
	WriteOut(){ rlen = 2;}
	static void *WriteOut_helper(void *context){
        return ((WriteOut *)context)->w_Runit();
    }
    void* w_Runit ();
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
#endif
