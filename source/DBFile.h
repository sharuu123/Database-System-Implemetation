#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Pipe.h"
#include <fstream>

typedef enum {heap, sorted, tree} fType;

typedef enum {DBFILE_R, DBFILE_W} mode;

// stub DBFile header..replace it with your own DBFile.h 
struct startinfo{
	OrderMaker *o; 
	int l;
};

class GenericDBFile {
protected:
	File f;
	Page p;
	int curpage;
	mode status;
	int totalpages;

public:
	GenericDBFile (){}
	virtual ~GenericDBFile(){}
	virtual int Create (char *fpath, fType file_type, void *startup){}
	virtual int Open (char *fpath){}
	virtual int Close (){}

	virtual void Load (Schema &myschema, char *loadpath){}

	virtual void MoveFirst (){}
	virtual void Add (Record &addme){}
	virtual int GetNext (Record &fetchme){}
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal){}
	virtual void Switchmode(mode){}

};

class DBFile {
private:
	GenericDBFile* myInternalVar;
public:
	DBFile(); 
	~DBFile();
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};


#endif
