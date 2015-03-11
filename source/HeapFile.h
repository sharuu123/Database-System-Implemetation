#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "DBFile.h"

class HeapFile : public GenericDBFile {
private:

public:
	HeapFile(){}
	~HeapFile(){}
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void Switchmode(mode);

};

#endif