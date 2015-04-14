#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>
#include "HeapFile.h"
#include "SortedFile.h"
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
}

DBFile::~DBFile () {
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {	

	char name[100];
	sprintf(name, "%s.metadata", f_path);
	ofstream myfile;
	// cout << "open metadata dile, f_type = " << f_type << endl;
	myfile.open(name);
	// myfile << f_type << endl;
	myfile.write ( (char *)(&f_type), sizeof(int) );
	cout << "name = " << name << endl;

	if(f_type == heap) {
		myInternalVar = new HeapFile;
		//myfile << "heap" << endl;
	} else {
		startinfo t = *(startinfo*)startup;
		// myfile << t.l << endl;
		myfile.write ( (char *)(&(t.l)), sizeof(int) );
		OrderMaker order = *(OrderMaker*)(t.o);
		// cout << "DBFile::Create() - " << endl;
		// order.Print();
		myfile.write ( (char *)(&order), sizeof(OrderMaker) );
		// myfile << order;
		myInternalVar = new SortedFile;
		// Store text version of order maker in metadata file.
		//myfile << "sorted" << endl;
		// myfile <<
	}
	myfile.flush();
	myfile.close();

	return myInternalVar->Create(f_path,f_type,startup);
}

int DBFile::Open (char *fpath){
	char name[100];
	sprintf(name, "%s.metadata", fpath);
	int type;

	ifstream myfile;
	myfile.open(name);
	// myfile >> type;
	myfile.read ( (char *)&type, sizeof(int) );
	// cout << "DBFile::Open - " << type << endl;
	if(type == 0){
		myInternalVar = new HeapFile;
	} else {
		myInternalVar = new SortedFile;
	}
	myInternalVar->Open(fpath);

}
int DBFile::Close (){
	int r = myInternalVar->Close();
	delete myInternalVar;
	return r;
}

void DBFile::Load (Schema &myschema, char *loadpath){
	myInternalVar->Load(myschema,loadpath);
}

void DBFile::MoveFirst (){
	myInternalVar->MoveFirst();
}

void DBFile::Add (Record &addme){
	myInternalVar->Add(addme);
}

int DBFile::GetNext (Record &fetchme){
	return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal){
	return myInternalVar->GetNext(fetchme,cnf,literal);
}