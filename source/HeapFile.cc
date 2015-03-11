
#include "HeapFile.h"


void HeapFile::Switchmode(mode change){
	if(status == change){
		return;
	}
	else{
		if(change == DBFILE_R){
			f.AddPage(&p, curpage);
			status = DBFILE_R;
			return;
		}
		else{
			p.EmptyItOut();
			// cout << f.GetLength()-2 << endl;
			curpage = f.GetLength()-2;
			f.GetPage(&p,curpage);
			// cout << p.numRecs << endl;
			status = DBFILE_W;
			return;
		}
	}
}	
int HeapFile::Create (char *f_path, fType f_type, void *startup) {
    try{
    	cout << " HeapFile::Create" << endl; 
	    f.Open(0,f_path);
	    f.AddPage (&p, 0);
	    curpage = 0;
	    status = DBFILE_W;
	    totalpages = 1;
	    return 1;
    }
    catch(...){
    	return 0;
    }
}

void HeapFile::Load (Schema &f_schema, char *loadpath) {
	cout << "HeapFile::Load" << endl;
    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    int counter = 0;
    while (temp.SuckNextRecord (&f_schema, tableFile ) == 1) {
		counter++;
		// if (counter % 10000 == 0) {
		// 	cerr << counter << "\n";
		// }
		Add(temp);
    }
    // cout << curpage << endl;
    // cout << p.numRecs << endl;
    // cout << "Number of records loaded: " << counter << endl;
    
}

int HeapFile::Open (char *f_path) {
	try{
		cout << "HeapFile::Open" << endl;
		p.EmptyItOut();
		f.Open(1,f_path);
		status = DBFILE_R;
		totalpages = f.GetLength();
		MoveFirst();
		return 1;
	}
	catch(...){
		return 0;
	}
	
}

void HeapFile::MoveFirst () {
	Switchmode(DBFILE_R);
	p.EmptyItOut();
	curpage = 0;
	f.GetPage(&p,curpage);
	
}

int HeapFile::Close () {
	// cout << f.GetLength() << "\n" ;
	try{
			Switchmode(DBFILE_R);
			p.EmptyItOut();
			f.Close();
			return 1;	
	}
	catch(...){
		return 0;
	}


}

void HeapFile::Add (Record &rec) {
	// Schema mySchema ("catalog", "region");p.Append(&rec)
	// rec.Print (&mySchema);
	Switchmode(DBFILE_W);
	if(!p.Append(&rec)){
		// cout << "new page" << "\n";
		f.AddPage(&p, curpage);
		p.EmptyItOut();
		curpage++;totalpages++;
		p.Append(&rec);
	}
	// cout << "Add" << "\n";
}

int HeapFile::GetNext (Record &fetchme) {
	//cout << curpage << endl;
	Switchmode(DBFILE_R);
	if(p.GetFirst(&fetchme)){
		return 1;
	}
	else{
		//cout << "pagechange" << curpage << " " << totalpages << endl;
		if(curpage < totalpages - 2){
			curpage++;
			f.GetPage(&p,curpage);
			if(GetNext(fetchme)){
				return 1;
			}
		}
		return 0;
	}
	
}

int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	Switchmode(DBFILE_R);
	// cout << f.GetLength() << endl;
	while(GetNext(fetchme)){
		// cout << "status" << endl;
		if (comp.Compare (&fetchme, &literal, &cnf)){
			return 1;
		}	
	}
	// cout << "end" << endl;
	return 0;	
}