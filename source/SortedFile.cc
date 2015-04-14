
#include "SortedFile.h"

static int fileLength;

void * producer1 (void *arg) {
	loadinfo *t = (loadinfo *) arg;
	if(t->flag == merg){
		Page p;
		// File f;
		Record temp;
		// t->f->Open(1,t->db_path);
	    int curpage = 0;
	    int totalpages = t->f->GetLength();
	    int counter = 0;

	    t->f->GetPage(&p,curpage);
	    while(true){
	    	if(!p.GetFirst(&temp)){
				if(curpage < totalpages - 2){
					curpage++;
					t->f->GetPage(&p,curpage);
					if(!p.GetFirst(&temp)){
						break;
					}
				} else {
					break;
				}
			}
			t->myPipe->Insert(&temp);
	    }

	    
	    t->myPipe->ShutDown ();

	} else {
		Record temp;
		int counter = 0;
		FILE *tableFile = fopen (t->loadpath, "r");
	    // cout << "laod records" << endl;
	    while (temp.SuckNextRecord(t->my_schema, tableFile ) == 1) {
			counter++;
			// cout << "Adding to toBigQ pipe" << endl;
			t->myPipe->Insert(&temp);
	    }
	    // cout << "Load finish" << endl;
		t->myPipe->ShutDown ();
		// cout << " producer: inserted " << counter << " recs into the pipe\n";
	}

}

void * consumer1 (void *arg) {
	loadinfo *t = (loadinfo *) arg;
	Page p;
	// File f;
	Record temp;
	// cout << "consumer = " << t->db_path << endl;
	// t->f->Open(1,t->db_path);
    int curpage = 0;
    int counter = 0;
    // Schema lineitem ("../source/catalog", "nation");
	while (t->myPipe->Remove(&temp)) {
		counter++;
		// temp.Print(&lineitem);
		// cout << "Remove record from BigQ pipe" << endl;
		if (!p.Append(&temp)) { 
			t->f->AddPage(&p, curpage);
			p.EmptyItOut();
			curpage++;
			p.Append(&temp);
		}
	}
	t->f->AddPage(&p, curpage);
	p.EmptyItOut();
	curpage++;
	// cout << "consumer - f.GetLength() = " << t->f->GetLength() << endl;
	// f.Close();
	t->myPipe->ShutDown();
	// cout << " Consumer: removed " << counter << " recs from the pipe\n";
	
}

void SortedFile::Merge(){
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	pthread_t thread1;
	Smode flag = merg;
	loadinfo lutils_producer = {&input, runLen, &so , NULL, db_path, NULL, flag, &f};
	pthread_create (&thread1, NULL, producer1, (void *)&lutils_producer);
	
	loadinfo lutils_consumer = {&output, runLen, &so , NULL, db_path, NULL, flag, &f};
	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	pthread_create (&thread2, NULL, consumer1, (void *)&lutils_consumer);
	// cout << "produce consumer threads started" << endl;
	BigQ bq (input, output, so, runLen);
	cout << "BigQ started" << endl;
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	// cout << "merge Done" << endl;

	curpage = 0;
	totalpages = f.GetLength();
	// cout << "merge - f.GetLength() = " << f.GetLength() << endl;
}

void SortedFile::Switchmode(mode change){
	if(status == change){
		return;
	}
	else{
		if(change == DBFILE_R){
			// Merge the newly added elements to original sorted dbfile.
			f.AddPage(&p, curpage);
			p.EmptyItOut();
			curpage++;totalpages++;
			Merge();
			//f.AddPage(&p, curpage);
			status = DBFILE_R;
			return;
		}
		else{
			p.EmptyItOut();
			 cout << f.GetLength()-2 << endl;
			curpage = f.GetLength()-2;
			f.GetPage(&p,curpage);
			 // cout << "done" << endl;
			status = DBFILE_W;
			return;
		}
	}
}	

int SortedFile::Create (char *f_path, fType f_type, void *startup) {
    try{
    	db_path = f_path;
    	// cout << "create = " << f_path << endl;
	    f.Open(0,f_path);
	    f.AddPage (&p, 0);
	    curpage = 0;
	    status = DBFILE_R;
	    totalpages = 1;
	    return 1;
    }
    catch(...){
    	return 0;
    }
}



void SortedFile::Load (Schema &f_schema, char *path) {

	status = DBFILE_W;
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);
	*my_schema = f_schema;
	// thread to dump data into the input pipe (for BigQ's consumption)
	Smode flag = init;
	pthread_t thread1;
	loadinfo lutils_producer = {&output, runLen, &so , path, db_path, &f_schema, flag, &f};
	pthread_create (&thread1, NULL, producer1, (void *)&lutils_producer);

	loadinfo lutils_consumer = {&output, runLen, &so , path, db_path, &f_schema, flag, &f};
	// thread to read sorted data from output pipe (dumped by BigQ)
	pthread_t thread2;
	pthread_create (&thread2, NULL, consumer1, (void *)&lutils_consumer);
	// cout << "produce consumer threads started" << endl;
	BigQ bq (input, output, so, runLen);
	cout << "BigQ started" << endl;
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	// cout << "load Done" << endl;
	// cout << "Load - f.GetLength() = " << f.GetLength() << endl;

	// TODO update currpage & totalpages
	curpage = 0;
	totalpages = f.GetLength();
	status = DBFILE_R;
}

int SortedFile::Open (char *f_path) {
	try{
		// cout << "SortedFile::Open()" << endl;
		char name[100];
		sprintf(name, "%s.metadata", f_path);

		ifstream myfile;
		myfile.open(name);
		int t;
		myfile.read ( (char *)&t, sizeof(int) );
		myfile.read ( (char *)&runLen, sizeof(int) );
		myfile.read ( (char *)&so, sizeof(OrderMaker) );
		// myfile >> t;
		//fType dbfileType = (fType)t;
		// myfile >> runLen;
		// while( myfile.read ( (char *)&so, sizeof(OrderMaker) ) )
		//myfile >> so; 
		myfile.close();

		//p.EmptyItOut();
		f.Open(1,f_path);
		status = DBFILE_R;
		totalpages = f.GetLength();
		// cout << "open - f.GetLength() = " << f.GetLength() << endl;
		MoveFirst();
		return 1;
	}
	catch(...){
		return 0;
	}
	
}

void SortedFile::MoveFirst () {
	// cout << "SortedFile::MoveFirst ()" << endl;
	Switchmode(DBFILE_R);
	p.EmptyItOut();
	curpage = 0;
	f.GetPage(&p,curpage);
}

int SortedFile::Close () {
	// cout << f.GetLength() << "\n" ;
	// cout << "SortedFile::Close()" << endl;
	try{
			Switchmode(DBFILE_R);
			p.EmptyItOut();
			f.Close();
			// cout << "close - f.GetLength() = " << f.GetLength() << endl;
			return 1;	
	}
	catch(...){
		return 0;
	}
}

void SortedFile::Add (Record &rec) {
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

int SortedFile::GetNext (Record &fetchme) {
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

extern struct AndList *final;


int SortedFile::CheckCNF(CNF &cnf, OrderMaker *& query,OrderMaker &sort,OrderMaker &sortorder, int k, int & flag, int i, int j){
	for (int l = 0; l < cnf.numAnds; l++) {
		for (int m = 0; m < cnf.orLens[l]; m++) {
			if(cnf.orList[l][m].op == Equals){
				if(cnf.orList[l][m].operand1 == Literal){
					if(cnf.orList[l][m].whichAtt2 == sort.whichAtts[i]){
						flag = 1;
						// cout << "Present " << sortorder.whichAtts[i] << endl;
						query->whichAtts[k] = sortorder.whichAtts[i];
						query->whichTypes[k] = sortorder.whichTypes[i];
						query->numAtts++;
						k++;
						break;
					}
				}
				else{
					if(cnf.orList[l][m].whichAtt1 == sort.whichAtts[i]){
						flag = 1;
						// cout << "Present " << sortorder.whichAtts[i] << endl;
						query->whichAtts[k] = sortorder.whichAtts[i];
						query->whichTypes[k] = sortorder.whichTypes[i];
						query->numAtts++;
						k++;
						break;
					}
				}
			}
		}
	}
	// cout << "in helper" << endl;
	// query->Print();
	return k;
}



int SortedFile::CompareOrders(OrderMaker &sort, CNF &cnf, OrderMaker *& query){
	Record lit;
	OrderMaker dummy;
	OrderMaker sortorder;
	cnf.GetSortOrders (sortorder, dummy);
	query = new OrderMaker();
	// sortorder.Print();
	int k = 0; query->numAtts = 0;
	int flag = 0;
	for (int i = 0; i < sort.numAtts; i++){
		for(int j = 0; j < sortorder.numAtts; j++){
			if(sortorder.whichAtts[i] == sort.whichAtts[i]){
				k = CheckCNF(cnf,query,sort,sortorder,k,flag,i,j);
			}
			if(flag != 1){
				break;
			}
		}
	}
	if(flag == 0){
		return 0;
	}
	// cout << "Printing query" << endl;
	// query->Print();
	return 1;
}

int SortedFile::check_file(OrderMaker * query){
	int entries = 0;
	curpage = 0;
	Record recs[2];
	p.EmptyItOut();
	f.GetPage(&p,curpage);
	Record *lasts = NULL, *prevs = NULL;
	int last_page = 0; int prev_page = 0;
	int errs = 0;
	int j = 0;
	ComparisonEngine comp;
	while (GetNext(recs[j%2])) {
		entries++;
		prevs = lasts;
		prev_page = last_page;
		lasts = &recs[j%2];
		// Schema lineitem ("../source/catalog", "lineitem");
		if(j == 0){
			// lasts->Print(&lineitem);
		}
		last_page = curpage;
		if (prevs && lasts) {
			if (comp.Compare (prevs, lasts, &so) == 1) {
				errs++;
				// Previous page and last page should be different 
				// and last_page should be multiple of runlength
				cout << j << " " << prev_page << " " << last_page << endl;
				// prevs->Print (&mySchema);
				// lasts->Print (&mySchema);
				// if(runlen > f.GetLength()){
				// 	exit(-1);
				// }
				
			}
		}
		j++;
	}
	curpage = 0;
	p.EmptyItOut();	
	// cout << "Checked " << j << ", Errors: " << errs << ", totalpages: " << f.GetLength() << endl;
}

int SortedFile::Searchbinary (Record &fetchme, CNF &cnf, Record &literal, OrderMaker *& query) {
	ComparisonEngine comp;

	// cout << "Binary searching" << endl;
	int k = 0;
	// query->Print();
	if(GetNext(fetchme)){
		k = comp.CompareSort(&fetchme, &literal, query, &cnf);
		if(k == 1){
			// check_file(query);
			// cout << curpage << " " << f.GetLength() << endl;
			// cout << "getting 1" << endl;
			return -1;
		}
		else if(k == 0){
			return 1;
		}
		else{
			// cout << "Doing binary search" << endl;
		    int m;
		    int l = curpage;
		    int r = f.GetLength()-2;
		    int start = curpage;
		    while(l <= r )
		    {	
		        m = l + (r-l)/2;
		        // cout << "checking " << m  << "start " << l << "End " << r << endl;
		        p.EmptyItOut();
				f.GetPage(&p,m);
				if(GetNext(fetchme)){
					k = comp.CompareSort(&fetchme, &literal, query, &cnf);
					if( k == 0 ) {
						r = m;
						// break;
					}
				 	else if(k == -1){
				 		l = m + 1;
				 	}
				 	else{
				 		r = m - 1;
				 	}
				}
				else{
					cout << "error" << endl;
				}
		    }
		    start = min(l,r);
		    curpage = start;
		    p.EmptyItOut();
			f.GetPage(&p,curpage);
		}
	}
	return -1;
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	// cnf.Print();
	// cout << "prnting cnf and  ordermaker" << endl;
	// so.Print();
	Switchmode(DBFILE_R);
	if(query == NULL){
		if(CompareOrders(so,cnf,query)){
			// cout << "printing in getnext" << endl;
			// query->Print();
			int result = Searchbinary(fetchme, cnf, literal, query);
			if( result == 1){
				return 1;
			}
			else if(result == 0){
				return 0;
			}
		}
		else{
			delete query;
			query = NULL;
		}
	}
	// cout <<  curpage << f.GetLength() << endl;
	while(GetNext(fetchme)){
		// cout << "status" << endl;
		if (comp.Compare (&fetchme, &literal, &cnf)){
			return 1;
		}	
	}
	return 0;	
}