#include <cmath>
#include "BigQ.h"

OrderMaker G_sortorder;
ComparisonEngine G_comp;
static File f,fm;
static Page p,pm;
static int curpage=0;
static int totalpages=0;
static int pm_curpage=0;
static int pm_totalpages=0;
static int G_runlen=0;
static int curSizeInBytes=0;
static int Create();
static int savelist(vector<Record*>);
static int GetNext(Record & );

class Compare{
	public:
	bool operator()(Record * R1, Record *R2)
	{
	    if((G_comp).Compare(R1,R2,&(G_sortorder)) == -1){
	    	return true;
	    }
	    else{
	    	return false;
	    }
	}
}mysorter;

int Create () {
    try{
    	char tbl_path[100]; // construct path of the tpch flat text file
		sprintf (tbl_path, "bigq1.sbin"); 
	    f.Open(0,tbl_path);
	    curpage = 0;
	    totalpages = 1;
	    return 1;
    }
    catch(...){
    	return 0;
    }
}

int fm_Create () {
    try{
    	char tbl_path[100]; // construct path of the tpch flat text file
		sprintf (tbl_path, "bigq2.sbin"); 
	    fm.Open(0,tbl_path);
	    pm_curpage = 0;
	    pm_totalpages = 1;
	    return 1;
    }
    catch(...){
    	return 0;
    }
}

int GetNext (Record &fetchme) {
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

// int GetNext_Run(Record &r1) {
// 	cout << "Get a Record from Run1" << endl;
// 	if( !(p1.GetFirst(&r1)) ) {
// 		if ( c1<RunLen-1 && c1<f.GetLength()) {
// 			c1++;
// 			cout << "Get next page from Run1" << endl;
// 			p.EmptyItOut();
// 			f.GetPage(&p1 , Run1+c1);
// 		} else {
// 			break;
// 		}
// 	}
// }


int savelist (vector<Record *> v) {
	sort(v.begin(),v.end(),mysorter);
	int count = 1;
	for(int i = 0;i < v.size();i++){
		Record * rec = v[i];
		if(!p.Append(rec)){
			if((count)%(G_runlen+1) != 0){
				f.AddPage(&p, curpage);
				p.EmptyItOut();
				curpage++;totalpages++;
				count++;
				p.Append(rec);
			}
			else{
				cout << "Error" << endl;
				return 0;
			}
		}
	}
	
	return 1;
}

// int BigQ::Create () {
//     try{
//     	char tbl_path[100]; // construct path of the tpch flat text file
// 		sprintf (tbl_path, "bigq.sbin"); 
// 	    f.Open(0,tbl_path);
// 	    curpage = 0;
// 	    totalpages = 1;
// 	    return 1;
//     }
//     catch(...){
//     	return 0;
//     }
// }

// int BigQ::GetNext (Record &fetchme) {
// 	if(p.GetFirst(&fetchme)){
// 		return 1;
// 	}
// 	else{
// 		//cout << "pagechange" << curpage << " " << totalpages << endl;
// 		if(curpage < totalpages - 2){
// 			curpage++;
// 			f.GetPage(&p,curpage);
// 			if(GetNext(fetchme)){
// 				return 1;
// 			}
// 		}
// 		return 0;
// 	}
	
// }

// int Add (Record &rec, File &f, Page &p) {
// 	if(!p.Append(&rec)){
// 		// cout << "new page" << "\n";
// 		f.AddPage(&p, curpage);
// 		p.EmptyItOut();
// 		curpage++;totalpages++;
// 		p.Append(&rec);
// 	}
// 	// cout << "Add" << "\n";
// }

// int BigQ::savelist (vector<Record *> v) {
// 	sort(v.begin(),v.end(),mysorter);
// 	int count = 1;
// 	for(int i = 0;i < v.size();i++){
// 		Record * rec = v[i];
// 		if(!p.Append(rec)){
// 			if((count)%(G_runlen+1) != 0){
// 				f.AddPage(&p, curpage);
// 				p.EmptyItOut();
// 				curpage++;totalpages++;
// 				count++;
// 				p.Append(rec);
// 			}
// 			else{
// 				cout << "Error" << endl;
// 				return 0;
// 			}
// 		}
// 	}
	
// 	return 1;
// }

void *TPMMS (void *arg) {
	
	BigQ_input *t = (BigQ_input *) arg;

	cout << Create() << endl;
	cout << fm_Create() << endl;
	vector<Record *> v;
	int curSizeInBytes = 0;
	int Runs=0;
	// priority_queue <Record*, vector<Record*>, Compare> pq;
	while (true) {
		Record * curr = new Record();
		if(t->in->Remove(curr)){
			// out.Insert(curr);
			char *b = curr->GetBits();
			int rec_size = ((int *) b)[0];
			if (curSizeInBytes + rec_size < (PAGE_SIZE)*G_runlen) {
				curSizeInBytes += rec_size;
				// pq.push(curr);
				v.push_back(curr);
			}
			else{
				if(!savelist(v)){
					cout << "overflowww";
				}
				Runs++;
				v.clear();
				v.push_back(curr);
				curSizeInBytes = rec_size;				
			}
		}
		else{
			break;
		}
		
	}
	if(!v.empty()){
		Runs++;
	}
	if(!savelist(v)){
		cout << "overflowww";
	} 
	v.clear();
	f.AddPage(&p, curpage);
	totalpages++;curpage++;

	// Merging Begins here

	cout << "Runs=" << Runs << endl;
	int RunLen = G_runlen;
	cout << "Runlen=" << RunLen << endl;
	int Run1,Run2;
	Page p1,p2;
	Record r1,r2;

	// while ( Runs != 1){
		// if((f.GetLength()-1) <= RunLen){
		// 	Run1=0;
		// 	Run2=f.GetLength();
		// 	f.GetPage(&p1 , Run1);
		// 	cout << "Goto CASE2" << endl;
		// 	goto CASE2;
		// }
		Run1 = 0;
		Run2 = RunLen;
		cout << f.GetLength() << endl;
		cout << "Start Merging" << endl;
		if ( Run1<(f.GetLength()-1) && Run2>=(f.GetLength()-1)) {
			// only run1 present
			curpage = 0;
			totalpages = f.GetLength();

			f.GetPage(&p1 , curpage);
			// write a function to dump a file
			while(true){
				if(!p.GetFirst(&r1)){
					if(curpage < totalpages - 2){
						curpage++;
						f.GetPage(&p,curpage);
					}
					else{
						break;
					}
				}
				else{
					t->out->Insert(&r1);
				}				
			}	
		}
		else{
			// both runs present
			while ( Run1 < (f.GetLength()-1) && Run2 < (f.GetLength()-1) ) {
				// Still runs left to merge
				int c1 = 0;
				int c2 = 0;
				f.GetPage(&p1 , Run1+c1);
				f.GetPage(&p2 , Run2+c2);
				bool first_enter = true;
				cout << "Get 1st pages from two runs" << endl;
				while (1) {
					// Get a Record from Run1 if present
					RUN1:
					// cout << "Get a Record from Run1" << endl;
					if( !(p1.GetFirst(&r1)) ) {
						if ( c1 < RunLen-1 && Run1+c1 < f.GetLength()-2) {
							c1++;
							cout << "Get next page from Run1" << endl;
							p1.EmptyItOut();
							cout << "totalpages=" << totalpages << endl;
							cout << "Run1=" << Run1 << " " << "c1=" << c1 << endl;
							f.GetPage(&p1 , Run1+c1);
						} else {
							break;
						}
					}
					if(first_enter == true) {
						first_enter = false;
					} else {
						goto COMPARE;
					}
					// Get a Record from Run2 if present
					RUN2:
					// cout << "Get a Record from Run2" << endl;
					if( !(p2.GetFirst(&r2)) ) {
						if ( c2 < RunLen-1 && Run2+c2 < f.GetLength()-2) {
							c2++;
							cout << "Get next page from Run2" << endl;
							f.GetPage(&p2 , Run2+c2);
						} else {
							break;
						}
					}		
					// Comapre and add it to fm file
					COMPARE:
					// cout << "Comapre the two records" << endl;
					if((G_comp).Compare(&r1,&r2,&(G_sortorder)) == -1){
						// Add r1 and get another record from Run1
						  // cout << "Adding from Run1" << endl;
						if(!pm.Append(&r1)){
							fm.AddPage(&pm, pm_curpage);
							pm.EmptyItOut();
							pm_curpage++;pm_totalpages++;
							pm.Append(&r1);
						}
						goto RUN1;
					} else {
						 // Add r2 and get another record from Run2
						 // cout << "Adding from Run1" << endl;
						if(!pm.Append(&r2)){
							fm.AddPage(&pm, pm_curpage);
							pm.EmptyItOut();
							pm_curpage++;pm_totalpages++;
							pm.Append(&r2);
						}
						goto RUN2;					
					}
				}
				Record temp;
				if ( !(p1.GetFirst(&temp)) ) {
					// Run1 got exhausted
					// Add remaining elements of Run2 to fm
					cout << "Run1 got exhausted, Add remaining elements of Run2 to fm" << endl;
					while(true){
						if( !(p2.GetFirst(&r2)) ) {
							if ( c2<RunLen-1 && Run2+c2<f.GetLength()-2) {
								c2++;
								f.GetPage(&p2 , Run2+c2);
							} else {
								break;
							}
						}
						// cout << "Adding from Run2" << endl;
						if(!pm.Append(&r2)){
							fm.AddPage(&pm, pm_curpage);
							pm.EmptyItOut();
							pm_curpage++;pm_totalpages++;
							pm.Append(&r2);
						}					
					}				
				} else if ( !(p2.GetFirst(&temp)) ) {
					// Run2 got exhausted
					// Add remaining elements of Run1 to fm
					// TODO: Also add temp and previous r1
					cout << "Run2 got exhausted, Add remaining elements of Run1 to fm" << endl;
					while(true){
						if( !(p1.GetFirst(&r1)) ) {
							if ( c1<RunLen-1 && Run1+c1<f.GetLength()-2) {
								c1++;
								f.GetPage(&p1 , Run1+c1);
							} else {
								break;
							}
						}
						// cout << "Adding from Run1" << endl;
						if(!pm.Append(&r1)){
							fm.AddPage(&pm, pm_curpage);
							pm.EmptyItOut();
							pm_curpage++;pm_totalpages++;
							pm.Append(&r1);
						}					
					}				
				}
				// Move to next two runs in the file 
				Run1 = Run1 + 2*RunLen;
				Run2 = Run2 + 2*RunLen;
			}

			fm.AddPage(&pm, pm_curpage);

			RunLen = 2*RunLen;
			Runs=ceil(Runs/2);
		// }
		cout << "Merging is Done" << endl;
		cout << "fm.GetLength()=" << fm.GetLength() << endl;
		cout << "pm_totalpages=" << pm_totalpages << endl;

		pm_curpage = 0;
		pm.EmptyItOut();

		fm.GetPage(&pm,pm_curpage);
		Record temp;

		while(true){
				if(!pm.GetFirst(&temp)){
					if(pm_curpage < pm_totalpages - 2){
						pm_curpage++;
						f.GetPage(&pm,pm_curpage);

					}
					else{
						break;
					}
				}
				else{
					t->out->Insert(&temp);
				}				
		}	

	}



	// if(!pm.GetFirst(&temp)){
	// 	if(pm_curpage < pm_totalpages - 2){
	// 		pm_curpage++;
	// 		f.GetPage(&pm,pm_curpage);

	// 	}
	// 	return 0;
	// }

	// while(GetNext(temp) == 1){
	// 	t->out->Insert(&temp);
	// }

	t->out->ShutDown ();
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages

 //    construct priority queue over sorted runs and dump sorted data 
 // 	into the out pipe

 //    finally shut down the out pipe
	G_sortorder = sortorder;
	G_runlen = runlen;
	BigQ_input input = {&in, &out};

	pthread_t thread;
	pthread_create (&thread, NULL, TPMMS , (void *)&input);

	pthread_join (thread, NULL);

}


BigQ::~BigQ () {
}





// BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
// 	// read data from in pipe sort them into runlen pages

//     // construct priority queue over sorted runs and dump sorted data 
 // 	// into the out pipe

 //    // finally shut down the out pipe
	// sortorder.Print();
	// G_sortorder = sortorder;
	// Record curr;
	// Record temp;
	// priority_queue <Record, vector<Record>, Compare> pq;
	// while (in.Remove(&curr)) {
	// 	temp = curr;
	// 	//char *b = curr.GetBits();
	// 	//int rec_size = ((int *) b)[0];
	// 	// temp = curr;
	// 	//if (curSizeInBytes + rec_size < (PAGE_SIZE)*runlen) {
	// 	pq.push(temp);
	// 	//	curSizeInBytes += rec_size;
	// 	//}
	// 	//else{

// 		//}

// 	}
// 	// 	if(p.numRecs == 0){
// 	// 		p.Append(&curr);
// 	// 	}
// 	// 	else{
// 	// 		char *b = curr.GetBits();
// 	// 		int rec_size = ((int *) b)[0];
// 	// 		// first see if we can fit the record
// 	// 		if (p.curSizeInBytes + rec_size > PAGE_SIZE) {
// 	// 			// code to add new page
// 	// 			f.AddPage(&p,curpage);
// 	// 			p.EmptyItOut();
// 	// 			curpage++;
// 	// 		}
// 	// 		else{
// 	// 			Sorted_insert(curr,0,p.numRecs-1,sortorder,rec_size);
// 	// 		}
// 	// 	}
// 	// }
// 	// Record temp;
// 	// int i = 0;
// 	// while (i<3)
// 	//   {	 
	  	 
// 	//   	 temp = pq.top();
// 	//   	 //pq.pop();
// 	//      out.Insert(&temp);
// 	//      i++;
// 	//   }
// 	// p.myRecs->MoveToStart();
// 	// while(p.GetFirst(&temp) == 1){
// 	// 	out.Insert(&temp);
// 	// }
// 	out.ShutDown ();
// }

// BigQ::~BigQ () {
// }
