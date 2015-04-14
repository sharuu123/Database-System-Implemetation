#include "BigQ.h"

// OrderMaker G_sortorder;
// ComparisonEngine G_comp;
// static int G_runlen=0;
// static int G_curSizeInBytes = 0;
// static BigQ_input G_input;

// Schema mySchema ("../source/catalog", "orders");

// Initial creation of temp files
int BigQ::Create (char* s, merger *merge_file) {
    try{
    	merge_file->name = s;
	    merge_file->f.Open(0,s);
	    merge_file->p.EmptyItOut();
	    merge_file->curpage = 0;
	    merge_file->totalpages = 1;
	    return 1;
    }
    catch(...){
    	return 0;
    }
}

int BigQ::GetNext (Record &fetchme, merger *merge_file) {
	if(merge_file->p.GetFirst(&fetchme)){
		return 1;
	}
	else{
		if(merge_file->curpage < merge_file->f.GetLength() - 2){
			merge_file->curpage++;
			merge_file->f.GetPage(&merge_file->p,merge_file->curpage);
			if(GetNext(fetchme,merge_file)){
				return 1;
			}
		}
		return 0;
	}
	
}

// helper function to send the file
int BigQ::SendAll(merger *merge_file, BigQ_input *t){
	merge_file->curpage = 0;
	merge_file->p.EmptyItOut();
	merge_file->f.GetPage(&(merge_file->p),merge_file->curpage);
	Record temp;
	while(GetNext(temp,merge_file) == 1){
		t->out->Insert(&temp);
	}
}

// finally sending the sorted
int BigQ::SendSocket(merger *merge_file, BigQ_input *t){
	// cout << merge_file->name << endl;
	merge_file->curpage = 0;
	merge_file->p.EmptyItOut();
	// cout << "test ?" << endl;
	merge_file->f.GetPage(&(merge_file->p),merge_file->curpage);
	// cout << "works ?" << endl;
	Record temp;
	int val = 0;
	mysorter.s=so;
	// merge_file->ex=excess;
	sort(excess.begin(),excess.end(),mysorter);
	while(GetNext(temp,merge_file) == 1){
		while(val< excess.size()){
			if(G_comp.Compare (&temp, excess[val], so) != 1){
				break;
			}
			else{
				//Sending overflowed record
				t->out->Insert(excess[val]);
				val++;
			}
		}
		// Sending record
		t->out->Insert(&temp);
	}
	while(val<excess.size()){
		// Send the overflowed pending records
		t->out->Insert(excess[val]);
		val++;
	}
	// cout << "sendng" << endl;
}

// This is a helper function to check  the unsorted records after each merge
int BigQ::check_file(merger *merge_file, int runlen){
	int entries = 0;
	merge_file->curpage = 0;
	Record recs[2];
	merge_file->p.EmptyItOut();
	merge_file->f.GetPage(&(merge_file->p),merge_file->curpage);
	Record *lasts = NULL, *prevs = NULL;
	int last_page = 0; int prev_page = 0;
	int errs = 0;
	int j = 0;
	while (GetNext((recs[j%2]),merge_file)) {
		entries++;
		prevs = lasts;
		prev_page = last_page;
		lasts = &recs[j%2];
		last_page = merge_file->curpage;
		if (prevs && lasts) {
			if (G_comp.Compare (prevs, lasts, so) == 1) {
				errs++;
				// Previous page and last page should be different 
				// and last_page should be multiple of runlength
				// cout << j << " " << prev_page << " " << last_page << endl;
				// prevs->Print (&mySchema);
				// lasts->Print (&mySchema);
				if(runlen > merge_file->f.GetLength()){
					exit(-1);
				}
				
			}
		}
		j++;
	}
	merge_file->curpage = 0;
	merge_file->p.EmptyItOut();	
	// cout << "Checked " << j << ", Errors: " << errs << ", totalpages: " << merge_file->f.GetLength() << endl;
}


int vals = 0;
int BigQ::savelist (vector<Record *> v, merger *merge_file) {
	// cout << " Writing from: " << merge_file->curpage << endl;
	vals = vals + v.size();
	merge_file->p.EmptyItOut();
	mysorter.s=so;
	sort(v.begin(),v.end(),mysorter);
	int count = rlen;
	int added_file = 0;
	int added_overflow = 0;
	bool saved = false;
	for(int i = 0;i < v.size();i++){
		Record * rec = v[i];
		if(count >  0){
			added_file++;
			if(!merge_file->p.Append(rec)){
				merge_file->f.AddPage(&(merge_file->p), merge_file->curpage);
				merge_file->p.EmptyItOut();
				saved = true;
				merge_file->curpage++;merge_file->totalpages++;
				// cout << " Adding " << count << " " << merge_file->curpage-1 << endl;
				count--;
				if(count >  0){
					merge_file->p.Append(rec);
					saved = false;
				}
				else{
					added_overflow++;
					added_file--;
					overflow.push_back(v[i]);
					saved = true;
					// pushing overflow records
				}
			}
		}
		else{
			added_overflow++;
			// v.erase(v.begin(), v.begin() + i);//
			overflow.push_back(v[i]);
		}
	}
	if(v.size() - added_overflow - added_file != 0){
		cout << "Check Save vector " << v.size() - added_overflow - added_file << endl;
	}
	v.clear();
	if(!saved){
		merge_file->f.AddPage(&(merge_file->p), merge_file->curpage);
		merge_file->curpage++;merge_file->totalpages++;
		merge_file->p.EmptyItOut();
	}
	if(count <= 0){
		return 0;
	}
	else{
		return 1;
	}
}

// void *TPMMS (void *arg) {
void* BigQ::TPMMS () {
	
	// BigQ_input *t = (BigQ_input *) arg;
	//creating two files
	merger *first_file = new merger();
	merger *second_file = new merger();
	char first_path[100]; 
	char second_path[100];
	sprintf (first_path, "Bigq.bin"); 
	Create(first_path,first_file);
	sprintf (second_path, "Bigq_temp.bin"); 
	Create(second_path,second_file);
	// Created two files for the two way merger sort

		// compare function for the vector sort.
	// class Compare{
	// 	public:
	// 	OrderMaker *s;
	// 	Compare(OrderMaker *sort) {
	// 		s = sort;
	// 	}
	// 	bool operator()(Record * R1, Record *R2)
	// 	{
	// 		ComparisonEngine comp;
	// 	    if((comp).Compare(R1,R2,s) == -1){
	// 	    	return true;
	// 	    }
	// 	    else{
	// 	    	return false;
	// 	    }
	// 	}
	// };

	// Compare mysorter(so);

	vector<Record *> v;

	G_curSizeInBytes = 0;
	while (true) {
		Record * curr = new Record();
		if(t->in->Remove(curr)){
			char *b = curr->GetBits();
			int rec_size = ((int *) b)[0];
			if (G_curSizeInBytes + rec_size < (PAGE_SIZE)*rlen) {
				G_curSizeInBytes += rec_size;
				// pq.push(curr);
				v.push_back(curr);
			}
			else{
				if(!savelist(v,first_file)){
					// cout << "overflowww" << endl;
				}
				v.clear();
				v.push_back(curr);
				G_curSizeInBytes = rec_size;
			}	
		}
		else{
			break;
		}
	}
	// Each runlength records are sorted and added to the file

	//Now adding the pending overflowed and last records 
	// cout << "Total : " << vals + v.size() <<  endl;
	vector <Record *> pending;
	pending.reserve( v.size() + overflow.size() ); // preallocate memory
	pending.insert( pending.end(), v.begin(), v.end() );
	pending.insert( pending.end(), overflow.begin(), overflow.end() );
	mysorter.s=so;
	sort(pending.begin(),pending.end(),mysorter);
	// cout << v.size() << " " << overflow.size() << " " << pending.size() << endl;

	first_file->p.EmptyItOut();
	for(int i = 0;i < pending.size();i++){
		Record * rec = pending[i];
		if(!first_file->p.Append(rec)){
			first_file->f.AddPage(&(first_file->p), first_file->curpage);
			first_file->p.EmptyItOut();
			first_file->curpage++;first_file->totalpages++;
			first_file->p.Append(rec);
			// cout << "Adding " << first_file->curpage-1 << endl;
		}
	}

	first_file->f.AddPage(&first_file->p,first_file->curpage);
	first_file->totalpages++;first_file->curpage++;
	// cout << "Adding " << first_file->curpage-1 << endl;

	v.clear();
	overflow.clear();
	pending.clear();
	// clear the pending data as you have already added the records to the file

	// cout << "Initial " << first_file->totalpages << " " << first_file->f.GetLength() << endl;
	
	check_file(first_file,rlen);
	// SendAll(first_file,t);
	// t->out->ShutDown();
	// exit(-1);


	// TPMMS CODE to merge
	first_file->mtype = source;
	second_file->mtype = destination;
	block *first = new block();
	first->curpage = 0;
	block *second = new block();
	second->curpage = rlen;
	first_file->first = first;
	first_file->second = second;
	second_file->first = new block();
	second_file->second = new block();
	second_file->first->curpage = 0;
	second_file->second->curpage = rlen;

	int cur_runlength = rlen;
	merger * source_file = first_file;
	merger * destination_file = second_file;
	int max_page = 0;
	block_state = initial;

	source_file->ex=excess;

	// cout << "Initial start" << endl;
	cout << "\nTPMMS Merge start\n" << endl;
	while(block_state != merge_done){
		switch(block_state){
			case initial:
				if(source_file->mtype != source){
					cout << "Source type doesnt match" << endl;
					exit(-1);
				}
				source_file->first->count = cur_runlength;
				source_file->second->count = cur_runlength;
				// cout << "Merging " << source_file->first->curpage << " " << source_file->second->curpage << endl;
				max_page = source_file->second->curpage + cur_runlength;
				// cout << "Load page of first block" << endl;

				source_file->loadpage(0);

				// cout << "get record of first block" << endl;
				if(!source_file->first->getRecord()){
					source_file->first->curpage++;
					if(!source_file->loadpage(0)){
						block_state = done;
						break;
					}
					else{
						if(!source_file->first->getRecord()){
							block_state = done;
							break;
						}
					}
				}
				// cout << "Load page of second block" << endl;
				if(!source_file->loadpage(1)){
					block_state = second_done;
					break;
				}
				// cout << "get record of second block" << endl;
				// source_file->first->rec.Print(&mySchema);
				if(!source_file->second->getRecord()){
					source_file->second->curpage++;
					if(!source_file->loadpage(1)){
						block_state = second_done;
						break;
					}
					else{
						if(!source_file->second->getRecord()){
							block_state = second_done;
							break;
						}
					}
				}
				block_state = compare;
				break;
			
			case compare:
				// cout << "compare" << endl;
				if((G_comp).Compare(&(source_file->first->rec),&(source_file->second->rec),so) != 1){
					// cout << "add firrst" << endl;
					destination_file->AddRec(source_file->first->rec,max_page);
					block_state = read_first;
				}
				else{
					// Adding the second record
					destination_file->AddRec(source_file->second->rec,max_page);
					block_state = read_second;
				}
				break;

			case read_first:
				// cout << "read_first" << endl;
				if(!source_file->first->getRecord()){
					source_file->first->curpage++;
					if(!source_file->loadpage(0)){
						block_state = first_done;
						break;
					}
					else{
						if(!source_file->first->getRecord()){
							block_state = first_done;
							break;
						}
					}
				}
				block_state = compare;
				break;

			case read_second:
				// cout << "read_second" << endl;
				// cout << block_state << endl;
				if(!source_file->second->getRecord()){
					source_file->second->curpage++;
					if(!source_file->loadpage(1)){
						block_state = second_done;
						break;
					}
					else{

						if(!source_file->second->getRecord()){
							block_state = second_done;
							break;
						}
					}
				}
				block_state = compare;
				break;
			
			case first_done:
				// cout << "first_done" << endl;
				destination_file->AddRec(source_file->second->rec,max_page);
				if(!source_file->second->getRecord()){
					source_file->second->curpage++;
					if(!source_file->loadpage(1)){
						block_state = done;
						break;
					}
					else{
						if(!source_file->second->getRecord()){
							block_state = done;
							break;
						}
					}
				}
				block_state = first_done;
				break;

			case second_done:
				// cout << "second_done" << endl;
				destination_file->AddRec(source_file->first->rec,max_page);
				if(!source_file->first->getRecord()){
					source_file->first->curpage++;
					if(!source_file->loadpage(0)){
						block_state = done;
						break;
					}
					else{
						if(!source_file->first->getRecord()){
							block_state = done;
							break;
						}
					}
				}
				else{
					block_state = second_done;
					break;
				}
				// cout << "adding records" << endl;
				break;

			case done:
				// cout << "Done" << endl;
				destination_file->f.AddPage(&destination_file->p,destination_file->curpage);
				destination_file->p.EmptyItOut();
				destination_file->totalpages++;
				destination_file->curpage++;
				// cout << source_file->first->curpage << " " << source_file->totalpages-2 << endl;
				if(source_file->first->curpage >= source_file->totalpages-1){
					block_state = finish;
					break;
				}
				else{
					// cout << "After Merge " << source_file->first->curpage << " " << source_file->second->curpage << endl;
					source_file->first->curpage += cur_runlength ;
					source_file->second->curpage += + cur_runlength;
					// cout << "Count " << source_file->first->count << " " << source_file->second->count << endl;
					// cout << "Go for next merge " << source_file->first->curpage << " " << source_file->second->curpage << endl;
					block_state = initial;	
					break;
				}
				break;
			
			case finish:
				// cout << "Merge done" << endl;
				// cout << cur_runlength << " " << source_file->curpage << " " << source_file->totalpages-2 << endl;
				destination_file->curpage++;
				if(cur_runlength >= source_file->f.GetLength() ){
					block_state = merge_done;
					break;
				}
				else{
					// cout << "TPMMS Merge done using " << cur_runlength << endl;
					cur_runlength = 2 * cur_runlength;
					// cout << "TPMMS Merge Start with Runlen: " << cur_runlength << endl;
					// cout << cur_runlength << endl;
					// check_file(destination_file,cur_runlength/2);

					// Phase Done
					// Swap Source and Destination and again do TPMMS 
					merger * temp = source_file;
					source_file = destination_file;
					destination_file = temp;
					source_file->mtype = source;
					source_file->curpage = 0;
					destination_file->curpage = 0;
					source_file->first->curpage = 0;
					destination_file->totalpages = 1;
					source_file->second->curpage = cur_runlength;
					destination_file->mtype = destination;
					block_state = initial;
					break;					
				}
				break;

		}
		

	}

	cout << "TPMMS Merge done\n" << endl;
	// cout << "Sending Records " << endl;
	// Sending records
	excess=destination_file->ex;
	SendSocket(destination_file,t);

	t->out->ShutDown();
	// remove(first_file->name);
	// remove(second_file->name);
	// remove ("Bigq.bin"); 
	// remove ("Bigq_temp.bin"); 
	delete first_file;
	delete second_file;
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages

 //    construct priority queue over sorted runs and dump sorted data 
 // 	into the out pipe

 //    finally shut down the out pipe
	// G_sortorder = sortorder;
	// G_runlen = runlen;
	// G_input = {&in, &out};

	t = new BigQ_input;
	*t = {&in,&out};
	rlen = runlen;
	so = new OrderMaker;
	*so = sortorder;

	// pthread_t thread;
	pthread_create (&thread, NULL, TPMMS_helper , this);
	// pthread_create (&thread, NULL, TPMMS , (void *)&G_input);

	// pthread_join (thread, NULL);

}


BigQ::~BigQ () {
}