#include "RelOp.h"

void * SelectPipe::sp_Runit () {
	// cout << "Inside thread SelectFile::Runit" << endl;
	ComparisonEngine ceng;
	Record rec;
	while(sp_in->in->Remove(&rec)){
		if(ceng.Compare(&rec,sp_in->literal,sp_in->selop)==1){
			sp_in->out->Insert(&rec);
		}
	}
	sp_in->out->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	// cout << "SelectPipe::Run()" << endl;

	sp_input *sp_in = new sp_input;
	*sp_in = {&inPipe, &outPipe, &selOp, &literal};
 	// sf_Ptr sft = &SelectFile::Runit;
	// G_p = *(PthreadPtr*)&sft;
  	pthread_create (&thread, NULL, SelectPipe_helper ,this);
  
}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {
	rlen = runlen;
}


// void * SelectFile::sf_Runit (void * arg) {
// 	// cout << "Inside thread SelectFile::Runit" << endl;
// 	sf_input *t = (sf_input *) arg;
// 	ComparisonEngine ceng;
// 	Record rec;
// 	while(t->inFile->GetNext(rec)){
// 		// cout << "Num of atts in record " << rec.GetNumAtts() << endl;
// 		if(ceng.Compare(&rec,t->literal,t->selop)==1){
// 			t->out->Insert(&rec);
// 		}
// 	}
// 	t->out->ShutDown();
// }

void * SelectFile::sf_Runit () {
	// cout << "Inside thread SelectFile::Runit" << endl;
	
	ComparisonEngine ceng;
	Record rec;
	while(sf_in->inFile->GetNext(rec,*sf_in->selop,*sf_in->literal)){
		// cout << "Num of atts in record " << rec.GetNumAtts() << endl;
			sf_in->out->Insert(&rec);
	}
	sf_in->out->ShutDown();
}


void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	// cout << "SelectFile::Run()" << endl;

	sf_in = new sf_input;
	*sf_in = {&inFile, &outPipe, &selOp, &literal};
 	// sf_Ptr sft = &SelectFile::Runit;
	// G_p = *(PthreadPtr*)&sft;
  	// pthread_create (&thread, NULL, sf_Runit ,(void *)sf_in);
  	pthread_create (&thread, NULL, SelectFile_helper ,this);
  
}



void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	rlen = runlen;
}

void * Project::p_Runit () {
	// cout << "Inside thread Project::Runit" << endl;
	// cout << "Size:" << (t->num_out) << endl;
	for(int i = 0;i< p_in->num_out;i++){
		cout << p_in->keep[i] << endl;
	}
	while(true){
	Record * temp = new Record;
		if(p_in->in->Remove(temp)){
			temp->Project(p_in->keep,p_in->num_out,p_in->num_in);
			p_in->out->Insert(temp);
		}	
		else{
			break;
		}	
	}
	p_in->out->ShutDown();
}
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	// cout << "Project::Run()" << endl;
	p_in = new p_input;
	*p_in = {&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput};
 	// sf_Ptr sft = &SelectFile::Runit;
	// G_p = *(PthreadPtr*)&sft;
  	pthread_create (&thread, NULL, Project_helper ,this);
  
}



void Project::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Project::Use_n_Pages (int runlen) {
	rlen = runlen;
}


void * Sum::s_Runit () {
	// cout << "Inside thread Sum::Runit" << endl;
	Record rec;
	double sum = 0.0;
	while (s_in->in->Remove (&rec)){
		int ival = 0; 
		double dval = 0.0;
		s_in->func->Apply(rec, ival, dval);
		sum += (ival + dval);
	}
	// cout << "Sum: " << sum << endl;
	Attribute DA = {"double", Double};
	Schema out_sch ("out_sch", 1, &DA);
	char buffer[32];
  	sprintf(buffer, "%1.2f", sum);
	// string str = to_string(sum) + "|";
	// cout << str << endl;

	ostringstream out;
    out << buffer;
    string str = out.str() + "|";
	// char *val = str.c_str();
	// char * cls = "10.20|\n";
	// rec.ComposeRecord(&out_sch,cls);
	rec.ComposeRecord(&out_sch,str.c_str());
	rec.Print(&out_sch);
	s_in->out->Insert(&rec);
	s_in->out->ShutDown();
	// cout << "thread done" << endl;
}
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	// cout << "Sum::Run()" << endl;
	s_in = new s_input;
	*s_in = {&inPipe, &outPipe, &computeMe};
 	// sf_Ptr sft = &SelectFile::Runit;
	// G_p = *(PthreadPtr*)&sft;
  	pthread_create (&thread, NULL, Sum_helper ,this);
  
}



void Sum::WaitUntilDone () {
	// cout << "twaiting" << endl;
	pthread_join (thread, NULL);
	// cout << "tjoined" << endl;
}

void Sum::Use_n_Pages (int runlen) {
	rlen = runlen;
}

void * DuplicateRemoval::d_Runit () {
	// cout << "Inside thread Sum::Runit" << endl;
	OrderMaker * ord  = new OrderMaker(d_in->sch);
	DBFile dbfile;
	struct {OrderMaker *o; int l;} startup = {ord, rlen};
	char * path = "./temp.bin";
	dbfile.Create (path, sorted, &startup);
	dbfile.Close();
	dbfile.Open(path);
	dbfile.MoveFirst ();
	Record temp;
	while(d_in->in->Remove(&temp)){
		dbfile.Add(temp);
	}
	// cout << "Created the file" << endl;
	dbfile.MoveFirst ();
	Record recs[2];
	ComparisonEngine comp;
	Record *lasts = NULL, *prevs = NULL;
	int j = 0;
	while (dbfile.GetNext(recs[j%2])) {
		prevs = lasts;
		lasts = &recs[j%2];
		if (prevs && lasts) {
			if (comp.Compare (prevs, lasts, ord) == 0) {
			}
			else{
				// prevs->Print(t->sch);
				d_in->out->Insert(prevs);
			}
		}
		j++;
	}
	// lasts->Print(t->sch);
	d_in->out->Insert(lasts);
	d_in->out->ShutDown();
	// cout << "thread done" << endl;
}
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	// cout << "DuplicateRemoval::Run()" << endl;
	d_in = new d_input;
	*d_in = {&inPipe, &outPipe, &mySchema};
  	pthread_create (&thread, NULL, Duplicate_helper ,this);
  
}



void DuplicateRemoval::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen) {
	rlen = runlen;
}


void * WriteOut::w_Runit () {
	// cout << "Inside thread write::Runit" << endl;
	Record temp;
	while(w_in->in->Remove(&temp)){
		int n = w_in->sch->GetNumAtts();
		Attribute *atts = w_in->sch->GetAtts();
		// loop through all of the attributes
		for (int i = 0; i < n; i++) {
			// print the attribute name
			fprintf(w_in->f, "%s: ", atts[i].name);
		

			// use the i^th slot at the head of the record to get the
			// offset to the correct attribute in the record
			int pointer = ((int *) temp.bits)[i + 1];

			// here we determine the type, which given in the schema;
			// depending on the type we then print out the contents
			fprintf(w_in->f, "[");
			// first is integer
			if (atts[i].myType == Int) {
				int *myInt = (int *) &(temp.bits[pointer]);
				// cout << *myInt;	
				fprintf(w_in->f, "%d", *myInt);

			// then is a double
			} else if (atts[i].myType == Double) {
				double *myDouble = (double *) &(temp.bits[pointer]);
				fprintf(w_in->f, "%f", *myDouble);	

			// then is a character string
			} else if (atts[i].myType == String) {
				char *myString = (char *) &(temp.bits[pointer]);
				fprintf(w_in->f, "%s", myString);	
			} 

			fprintf(w_in->f, "]");

			// print out a comma as needed to make things pretty
			if (i != n - 1) {
				fprintf(w_in->f, ",");
			}
		}

		fprintf(w_in->f, "\n");
	}
	// cout << "thread done" << endl;
}
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	// cout << "WriteOut::Run()" << endl;
	w_in = new w_input;
	*w_in = {&inPipe, outFile, &mySchema};
  	pthread_create (&thread, NULL, WriteOut_helper ,this);
  
}



void WriteOut::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void WriteOut::Use_n_Pages (int runlen) {
	rlen = runlen;
}

void * GroupBy::g_Runit () {
	// cout << "Inside thread write::Runit" << endl;
	DBFile dbfile;
	struct {OrderMaker *o; int l;} startup = {g_in->ord, rlen};
	char * path = "./group_temp.bin";
	dbfile.Create (path, sorted, &startup);
	dbfile.Close();
	dbfile.Open(path);
	dbfile.MoveFirst ();
	Record temp;
	while(g_in->in->Remove(&temp)){
		dbfile.Add(temp);
	}
	dbfile.MoveFirst ();
	Record recs[2];
	ComparisonEngine comp;
	Record *lasts = NULL, *prevs = NULL;
	int j = 0;
	double sum = 0.0;
	Attribute DA = {"double", Double};
	Schema out_sch ("out_sch", 1, &DA);
	// cout << "shit is about to het serious " << endl;
	while (dbfile.GetNext(recs[j%2])) {
		prevs = lasts;
		lasts = &recs[j%2];
		// cout << "loop start" << endl;
		if (prevs && lasts) {
			int ival = 0; 
			double dval = 0.0;
			// cout << "error" << endl;
			g_in->func->Apply(*prevs, ival, dval);
			sum += (ival + dval);
			if (comp.Compare (prevs, lasts, g_in->ord) == 0) {
				
			}
			else{
				// diff records - generate new record for prevs group
				char buffer[32];
  				sprintf(buffer, "%1.2f", sum);
				ostringstream out;
    			out << buffer;
    			string str = out.str() + "|";
    			// cout << str << endl;
    			// cout << "loop" << endl;
				prevs->ComposeRecord(&out_sch,str.c_str());
				prevs->Print(&out_sch);
				sum = 0.0;
				g_in->out->Insert(prevs);
				// cout << "loop end" << endl;
				// t->out->Insert(prevs);
			}
		}
		j++;
	}
	// generating for the last record
	// cout << "end loop" << endl;
	int ival = 0; 
	double dval = 0.0;
	g_in->func->Apply(*lasts, ival, dval);
	sum += (ival + dval);
	char buffer[32];
	sprintf(buffer, "%1.2f", sum);
	// cout << "Done" << endl;
	ostringstream out;
	out << buffer;
	string str = out.str() + "|";
	// cout << "Holy shit" << endl;
	lasts->ComposeRecord(&out_sch,str.c_str());
	// lasts->Print(&out_sch);
	// cout << str << endl;
	// lasts->Print(t->sch);
	g_in->out->Insert(lasts);
	g_in->out->ShutDown();
	// cout << "Groupby thread done" << endl;

}
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	// cout << "WriteOut::Run()" << endl;
	g_in = new g_input;
	*g_in = {&inPipe, &outPipe, &groupAtts, &computeMe};
  	pthread_create (&thread, NULL, GroupBy_helper ,this);
  
}



void GroupBy::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void GroupBy::Use_n_Pages (int runlen) {
	rlen = runlen;
}

void* Join::j_Runit () {
	// cout << "Join::Runit" << endl;
	// Create ordermaker
	OrderMaker so1,so2;
	j_in->cnf->GetSortOrders(so1,so2);
	// so1.Print();
	// so2.Print();
	DBFile db1,db2;
	struct {OrderMaker *o; int l;} startup1 = {&so1, rlen};
	struct {OrderMaker *o; int l;} startup2 = {&so2, rlen};

	char *path1 = "./join_left_temp.bin";
	char *path2 = "./join_right_temp.bin";
	remove(path1);
	remove(path2);
	db1.Create (path1, sorted, &startup1);
	db2.Create (path2, sorted, &startup2);
	db1.Close();
	db1.Open(path1);
	db2.Close();
	db2.Open(path2);
	Record temp;
	int l, r;
	// cout << " starting merging" << endl;
	// if(t->inL->Remove(&temp)){
	// 	l = temp.GetNumAtts();
	// 	db1.Add(temp);
	// }
	// if(t->inR->Remove(&temp)){
	// 	r = temp.GetNumAtts();
	// 	db2.Add(temp);
	// }
	while(j_in->inL->Remove(&temp)){
		l = temp.GetNumAtts();
		// cout << ".";
		db1.Add(temp);
		// cout << "*" ;
	}
	// cout << "One file records done" << endl;
	while(j_in->inR->Remove(&temp)){
		r = temp.GetNumAtts();
		// cout << ".";
		db2.Add(temp);
		// cout << "*" ;
	}
	// cout << "records added" << endl;
	int A[l+r];
	// cout << "Size :" <<  l+r << endl;
	int i = 0;
	for(i = 0;i<l;i++){
		A[i] = i;
	}
	for(int k=0;k<r;k++){
		A[i]=k;
		i++;
	}
	// for(int k = 0;k<i;k++){
	// 	cout << A[k] << "\t" ;
	// }
	// cout << "Adding records" << endl;
	// cout << "Add Done" << endl;
	db1.MoveFirst();
	
	// cout << "first mode" << endl;
	db2.MoveFirst();
	// cout << "second" << endl;
	ComparisonEngine comp;
	Record rec1,rec2;
	// cout << "starting comparison merger" << endl;
	int flag1 = 0;
	int flag2 = 0;
	if(db1.GetNext(rec1) && db2.GetNext(rec2)){
		flag1 = 1;
		flag2= 1;
	}
	while(true){
		if(flag1 && flag2){
			int res = comp.Compare(&rec1,&so1,&rec2,&so2);
			if(res == 0){
				// merge
				Record *temp = new Record();
				temp->MergeRecords (&rec1,&rec2,l,r,A,l+r-(so2.numAtts),l);		
				// push to output
				// cout << "pushing" << endl;
				j_in->out->Insert(temp);
				if(!(db1.GetNext(rec1) && db2.GetNext(rec2))){
					flag1 = 0;
					flag2= 0;
				}
			}
			else if(res == -1){
				if(!db1.GetNext(rec1)) {flag1 = 0; break;}
			}
			else{
				if(!db2.GetNext(rec2)) {flag2 = 0; break;}
			}
		}
		else{
			break;
		}
	}
	// cout << "done" << endl;
	j_in->out->ShutDown();
	// cout << "Join thread done" << endl;
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	// cout << "Join::Run()" << endl;
	j_in = new j_input;
	*j_in = {&inPipeL, &inPipeR, &outPipe, &selOp, &literal};
  	pthread_create (&thread, NULL, Join_helper ,this);
  
}

void Join::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int runlen) {
	rlen = runlen;
}