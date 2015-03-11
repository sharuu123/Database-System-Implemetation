
#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include <stdlib.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {

	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
        FILE *tableFile = fopen ("../DATA/1G/lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	// Page sample;
	DBFile result;
    char* fname;
    fname = "./results.txt";
    result.Create(fname,heap,NULL);
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}
		result.Add(temp);
		// sample.Append(&temp);
		// break;
		// if (comp.Compare (&temp, &literal, &myComparison))
  //               	temp.Print (&mySchema);

        }


  

    // result.AddPage (&sample, result.GetLength());
    // result.Close();
  	result.Close();

  	cout << "check" << endl;
  	DBFile check;
  	Record rec;
  	check.Load(mySchema,fname);
  	cout << "check" << endl;
	check.MoveFirst();
	cout << "check" << endl;
	// int i=0;
	while(check.GetNext(rec, myComparison ,literal)){
		rec.Print (&mySchema);
		// i++;
	}

	check.Close();
  	// checking
    // Page dats;
    // File r;
    // Record stat;
    // r.Open(1,fname);
    // cout << r.GetLength() << endl;
    // r.GetPage(&dats,0);


    // for(int i=0;i<5;i++){
	    
    // 	dats.GetFirst(&stat);
    // 	cout << "record: " << i << endl;
    // 	stat.Print (&mySchema);
    // }

    // r.Close();

}


