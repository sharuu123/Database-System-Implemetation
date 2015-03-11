#include "fused-src/gtest/gtest.h"
//#include "gmock/gmock.h"
#include "DBFile.h"

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}


extern struct AndList *final;


// make sure that the file path/dir information below is correct
char *dbfile_dir = ""; // dir where binary heap files should be stored
char *tpch_dir ="../DATA/10M/"; // dir where dbgen tpch files (extension *.tbl) can be found
char *catalog_path = "../source/catalog"; // full path of the catalog file


int open_test(DBFile &db, char * fname, Schema &mySchema){
    try{
    	// opens the lineitem.tbl fine and loads the records into bin file.
    	// returns 1 if success else 0.
	    char rpath[100];
	    sprintf (rpath, "%s%s.bin", dbfile_dir , fname);

	    if(!db.Create(rpath,heap,NULL)){
	    	cout << "Create is failing" << endl;
	    	return 0;
	    }
	    char tbl_path[100]; // construct path of the tpch flat text file
		sprintf (tbl_path, "%s%s.tbl", tpch_dir, fname);
	    cout << "Loading data from " <<  tbl_path << endl;
	    db.Load(mySchema,tbl_path);
		if(!db.Close ()){
			cout << "Close is failing" << endl;
			return 0;
		}
	    return 1;    	
    }
    catch(exception& e){
    	cout << e.what() << '\n';
	    return 0;
    }


}





int load_test(DBFile &db, char * fname, Schema &mySchema){
    try{
    	// reopen the loaded file and check the number of records
	    char rpath[100];
	    sprintf (rpath, "%s%s.bin", dbfile_dir , fname);
	    if(!db.Open(rpath)){
	    	cout << "Unable to open the file" << endl;
	    	return 0;
	    }
		db.MoveFirst ();
		Record temp;
		int counter = 0;
		while (db.GetNext (temp) == 1) {
			counter += 1;
			// temp.Print (&mySchema);
			// if (counter % 10000 == 0) {
			// 	cout << counter << "\n";
			// }
		}
		cout << " selected " << counter << " recs \n";
		db.Close ();	    
	    return counter;    	
    }
    catch(exception& e){
    	cout << e.what() << '\n';
	    return 0;
    }


}

int add_test(DBFile &db, char * fname, Schema &mySchema){
    try{
    	// Fetch the first record and add it at the end
	    char rpath[100];
	    sprintf (rpath, "%s%s.bin", dbfile_dir , fname);
	    db.Open(rpath);
		db.MoveFirst ();
		Record temp;
		if (db.GetNext (temp) == 1) {
			db.Add(temp);
			// temp is null after this as Add consumes the record.
			// gives a seg fault if you try to print it.
		}
		else{
			return 0;
		}
		db.Close ();	    
	    return 1;    	
    }
    catch(exception& e){
    	cout << e.what() << '\n';
	    return 0;
    }
}


int getnext_cnf_test(DBFile &db, char * fname, Schema &mySchema){
    try{
    	// Fetch the first record and add it at the end
	    char rpath[100];
	    sprintf (rpath, "%s%s.bin", dbfile_dir , fname);
	    db.Open(rpath);
		db.MoveFirst ();
		cout << "Enter in your CNF: ";
	  	if (yyparse() != 0) {
			cout << "Can't parse your CNF.\n";
			exit (1);
		}
		Record temp;
		Record literal;
		CNF myComparison;
		myComparison.GrowFromParseTree (final, &mySchema, literal);
		myComparison.Print ();
		int counter = 0;
		while (db.GetNext(temp, myComparison, literal) == 1) {
			counter += 1;
			// temp.Print (&mySchema);
		}
		db.Close ();	    
	    return counter;    	
    }
    catch(exception& e){
    	cout << e.what() << '\n';
	    return 0;
    }
}



TEST(lineitem_test, Open_test){
	cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " heap files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";
	
	DBFile db;
	// MockDBfile db;
	char * fname;
	string filename = "lineitem";
	fname = (char *)filename.c_str();
	Schema mySchema (catalog_path, "lineitem");
	EXPECT_EQ(1,open_test(db, fname, mySchema));
}

TEST(lineitem_test, Load_test){
	DBFile db;
	// MockDBfile db;
	char * fname;
	string filename = "lineitem";
	fname = (char *)filename.c_str();
	Schema mySchema (catalog_path, "lineitem");
	EXPECT_EQ(60175,load_test(db, fname, mySchema));
}

TEST(lineitem_test, Add_test){
	DBFile db;
	// MockDBfile db;
	char * fname;
	string filename = "lineitem";
	fname = (char *)filename.c_str();
	Schema mySchema (catalog_path, "lineitem");
	EXPECT_EQ(1,add_test(db, fname, mySchema));
}

TEST(lineitem_test, Write_test){
	DBFile db;
	// MockDBfile db;
	char * fname;
	string filename = "lineitem";
	fname = (char *)filename.c_str();
	Schema mySchema (catalog_path, "lineitem");
	EXPECT_EQ(60176,load_test(db, fname, mySchema));
}

TEST(lineitem_test, Getnext_test){
	DBFile db;
	// MockDBfile db;
	char * fname;
	string filename = "lineitem";
	fname = (char *)filename.c_str();
	Schema mySchema (catalog_path, "lineitem");
	// TODO :
	EXPECT_EQ(30,getnext_cnf_test(db, fname, mySchema));
}


TEST(lineitem_test, Mock_Open_test){
	DBFile db;
	// MockDBfile db;
	char * fname;
	string filename = "lineitem";
	fname = (char *)filename.c_str();
	Schema mySchema (catalog_path, "lineitem");
	// EXPECT_CALL(db,Load(_,_)).Times(AtLeast(1));
	EXPECT_EQ(1,open_test(db, fname, mySchema));
}




// TEST(regions_test, OpenCreatetest){
// 	// DBFile db;
// 	MockDBfile db;
// 	char * fname;
// 	string filename = "lineitem";
// 	fname = (char *)filename.c_str();
// 	Schema mySchema ("catalog", "lineitem");
// 	// EXPECT_CALL(db,Load(_,_)).Times(AtLeast(1));
// 	EXPECT_CALL(db,Add(_)).Times(60175);
// 	// EXPECT_CALL(db,GetNext(_,_,_)).Times(6017999);
// 	EXPECT_EQ(1,open_test(db, fname, mySchema));
// 	EXPECT_EQ(60175,load_test(db, fname, mySchema));
	
// 	// EXPECT_EQ(1,open_test(db));
// 	// EXPECT_EQ(1,close_test(db));
// }
