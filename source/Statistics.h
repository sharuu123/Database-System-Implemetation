#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include <string>
#include <set>
#include <vector>
#include <iostream>
#include <fstream>
using namespace std;

class RelInfo
{
public:
	int numTuples;
	map<string,int> AttInfo;
	RelInfo(){}
	~RelInfo(){}
	void SetTuples(int num){
		numTuples = num;
	}
	void AddAtt(string att, int num){
		AttInfo[att]=num;
	}
	void print(){
		cout << numTuples << endl;
		for(auto& i:AttInfo){
			cout << i.first << " => " << i.second << endl;
		}
	}
	void copyAttrs(RelInfo & rel){
		AttInfo.insert(rel.AttInfo.begin(),rel.AttInfo.end());
	}
	friend ostream& operator<<(ostream& os, RelInfo &relinfo){

        os << relinfo.numTuples << endl;
        os << relinfo.AttInfo.size() << endl;

        for(auto i:relinfo.AttInfo){
            os << i.first << endl;
            os << i.second << endl;
        }
        return os;
    }
    friend istream& operator>>(istream& is, RelInfo &relinfo){

        int numTuples;
        is >> numTuples;
        relinfo.numTuples = numTuples;
        int numatts;
        is >> numatts;

        for (int i = 0; i < numatts; i++){
            string attr;
            int distinct;
            is >> attr;
            is >> distinct;
            relinfo.AttInfo[attr] = distinct;
        }
        return is;
    }

};


class Statistics
{
public:
	map<string,RelInfo> relation_stats;
	map<string,string> attribute_stats;
	map<string,string> merged_stats;

	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	bool Case_Join(struct AndList *parseTree);
	double EstimateResult(struct AndList *parseTree);
	vector<string> CheckParsetree(struct AndList *p_And);
	void CheckRelations(char *relNames[],int numToJoin);
};


#endif
