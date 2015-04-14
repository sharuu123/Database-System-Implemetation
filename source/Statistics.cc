#include "Statistics.h"

using namespace std;

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
	string relation(relName);
	RelInfo newrelinfo;
	newrelinfo.SetTuples(numTuples);
	relation_stats[relation] = newrelinfo;
	// newrelinfo.print();
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	string relation(relName);
	string attribute(attName);
	if(numDistincts==-1){
		relation_stats[relation].AddAtt(attribute,relation_stats[relation].numTuples);
	} else {
		relation_stats[relation].AddAtt(attribute,numDistincts);
	}
	attribute_stats[attribute] = relation;
	// relation_stats[relation].print();
}

void Statistics::CopyRel(char *oldName, char *newName)
{	
	string oldrelation(oldName);
	string newrelation(newName);
	// cout << oldrelation << " chaging to " << newrelation << endl;
	// map<string,int> oldAttInfo = rels[oldname].AttInfo;
	RelInfo newrelinfo;
	newrelinfo.SetTuples(relation_stats[oldrelation].numTuples);

	for(auto& i:relation_stats[oldrelation].AttInfo){
		string newattr(newrelation + "." + i.first);
		newrelinfo.AddAtt(newattr,i.second);
		// cout << newattr << " " <<  newrelation << endl;
		attribute_stats[newattr] = newrelation;
	}
	// newrelinfo.print();
	relation_stats[newrelation] = newrelinfo;

}
	
void Statistics::Read(char *fromWhere)
{
    ifstream stats_file(fromWhere);
    if (!stats_file.good()){
        return;
    }
    unsigned count;

    stats_file >> count;
    for(int i=0;i<count;i++){
        string rel;
        RelInfo relinfo;
        stats_file >> rel;
        stats_file >> relinfo;
        relation_stats[rel]=relinfo;
    }

    stats_file >> count;
    for(int i=0;i<count;i++){
        string attribute;
        string rel;
        stats_file >> attribute;
        stats_file >> rel;
        attribute_stats[attribute]=rel;
    }

    stats_file >> count;
    for(int i=0;i<count;i++){
        string rel;
        string mergedrel;
        stats_file >> rel;
        stats_file >> mergedrel;
        merged_stats[rel]=mergedrel;
    }
}

void Statistics::Write(char *fromWhere)
{
    ofstream stats_file(fromWhere);
    stats_file << relation_stats.size() << endl;
    for(auto i:relation_stats){
        stats_file << i.first << endl;
        stats_file << i.second << endl;
    }

    stats_file << attribute_stats.size() << endl;
    for(auto i:attribute_stats){
        stats_file << i.first << endl;
        stats_file << i.second << endl;
    }

    stats_file << merged_stats.size() << endl;
    for(auto i:merged_stats){
        stats_file << i.first << endl;
        stats_file << i.second << endl;
    }

    stats_file.close();
}

vector<string> Statistics::CheckParsetree(struct AndList *p_And){
	vector<string> attrs;
	while(p_And){
		OrList *p_or = p_And->left;
		while(p_or){
			ComparisonOp *comp = p_or->left;
			if(comp!=NULL){
				Operand *leftop = comp->left;
				if(leftop!=NULL && leftop->code == NAME){
					string attribute(leftop->value);
					if(attribute_stats.count(attribute)==0){
						cout << "attribute name " << attribute << " not found" << endl;
						exit(-1);
					}
					attrs.push_back(attribute);
				}
				Operand *rightop = comp->right;
				if(rightop!=NULL && rightop->code == NAME){
					string attribute(rightop->value);
					if(attribute_stats.count(attribute)==0){
						cout << "attribute name " << attribute << " not found" << endl;
						exit(-1);
					}
					attrs.push_back(attribute);
				}

			}
			p_or = p_or->rightOr;
		}
		p_And = p_And->rightAnd;
	}
	return attrs;
}

void Statistics::CheckRelations(char *relNames[],int numToJoin){
	for(int i=0;i<numToJoin;i++){
		string rel(relNames[i]);
		if(relation_stats.count(rel)==0 && merged_stats.count(rel)==0){
			cout << "Relation not found" << rel << endl;
			exit(-1);
		}
	}
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin){
	// # todo
	// Parsetree check
	// CheckParsetree(parseTree);
	// relations check
	CheckRelations(relNames,numToJoin);

	// Do estimate
	double estimate = 0.0l;
	if (0 == parseTree and numToJoin <= 2){
		double result = 1.0l;
		for (signed i = 0; i < numToJoin; i++){
			string relation(relNames[i]);
			result *= relation_stats[relation].numTuples;
		}
		estimate = result;
	}
	else{
		estimate = EstimateResult(parseTree);
	}

	string newrelation;
	if (Case_Join(parseTree)){
		for(int i=0;i<numToJoin;i++){
			string rel(relNames[i]);
			newrelation += rel;
		}

		RelInfo joinRelInfo;
		joinRelInfo.numTuples=estimate;

		for(int i=0;i<numToJoin;i++){
			joinRelInfo.copyAttrs(relation_stats[relNames[i]]);
		}

		// joinRelInfo.print();
		relation_stats[newrelation]=joinRelInfo;

		vector<string> attrs = CheckParsetree(parseTree);
		set<string> relset;

		for(auto i:attrs){
			relset.insert(attribute_stats[i]);
		}

		for(auto i:relset){
			joinRelInfo.copyAttrs(relation_stats[i]);
			relation_stats.erase(i);
		}

		for(int i=0;i<numToJoin;i++){
			relation_stats.erase(relNames[i]);
			merged_stats[relNames[i]]=newrelation;
		}

		for(auto i:joinRelInfo.AttInfo){
			attribute_stats[i.first]=newrelation;
		}
	}
}


double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
	if (0 == parseTree and numToJoin <= 2){
		double result = 1.0l;
		for (signed i = 0; i < numToJoin; i++){
			string relation(relNames[i]);
			result *= relation_stats[relation].numTuples;
		}
		return result;
	}
	// # todo
	// Parsetree check
	// CheckParsetree(parseTree);
	// relations check
	CheckRelations(relNames,numToJoin);

	double result = EstimateResult(parseTree);
  	// cout << "estimated result is " << result << endl;
  	return result;
}

bool Statistics::Case_Join(struct AndList *p_And){
	bool case_join = false;
	while(p_And){
		OrList *p_or = p_And->left;
	 	while(p_or){
	 		ComparisonOp * comp = p_or->left;
	 		if(comp != NULL){
	 			Operand *leftop = comp->left;
	 			Operand *rightop = comp->right;
	 			if( (leftop != NULL && (leftop->code == NAME)) &&
		 			(rightop != NULL && (rightop->code == NAME) ) && comp->code == EQUALS) {
					// Join sucks
					// cout << "Case join" << endl;
					case_join = true;   
					return case_join;                   	
				}

		 	}
	 		p_or = p_or->rightOr;
	 	}
		p_And = p_And->rightAnd;

	}
	return case_join;
}


double Statistics::EstimateResult(struct AndList *p_And){
	double result = 1.0l;
	bool case_join = false;
	double selectOnlySize = 0.0l;

	while (p_And){
	 	OrList *p_or = p_And->left;
	 	bool independent = true;
	 	bool single = false;
	 	set <string> orset;
	 	int count = 0;

	 	// Checking for independent or dependent or
	 	while(p_or){ // looping through or list
	 		ComparisonOp * comp = p_or->left;
	 		if(comp != NULL){
	 			count++;
	 			string attribute(comp->left->value); // get left attribute val
	 			orset.insert(attribute);	// add to or set
	 			// cout << "Inserting attribute: " << attribute << endl;
	 		}
	 		p_or = p_or->rightOr; // set next right or
	 	}
	 	if(orset.size() != count){
	 		independent = false;
	 	}
	 	if(count == 1){
	 		independent = false;
	 		single = true;
	 	}

	 	// cout << "Independent: " << independent << "Single: " << single << endl;

	 	p_or = p_And->left;
	 	double ORresult = 0.0l;
	 	if(independent){
	 		ORresult = 1.0l;
	 	}

	 	// Estimating 

	 	while(p_or){
	 		ComparisonOp * comp = p_or->left;
	 		if(comp != NULL){
	 			Operand *leftop = comp->left;
	 			Operand *rightop = comp->right;
	 			switch(comp->code){
	 				case EQUALS:
	 					// cout << "Equals" << endl;
	 					if( (leftop != NULL && (leftop->code == NAME)) &&
	 						 (rightop != NULL && (rightop->code == NAME) )) {
	 						 	// Join sucks
		 						// cout << "Case join" << endl;
                        case_join = true;
                        string lattr(leftop->value);
                        string rattr(rightop->value);
                        // cout << lattr << " " << rattr << endl;
                        // for(auto& i:attribute_stats){
                        // 	cout << i.first << " " << i.second << endl;
                        // }

                        string lrel = attribute_stats[lattr];
                        string rrel = attribute_stats[rattr];
                        // cout << lrel << " " << rrel << endl;

                        int lRelSize = relation_stats[lrel].numTuples;
                   		
                        int lDistinct = relation_stats[lrel].AttInfo[lattr];
                        
                        
                        int rRelSize = relation_stats[rrel].numTuples;
                        int rDistinct = relation_stats[rrel].AttInfo[rattr];

                        // cout << lRelSize << " " << lDistinct << " " << rRelSize << " " << rDistinct << endl;
                        double denominator = max(lDistinct,rDistinct);
                        ORresult += (min(lRelSize,rRelSize) * (max(rRelSize,lRelSize) / denominator));
                        // cout << "Joing : " << ORresult << endl;
                      	
	 					}
	 					else{
	 						Operand *record = NULL;
	 						Operand *literal = NULL;
	 						if(leftop->code == NAME){
	 							record = leftop;
	 							literal = rightop;
	 						}
	 						else{
	 							record = rightop;
	 							literal = leftop;
	 						}

	 						string attribute(record->value);
	 						string relation = attribute_stats[attribute];
	 						int distinct = relation_stats[relation].AttInfo[attribute];
	 						// if(single){
	 						// 	double prob = (1.0l/distinct);
	 						// 	ORresult += prob;
	 						// }
	 						// else{
	 						// 	if(independent){
	 						// 		double prob = 1.0l - (1.0l/distinct);
		 					// 		ORresult *= prob;
	 						// 	}
	 						// 	else{
	 						// 		double prob = (1.0l/distinct);
	 						// 		ORresult += prob;
	 						// 	}
	 						// }
	 						if(independent && !single){
	 							double prob = 1.0l - (1.0l/distinct);
		 							ORresult *= prob;
	 						}
	 						else{
	 							double prob = (1.0l/distinct);
	 								ORresult += prob;
	 						}
	 						// cout << "Result after " << attribute << " "  << ORresult<< endl;

	 					}
	 					break;
	 				case LESS_THAN : case GREATER_THAN:
	 					Operand *record = NULL;
 						Operand *literal = NULL;
 						if(leftop->code == NAME){
 							record = leftop;
 							literal = rightop;
 						}
 						else{
 							record = rightop;
 							literal = leftop;
 						}

 						string attribute(record->value);
 						string relation = attribute_stats[attribute];
 						// int distinct = relation_stats[relation].AttInfo[attribute];

	 					if(independent){
	 						double prob = 1.0l - (1.0l/3.0l);
 							ORresult *= prob;
 							// cout << "independent " << ORresult << endl;
	 					}
	 					else{

							double prob = 1.0l/3.0l;
 							ORresult += prob;
 							// cout << "dependent " << ORresult << endl;
	 					}
	 					// cout << "Result after " << attribute << " "  << ORresult<< endl;
	 					break;
	 			}
	 			if (!case_join){
               Operand *record = NULL;
               if (leftop->code == NAME)
                 {record = leftop;}
               else if (rightop->code == NAME)
                 {record = rightop;}

					string attribute(record->value);
					string relation = attribute_stats[attribute];
					int relationSize = relation_stats[relation].numTuples;
					selectOnlySize = relationSize;
            }

	 		}
	 		p_or = p_or->rightOr;
	 	}

	 	if(independent){
	 		// cout << "independent " << ORresult << endl;
	 		result *= (1 - ORresult);
	 	}
	 	else{
	 		// cout << "dependent " << ORresult << endl;
	 		result *=  ORresult;
	 	}

		p_And = p_And->rightAnd;

	}
	if (case_join){ return result; }
	return result * selectOnlySize;
}
