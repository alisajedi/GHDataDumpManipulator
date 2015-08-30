import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import Utils.Constants;
import Utils.Constants.ConditionType;
import Utils.Constants.FieldType;
import Utils.Constants.JoinType;
import Utils.Constants.LogicalOperand;
import Utils.FileManipulationResult;
import Utils.MyUtils;
import Utils.TSVManipulations;
import Utils.Constants.SortOrder;


public class LanguagesStudy {
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static void measure_languageBased_WatchAndForkInfluenceMetrics(String inputPath, String inputPathForJustNumericFields, String outputPath, 
			int indentationLevel, long testOrReal, int showProgressInterval, String writeMessageStep){
		//There should be these files (with all fields) in the inputPath: "projects.tsv", "users.tsv" and "watchers.tsv".
		//There should be this file (just numeric fields) in the inputPathForJustNumericFields: "projects.tsv" with 4 columns.
		Date d1 = new Date();
		System.out.println("-----------------------------------");
		System.out.println("-----------------------------------");
		FileManipulationResult totalFMR = new FileManipulationResult(), fMR;
		
		if (!writeMessageStep.equals(""))
			writeMessageStep = writeMessageStep + "-";
		//1- Counting languages and saving them in "languagesAndTheirNumberOfProjects.tsv":
		String numberOfLanguagesFieldName = "numberOfProjects";
		String languagesOutputFileName = "languagesAndTheirNumberOfProjects.tsv";
		fMR = TSVManipulations.runGroupBy_count_andSaveResultToTSV(inputPath, "projects.tsv", outputPath, languagesOutputFileName, 
				"language", 9, "language", numberOfLanguagesFieldName, SortOrder.DEFAULT_FOR_STRING, 
				true, indentationLevel, testOrReal, showProgressInterval, writeMessageStep+"1");
		totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);

		//2- Reading the result file (of languages and number of projects of each) ordered by number of projects using each language:
		FileManipulationResult[] fMRArray = new FileManipulationResult[1]; 
		fMRArray[0] = new FileManipulationResult();
		TreeMap<String, ArrayList<String[]>> numberOfProjectsAndRespectiveLanguages = TSVManipulations.readNonUniqueKeyAndItsValueFromTSV(
				outputPath+"\\"+languagesOutputFileName, fMRArray, 
				null, 1, SortOrder.DESCENDING_INTEGER, 2, "0", LogicalOperand.NO_CONDITION, 
				0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
				true, showProgressInterval, indentationLevel, testOrReal, writeMessageStep+"2");
		totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);

		//3- Filter the projects to <the projects that are using top 10 languages> (and saving the filtered projects in 10 different files; "projects_1_JavaScript.tsv", "projects_2_Ruby.tsv", etc.):
		MyUtils.println(writeMessageStep+"3- Producing different files for projects of different languages: ", indentationLevel);
		int numberOfLanguagesConsidered = 0;
		for (Map.Entry<String, ArrayList<String[]>> entry:numberOfProjectsAndRespectiveLanguages.entrySet()){
			if (!entry.getValue().get(0)[0].equals("")){//:i.e., if this is not the number of projects without a language.
				for (int i=0; i<entry.getValue().size(); i++){//i.e., for (the first 10 = first Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY) languages with the same number of projects.
					numberOfLanguagesConsidered++;
					String language = entry.getValue().get(i)[0];
					MyUtils.println(writeMessageStep+"3-" + numberOfLanguagesConsidered + "- " + language + " (mentioned " + entry.getKey() + " times)", indentationLevel+1);
					//filter projects.tsv to "language":
					TreeMap<String, ArrayList<String[]>> projectsWithASpecificLanguage = TSVManipulations.readNonUniqueKeyAndItsValueFromTSV(
							inputPath+"\\"+"projects.tsv", fMRArray, 
							null, 0, SortOrder.ASCENDING_INTEGER, 9, "2$7$8", LogicalOperand.IGNORE_THE_SECOND_OPERAND, 
							5, ConditionType.EQUALS, language, FieldType.NOT_IMPORTANT, 0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
							false, showProgressInterval, indentationLevel+2, testOrReal, writeMessageStep+"3-" + numberOfLanguagesConsidered + "-1");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMRArray[0]);
					
					//Now, saving this list of filtered projects:
					String filteredProjectsFileName = "projects_"+numberOfLanguagesConsidered+"_"+language + ".tsv";
					String neededFieldsInProjects = "id" + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + 
							"ownerId" + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE +  
							"forkedFrom" + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + "deleted";
					fMR = TSVManipulations.saveTreeMapToTSVFile(outputPath+"\\"+filteredProjectsFileName, projectsWithASpecificLanguage, neededFieldsInProjects, true, 
							false, showProgressInterval, indentationLevel+2, testOrReal, writeMessageStep+"3-" + numberOfLanguagesConsidered + "-2");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					
					if (numberOfLanguagesConsidered >= Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY)
						break;
				}
				if (numberOfLanguagesConsidered >= Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY)
					break;
			}//if (!entry....
		}//for(Map.Entry....
		totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
		System.out.println("======================");

		//4- counting languageBased#ofWatchers and languageBased#ofForks of every user (for each top language) (first 10 = first Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY):
		//and saving them in separate files (all with PK="userId")
		System.out.println("4- Counting #of_LANGUAGE_Watchers and #of_LANGUAGE_Forks for users: ");
		//First, define (and initialize) arrayLists for keeping file names and field names for joining them later:
		//
		ArrayList<String> fileNamesForInfluenceMetrics_watchers = new ArrayList<String>();
		fileNamesForInfluenceMetrics_watchers.add("users.tsv");
		ArrayList<String> keysToJoin_watchers = new ArrayList<String>();
		keysToJoin_watchers.add("id");
		ArrayList<String> neededFields_watchers = new ArrayList<String>();
		neededFields_watchers.add("id\tlogin");

		ArrayList<String> fileNamesForInfluenceMetrics_forks = new ArrayList<String>();
		fileNamesForInfluenceMetrics_forks.add("users.tsv");
		ArrayList<String> keysToJoin_forks = new ArrayList<String>();
		keysToJoin_forks.add("id");
		ArrayList<String> neededFields_forks = new ArrayList<String>();
		neededFields_forks.add("id\tlogin");
		//Now, counting languageBased#ofWatchers and languageBased#ofForks of every user (for each top language): 
		numberOfLanguagesConsidered = 0;
		for (Map.Entry<String, ArrayList<String[]>> entry:numberOfProjectsAndRespectiveLanguages.entrySet()){
			if (!entry.getValue().get(0)[0].equals("")){//:i.e., if this is not the number of projects without a language.
				for (int i=0; i<entry.getValue().size(); i++){//i.e., for (the first 10 = first Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY) languages with the same number of projects.
					numberOfLanguagesConsidered++;
					String language = entry.getValue().get(i)[0];
					MyUtils.println("4-" + numberOfLanguagesConsidered + "- " + language, indentationLevel+1);
					//A- Watchers (watchers_1 to watchers_10; for top 10 languages):
					//First, count the number of watchers for each project (and save it in "temp-projects1-watchers_i_LANGUAGE.tsv"):
					String temp_project1_watchers = "temp-projects1-watchers_"+numberOfLanguagesConsidered+"_"+language+".tsv", 
							title1_projectId = "projectId", title2_numWatchers = "numberOfWatchers";
					fMR = TSVManipulations.runGroupBy_count_andSaveResultToTSV(inputPath, "watchers.tsv", outputPath, temp_project1_watchers, "repoId", 2, 
							title1_projectId, title2_numWatchers, SortOrder.ASCENDING_INTEGER, 
							true, indentationLevel+2, testOrReal, showProgressInterval*2, writeMessageStep+"4-" + numberOfLanguagesConsidered + "-1");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					
					//Then, using this number of watchers, convert <projectId, userId> in 'filteredProjectsFileName' to <#OfWatchers, userId> and save it in 'temp_project1_watchers_i_LANGUAGE_idReplacedByNumberOfWatchers':
					String temp_project1_idReplacedByNumberOfWatchers= "temp-projects1_watchers_"+numberOfLanguagesConsidered+"_"+language+"-idReplacedByNumberOfWatchers.tsv";
					String filteredProjectsFileName = "projects_"+numberOfLanguagesConsidered+"_"+language + ".tsv";
					fMR = TSVManipulations.replaceForeignKeyInTSVWithValueFromAnotherTSV(outputPath+"\\"+filteredProjectsFileName, outputPath+"\\"+temp_project1_watchers, 
							outputPath+"\\"+temp_project1_idReplacedByNumberOfWatchers, "id", 4, title1_projectId, 2, title2_numWatchers, title2_numWatchers, 
							true, showProgressInterval, indentationLevel+2, testOrReal, writeMessageStep+"4-" + numberOfLanguagesConsidered + "-2");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);

					//Now, using this #ofWatchers, get sum(numberOfWatchers) for each "userId" and save it as "usersAndNumberOfUsersWatchingTheirProjects.tsv":
					String numberOfWatchersFieldName = "numberOfWatchersOfProjectsOfThisUser_"+numberOfLanguagesConsidered+"_"+language;
					String watchersOutputFileName = "usersAndNumberOfUsersWatchingTheirProjects_"+numberOfLanguagesConsidered+"_"+language+".tsv";
					fMR = TSVManipulations.runGroupBy_sum_andSaveResultToTSV(outputPath, temp_project1_idReplacedByNumberOfWatchers, outputPath, watchersOutputFileName, 
							"ownerId", title2_numWatchers, 4, "userId", numberOfWatchersFieldName, SortOrder.ASCENDING_INTEGER, 
							true, indentationLevel+2, testOrReal, showProgressInterval, writeMessageStep+"4-" + numberOfLanguagesConsidered + "-3");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					//Providing file names and field names for joining them later:
					fileNamesForInfluenceMetrics_watchers.add(watchersOutputFileName);
					keysToJoin_watchers.add("userId");
					neededFields_watchers.add(numberOfWatchersFieldName);
					
					//Deleting temporary files:
					MyUtils.deleteTemporaryFiles(outputPath, new String[]{temp_project1_watchers, temp_project1_idReplacedByNumberOfWatchers}, true, 
							indentationLevel+2, writeMessageStep+"4-" + numberOfLanguagesConsidered + "-4");
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////					
					//B- Forks (forks_1 to forks_10; for top 10 languages):
					//First, count the number of forks from each project (and save it in "temp-projects2-Forks_i_LANGUAGE.tsv"):
					String temp_project2_forks = "temp-projects2-forks_"+numberOfLanguagesConsidered+"_"+language+".tsv",
							title3_projectId = "projectId", title4_numForks = "numberOfForks";
					fMR = TSVManipulations.runGroupBy_count_andSaveResultToTSV(inputPathForJustNumericFields, "projects.tsv", outputPath, temp_project2_forks, "forkedFrom", 4, 
							title3_projectId, title4_numForks, SortOrder.ASCENDING_INTEGER, 
							true, indentationLevel+2, testOrReal, showProgressInterval, writeMessageStep+"4-" + numberOfLanguagesConsidered + "-5");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					
					//Then, using this number of forks, convert <projectId, userId> in "projects.tsv" to <#OfForks, userId> and save it in "temp_project2_forks_i_LANGUAGE_idReplacedByNumberOfForks.tsv":
					String temp_project2_idReplacedByNumberOfForks = "temp-projects2-forks_"+numberOfLanguagesConsidered+"_"+language+"-idReplacedByNumberOfForks.tsv";
					fMR = TSVManipulations.replaceForeignKeyInTSVWithValueFromAnotherTSV(outputPath+"\\"+filteredProjectsFileName, outputPath+"\\"+temp_project2_forks, 
							outputPath+"\\"+temp_project2_idReplacedByNumberOfForks, "id", 4, title3_projectId, 2, title4_numForks, title4_numForks, 
							true, showProgressInterval, indentationLevel+2, testOrReal, writeMessageStep+"4-" + numberOfLanguagesConsidered + "-6");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					
					//Now, using this #ofProjectsForkedFromThis, get sum(#ofProjectsForkedFromThis) for each "userId" and save it as "usersAndNumberOfForksFromTheirProjects.tsv":
					String numberOfForksFieldName = "numberOfProjectsForkedFromProjectsOfThisUser_"+numberOfLanguagesConsidered+"_"+language;
					String forksOutputFileName = "usersAndNumberOfForksFromTheirProjects_"+numberOfLanguagesConsidered+"_"+language+".tsv";
					fMR = TSVManipulations.runGroupBy_sum_andSaveResultToTSV(outputPath, temp_project2_idReplacedByNumberOfForks, outputPath, forksOutputFileName, 
							"ownerId", title4_numForks, 4, "userId", numberOfForksFieldName, SortOrder.ASCENDING_INTEGER, 
							true, indentationLevel+2, testOrReal, showProgressInterval, writeMessageStep+"4-" + numberOfLanguagesConsidered + "-7");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					//Providing file names and field names for joining them later:
					fileNamesForInfluenceMetrics_forks.add(forksOutputFileName);
					keysToJoin_forks.add("userId");
					neededFields_forks.add(numberOfForksFieldName);
					
					//Deleting temporary files:
					MyUtils.deleteTemporaryFiles(outputPath, new String[]{temp_project2_forks, temp_project2_idReplacedByNumberOfForks}, true,  
							indentationLevel+2, writeMessageStep+"4-" + numberOfLanguagesConsidered + "-8");
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////					
					if (numberOfLanguagesConsidered >= Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY)
						break;
				}
			}//if (!entry....
			if (numberOfLanguagesConsidered >= Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY)
				break;
		}//for(Map.Entry....

		//5- Joining userId from "users.tsv" with all files related to top (10) languages (several files for languageBased#ofWatchers and languageBased#ofForks):
		MyUtils.println(writeMessageStep+"5- Joining all output files:", indentationLevel);
		//Copying "users.tsv" to outputPath, since we are joining it with several other files (which are generated in this method), and all the input files for the join process should be in one folder: 
		fMR = MyUtils.copyFile(inputPath, "users.tsv", outputPath, "users.tsv", indentationLevel+1, writeMessageStep+"5-1");
		totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
		//Joining:
		//Watchers:
		TSVManipulations.joinSeveralTSVs(outputPath, fileNamesForInfluenceMetrics_watchers, 
				"Out_watchers.tsv", keysToJoin_watchers, JoinType.FULL_JOIN, neededFields_watchers, 
				SortOrder.ASCENDING_INTEGER, "0", true, indentationLevel+1, testOrReal, showProgressInterval, writeMessageStep+"5-2");
		//Forks:
		TSVManipulations.joinSeveralTSVs(outputPath, fileNamesForInfluenceMetrics_forks, 
				"Out_forks.tsv", keysToJoin_forks, JoinType.FULL_JOIN, neededFields_forks, 
				SortOrder.ASCENDING_INTEGER, "0", true, indentationLevel+1, testOrReal, showProgressInterval, writeMessageStep+"5-2");
		//Deleting temporary "users.tsv" file:
		MyUtils.deleteTemporaryFiles(outputPath, new String[]{"users.tsv"}, true, indentationLevel+1, writeMessageStep+"5-3");
		
		//Summary:
		System.out.println("-----------------------------------");
		System.out.println("Summary: ");
		System.out.println(MyUtils.indent(indentationLevel+1) + totalFMR.processed + " files processed in total.");
		System.out.println(MyUtils.indent(indentationLevel+1) + totalFMR.doneSuccessfully + " files processed successfully.");
		if (totalFMR.errors == 0){
			System.out.println(MyUtils.indent(indentationLevel+1) + "Done successfully!");
		}
		else
			System.out.println(MyUtils.indent(indentationLevel+1) + totalFMR.errors + " errors!");
		Date d2 = new Date();
		System.out.println(MyUtils.indent(indentationLevel+1) + "Total time: " + (float)(d2.getTime()-d1.getTime())/1000  + " seconds.");
		System.out.println("-----------------------------------");
		System.out.println("-----------------------------------");
		System.out.println("-----------------------------------");
	}//countLanguages().
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static void main(String[] args) {
		// TODO Auto-generated method stub

//		countLanguages(Constants.DATASET_DIRECTORY_GH_TSV, Constants.DATASET_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS, Constants.DATASET_DIRECTORY_GH_TSV__LANGUAGE_STUDY, 
//		1, Constants.THIS_IS_REAL, 500000, "1");

		measure_languageBased_WatchAndForkInfluenceMetrics(Constants.DATASET_EXTERNAL_DIRECTORY_GH_TSV, Constants.DATASET_EXTERNAL_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS, Constants.DATASET_EXTERNAL_DIRECTORY_GH_TSV__LANGUAGE_STUDY, 
				1, Constants.THIS_IS_REAL, 5000000, "");
	
	}

}
