import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import Utils.Constants;
import Utils.Constants.ConditionType;
import Utils.Constants.FieldType;
import Utils.Constants.LogicalOperand;
import Utils.FileManipulationResult;
import Utils.MyUtils;
import Utils.TSVManipulations;
import Utils.Constants.SortOrder;


public class LanguagesStudy {
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static void countLanguages(String inputPath, String outputPath, 
			int indentationLevel, long testOrReal, int showProgressInterval){
		Date d1 = new Date();
		System.out.println("-----------------------------------");
		System.out.println("-----------------------------------");
		FileManipulationResult totalFMR = new FileManipulationResult(), fMR;
		
		//1- Counting languages and saving them in "languagesAndTheirNumberOfProjects.tsv":
		String numberOfLanguagesFieldName = "numberOfProjects";
		String languagesOutputFileName = "languagesAndTheirNumberOfProjects.tsv";
		fMR = TSVManipulations.runGroupBy_count_andSaveResultToTSV(inputPath, "projects.tsv", outputPath, languagesOutputFileName, 
				"language", 9, "language", numberOfLanguagesFieldName, SortOrder.DEFAULT_FOR_STRING, 
				true, indentationLevel, testOrReal, showProgressInterval, "1");
		totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
		//2- Reading the result file (of languages and number of projects of each) ordered by number of projects using each language:
		TreeMap<String, ArrayList<String[]>> numberOfProjectsAndRespectiveLanguages = TSVManipulations.readNonUniqueKeyAndItsValueFromTSV(
				outputPath+"\\"+languagesOutputFileName, 
				null, 1, SortOrder.DESCENDING_INTEGER, 2, "0", LogicalOperand.NO_CONDITION, 
				0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
				true, showProgressInterval, indentationLevel, testOrReal, "2");
		//3- Filter the projects to the top 10 projects (and saving the filtered projects in 10 different files):
		System.out.println("3- Producing different files for projects of different languages: ");
		int numberOfLanguagesConsidered = 0;
		for (Map.Entry<String, ArrayList<String[]>> entry:numberOfProjectsAndRespectiveLanguages.entrySet()){
			if (!entry.getValue().get(0)[0].equals("")){//:i.e., if this is not the number of projects without a language.
				for (int i=0; i<entry.getValue().size(); i++){//i.e., for (the first 10 = first Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY) languages with the same number of projects.
					numberOfLanguagesConsidered++;
					String language = entry.getValue().get(i)[0];
					MyUtils.println("3-" + numberOfLanguagesConsidered + "- " + language + " (mentioned " + entry.getKey() + " times)", indentationLevel+1);
					//filter projects.tsv to "language":
					TreeMap<String, ArrayList<String[]>> projectsWithASpecificLanguage = TSVManipulations.readNonUniqueKeyAndItsValueFromTSV(
							inputPath+"\\"+"projects.tsv", 
							null, 0, SortOrder.ASCENDING_INTEGER, 9, "2$7$8", LogicalOperand.IGNORE_THE_SECOND_OPERAND, 
							5, ConditionType.EQUALS, language, FieldType.NOT_IMPORTANT, 0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
							false, showProgressInterval, indentationLevel+2, testOrReal, "3-" + numberOfLanguagesConsidered + "-1");
					String filteredProjectsFileName = "projects_"+numberOfLanguagesConsidered+"_"+language + ".tsv";
					String neededFieldsInProjects = "id" + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + 
							"ownerId" + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE +  
							"forkedFrom" + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + "deleted";
					//Now, saving this list of filtered projects:
					fMR = TSVManipulations.saveTreeMapToTSVFile(outputPath+"\\"+filteredProjectsFileName, projectsWithASpecificLanguage, neededFieldsInProjects, 
							false, showProgressInterval, indentationLevel+2, testOrReal, "3-" + numberOfLanguagesConsidered + "-2");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					
					if (numberOfLanguagesConsidered >= Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY)
						break;
				}
				if (numberOfLanguagesConsidered >= Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY)
					break;
			}//if (!entry....
		}//for(Map.Entry....
		System.out.println("======================");
		//Comment ?
		//3- Get the #ofWatchers and #ofForks of all users for each top language (first 10 = first Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY):
		//Comment ?
		System.out.println("4- Counting #of_LANGUAGE_Watchers and #of_LANGUAGE_Forks for users: ");
		numberOfLanguagesConsidered = 0;
		for (Map.Entry<String, ArrayList<String[]>> entry:numberOfProjectsAndRespectiveLanguages.entrySet()){
			if (!entry.getValue().get(0)[0].equals("")){//:i.e., if this is not the number of projects without a language.
				for (int i=0; i<entry.getValue().size(); i++){//i.e., for (the first 10 = first Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY) languages with the same number of projects.
					numberOfLanguagesConsidered++;
					String language = entry.getValue().get(i)[0];
					MyUtils.println("4-" + numberOfLanguagesConsidered + "- " + language, indentationLevel+1);
					//A- Watchers (watchers_1 to watchers_10; for top 10 languages):
					//First, count the number of watchers for each project (and save it in "temp-projects1-Watchers.tsv"):
					String temp_project1_watchers = "temp-projects1-Watchers_"+numberOfLanguagesConsidered+"_"+language+".tsv", 
							title1_projectId = "projectId", title2_numWatchers = "numberOfWatchers";
					fMR = TSVManipulations.runGroupBy_count_andSaveResultToTSV(inputPath, "watchers.tsv", outputPath, temp_project1_watchers, "repoId", 2, 
							title1_projectId, title2_numWatchers, SortOrder.ASCENDING_INTEGER, 
							true, indentationLevel+2, testOrReal, showProgressInterval, "4-" + numberOfLanguagesConsidered + "-1");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					//Then, using this number of watchers, convert <projectId, userId> in 'filteredProjectsFileName' to <#OfWatchers, userId> and save it in 'temp_project2_idReplacedByNumberOfWatchers':
					String temp_project2_idReplacedByNumberOfWatchers= "temp-projects2_"+numberOfLanguagesConsidered+"_"+language+"-idReplacedByNumberOfWatchers.tsv";
					String filteredProjectsFileName = "projects_"+numberOfLanguagesConsidered+"_"+language + ".tsv";
					fMR = TSVManipulations.replaceForeignKeyInTSVWithValueFromAnotherTSV(outputPath+"\\"+filteredProjectsFileName, outputPath+"\\"+temp_project1_watchers, 
							outputPath+"\\"+temp_project2_idReplacedByNumberOfWatchers, "id", 4, title1_projectId, 2, title2_numWatchers, title2_numWatchers, 
							true, showProgressInterval, indentationLevel+2, testOrReal, "4-" + numberOfLanguagesConsidered + "-2");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					//Now, using this #ofWatchers, get sum(numberOfWatchers) for each "userId" and save it as "usersAndNumberOfUsersWatchingTheirProjects.tsv":
					String numberOfWatchersFieldName = "numberOfWatchersOfProjectsOfThisUser_"+numberOfLanguagesConsidered+"_"+language;
					String watchersOutputFileName = "usersAndNumberOfUsersWatchingTheirProjects"+numberOfLanguagesConsidered+"_"+language+".tsv";
					fMR = TSVManipulations.runGroupBy_sum_andSaveResultToTSV(outputPath, temp_project2_idReplacedByNumberOfWatchers, outputPath, watchersOutputFileName, 
							"ownerId", title2_numWatchers, 4, "userId", numberOfWatchersFieldName, SortOrder.ASCENDING_INTEGER, 
							true, indentationLevel+2, testOrReal, showProgressInterval, "4-" + numberOfLanguagesConsidered + "-3");
					totalFMR = MyUtils.addFileManipulationResults(totalFMR, fMR);
					
					MyUtils.deleteTemporaryFiles(outputPath, new String[]{temp_project1_watchers, temp_project2_idReplacedByNumberOfWatchers}, 
							indentationLevel+2, "4-" + numberOfLanguagesConsidered + "-4");
					
					
					if (numberOfLanguagesConsidered >= Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY)
						break;
				}
				if (numberOfLanguagesConsidered >= Constants.NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY)
					break;
			}//if (!entry....
		}//for(Map.Entry....
		//Summary:
		System.out.println("-----------------------------------");
		System.out.println("Summary: ");
		System.out.println(MyUtils.indent(indentationLevel+1) + totalFMR.processed + " files detected.");
		System.out.println(MyUtils.indent(indentationLevel+1) + totalFMR.DoneSuccessfully + " files summaried successfully.");
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
	//--------------------------------------------------------------------------------------)--------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		countLanguages(Constants.DATASET_DIRECTORY_GH_TSV, Constants.DATASET_DIRECTORY_GH_TSV__LANGUAGE_STUDY, 
				1, Constants.THIS_IS_REAL, 500000);

	}

}
