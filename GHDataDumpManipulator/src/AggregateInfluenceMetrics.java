import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;

import Utils.Constants;
import Utils.Constants.ConditionType;
import Utils.Constants.FieldType;
import Utils.Constants.LogicalOperand;
import Utils.Constants.SortOrder;
import Utils.FileConversionResult;
import Utils.MyUtils;
import Utils.TSVManipulations;

public class AggregateInfluenceMetrics {
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	private static FileConversionResult runGroupBy_count_andSaveResultToTSV(String inputPath, String inputTSVFileName, String outputPath, String outputTSVFileName,  
			String groupByField, int totalFieldsCount, String titleOfGroupByFieldInOutputFile, String titleOfCountedFieldForOutputFile, SortOrder sortOrder, 
			int indentationLevel, long testOrReal, int showProgressInterval, String writeMessageStep) {
		System.out.println("-----------------------------------");
		System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Groupping by \"" + groupByField + "\" (in \"" + inputTSVFileName + "\"):");
		System.out.println(MyUtils.indent(indentationLevel+1) + "Started ...");
		FileConversionResult result = new FileConversionResult();	

		FileConversionResult[] fCRArray = new FileConversionResult[1]; 
		fCRArray[0] = new FileConversionResult();//: this variable is for making a call-by-reference variable.
//		System.out.println(fCRArray.length);
		TreeMap<String, Long> usersAndTheirFollowers = TSVManipulations.groupBy_count_fromTSV(
			inputPath+"\\"+inputTSVFileName, null, groupByField, sortOrder , totalFieldsCount, fCRArray, 
			showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-1");
		result = MyUtils.addFileConversionResults(result, fCRArray[0]);
		
		String[] titles = new String[]{titleOfGroupByFieldInOutputFile, titleOfCountedFieldForOutputFile};
		fCRArray[0] = TSVManipulations.saveKeyAndValuesAsTSVFile(outputPath+"\\"+outputTSVFileName, usersAndTheirFollowers,  
				totalFieldsCount, titles, showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-2");
		result = MyUtils.addFileConversionResults(result, fCRArray[0]);
		
		System.out.println(MyUtils.indent(indentationLevel+1) + "Finished.");
		System.out.println("-----------------------------------");
		result.processed = 1;
		if (result.errors == 0)
			result.converted = 1;
		return result;
	}//convertAllFilseInFolderToTSV().
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	private static FileConversionResult runGroupBy_sum_andSaveResultToTSV(String inputPath, String inputTSVFileName, String outputPath, String outputTSVFileName,  
			String groupByField, String summingField, int totalFieldsCount, String titleOfGroupByFieldInOutputFile, String titleOfSummedFieldForOutputFile, SortOrder sortOrder, 
			int indentationLevel, long testOrReal, int showProgressInterval, String writeMessageStep) {
		System.out.println("-----------------------------------");
		System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Groupping by \"" + groupByField + "\" (in \"" + inputTSVFileName + "\"):");
		System.out.println(MyUtils.indent(indentationLevel+1) + "Started ...");
		FileConversionResult result = new FileConversionResult();	

		FileConversionResult[] fCRArray = new FileConversionResult[1]; 
		fCRArray[0] = new FileConversionResult();//: this variable is for making a call-by-reference variable.
//		System.out.println(fCRArray.length);
		TreeMap<String, Long> usersAndTheirFollowers = TSVManipulations.groupBy_sum_fromTSV(
			inputPath+"\\"+inputTSVFileName, null, groupByField, summingField, sortOrder , totalFieldsCount, fCRArray, 
			showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-1");
		result = MyUtils.addFileConversionResults(result, fCRArray[0]);
		
		String[] titles = new String[]{titleOfGroupByFieldInOutputFile, titleOfSummedFieldForOutputFile};
		fCRArray[0] = TSVManipulations.saveKeyAndValuesAsTSVFile(outputPath+"\\"+outputTSVFileName, usersAndTheirFollowers,  
				totalFieldsCount, titles, showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-2");
		result = MyUtils.addFileConversionResults(result, fCRArray[0]);
		
		System.out.println(MyUtils.indent(indentationLevel+1) + "Finished.");
		System.out.println("-----------------------------------");
		result.processed = 1;
		if (result.errors == 0)
			result.converted = 1;
		return result;
	}//groupBy_sum().
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	private static void aggregateMetrics(String inputPath, String outputPath, 
			int indentationLevel, long testOrReal, int showProgressInterval) {
		FileConversionResult totalFCR = new FileConversionResult();
		Date d1 = new Date();
		System.out.println("-----------------------------------");
		System.out.println("-----------------------------------");
		FileConversionResult fCR;
		
		//1- Followers:
		fCR = runGroupBy_count_andSaveResultToTSV(inputPath, "followers.tsv", outputPath, "followers.tsv", "userId", 2, "userId", "#OfFollowers", SortOrder.ASCENDING_INTEGER, indentationLevel, testOrReal, showProgressInterval, "1");
		totalFCR = MyUtils.addFileConversionResults(totalFCR, fCR);

		
		
		//2- Watchers (total # of watchers of all projects of each developer):
		//First, count the number of watchers for each project (and save it in "projectsWatched.tsv"):
		String projectsAndTheirNumberOfWatchersFileName = "projectsAndTheirNumberOfWatchers.tsv";
		fCR = runGroupBy_count_andSaveResultToTSV(inputPath, "watchers.tsv", outputPath, projectsAndTheirNumberOfWatchersFileName, "repoId", 2, "projectId", "numberOfWatchers", SortOrder.ASCENDING_INTEGER, indentationLevel, testOrReal, showProgressInterval, "2");
		totalFCR = MyUtils.addFileConversionResults(totalFCR, fCR);
		
		//Then, using this number of watchers, convert <projectId, userId> in "projects.tsv" to <#OfWatchers, userId>:
		String temporaryProjectsFileIncludingNumberOfWatchersInsteadOfProjectId = "projects_idReplacedByNumberOfWatchers-temporaryFile.tsv";
		TSVManipulations.replaceForeignKeyInTSVWithValueFromAnotherTSV(inputPath+"\\"+"projects.tsv", outputPath+"\\"+projectsAndTheirNumberOfWatchersFileName, 
				outputPath+"\\"+temporaryProjectsFileIncludingNumberOfWatchersInsteadOfProjectId, 0, 4, 0, 2, 1, "#ofWatchers", showProgressInterval, testOrReal, "3");
		//Now, using this #ofWatchers, get sum(numberOfWatchers) for each "userId":
		fCR = runGroupBy_sum_andSaveResultToTSV(outputPath, temporaryProjectsFileIncludingNumberOfWatchersInsteadOfProjectId, outputPath, "usersAndNumberOfUsersWatchingTheirProjects.tsv", 
				"ownerId", "#ofWatchers", 4, "userId", "sumOfNumberOfWatchersOfAllProjects", SortOrder.ASCENDING_INTEGER, 
				indentationLevel, testOrReal, showProgressInterval, "4");
		totalFCR = MyUtils.addFileConversionResults(totalFCR, fCR);
		
//		//And, read this (projectWatchers') info in memory:
//		TreeMap<String, String[]> projectsWatchers = TSVManipulations.readUniqueKeyAndItsValueFromTSV(inputPath+"\\" + projectsWatchedFileName_WithoutExtension + ".tsv", 
//				null, 0, 2, "1", LogicalOperand.NO_CONDITION, 0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
//				showProgressInterval, testOrReal, "2");
//		//Second, read the projects' owners' info:
//		TreeMap<String, ArrayList<String[]>> userIdsAndTheirOwnedProjects = TSVManipulations.readNonUniqueKeyAndItsValueFromTSV(inputPath+"\\" + "projects.tsv", 
//				null, 1, SortOrder.ASCENDING_INTEGER, 4, "0", LogicalOperand.IGNORE_THE_SECOND_OPERAND, 3, ConditionType.EQUALS, "0", FieldType.LONG, 
//				0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
//				showProgressInterval, testOrReal, "2");
		
		//3-Forks:
		fCR = runGroupBy_count_andSaveResultToTSV(inputPath, "projects.tsv", outputPath, "projectsForked.tsv", "forkedFrom", 4, "projectId", "numberProjectsForkedFromThisProject", SortOrder.ASCENDING_INTEGER, indentationLevel, testOrReal, showProgressInterval, "5");
		totalFCR = MyUtils.addFileConversionResults(totalFCR, fCR);

		//Summary:
		System.out.println("-----------------------------------");
		System.out.println(totalFCR.processed + " files processed.");
		System.out.println(totalFCR.converted + " files converted to TSV.");
		if (totalFCR.errors == 0){
			System.out.println("Done successfully!");
		}
		else
			System.out.println(totalFCR.errors + " errors!");
		Date d2 = new Date();
		System.out.println("Total time: " + (float)(d2.getTime()-d1.getTime())/1000  + " seconds.");
		System.out.println("-----------------------------------");
		System.out.println("-----------------------------------");
		System.out.println("-----------------------------------");
	}
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static void main(String[] args) {
		aggregateMetrics(Constants.DATASET_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS, Constants.DATASET_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS__Aggregated, 1, Constants.THIS_IS_REAL, 10000);
	}

}
