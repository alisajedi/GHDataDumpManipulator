import java.util.Date;

import Utils.Constants;
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
		
		//2- Watchers (watchers1 to watchers10; for top 10 languages):
		
		
		
		
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
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		countLanguages(Constants.DATASET_EXTERNAL_DIRECTORY_GH_TSV, Constants.DATASET_EXTERNAL_DIRECTORY_GH_TSV__LANGUAGE_STUDY, 
				1, Constants.THIS_IS_REAL, 500000);

	}

}
