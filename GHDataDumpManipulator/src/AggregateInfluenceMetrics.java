import java.util.Date;
import Utils.Constants;
import Utils.Constants.JoinType;
import Utils.Constants.SortOrder;
import Utils.FileManipulationResult;
import Utils.MyUtils;
import Utils.TSVManipulations;

public class AggregateInfluenceMetrics {
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	private static void aggregateMetrics(String inputPath, String outputPath, 
			int indentationLevel, long testOrReal, int showProgressInterval) {
		Date d1 = new Date();
		System.out.println("-----------------------------------");
		System.out.println("-----------------------------------");
		FileManipulationResult totalFCR = new FileManipulationResult(), fCR;
		
		//1- Followers:
		String followersOutputFileName = "temp-usersWithAtLeastOneFollower.tsv";
		String numberOfFollowersFieldName = "#ofFollowers";
		fCR = TSVManipulations.runGroupBy_count_andSaveResultToTSV(inputPath, "followers.tsv", outputPath, followersOutputFileName, 
				"userId", 2, "userId", numberOfFollowersFieldName, SortOrder.ASCENDING_INTEGER, 
				true, indentationLevel, testOrReal, showProgressInterval, "1");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);
	
		//2- Watchers (total # of watchers of all projects of each developer):
		//First, count the number of watchers for each project (and save it in "temp-projects1-Watchers.tsv"):
		String temp_project1_watchers = "temp-projects1-Watchers.tsv", title1_projectId = "projectId", title2_numWatchers = "#ofWatchers";
		fCR = TSVManipulations.runGroupBy_count_andSaveResultToTSV(inputPath, "watchers.tsv", outputPath, temp_project1_watchers, "repoId", 2, 
				title1_projectId, title2_numWatchers, SortOrder.ASCENDING_INTEGER, 
				true, indentationLevel, testOrReal, showProgressInterval, "2");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);
		//Then, using this number of watchers, convert <projectId, userId> in "projects.tsv" to <#OfWatchers, userId> and save it in "temp-projects2-idReplacedByNumberOfWatchers.tsv":
		String temp_project2_idReplacedByNumberOfWatchers= "temp-projects2-idReplacedByNumberOfWatchers.tsv";
		fCR = TSVManipulations.replaceForeignKeyInTSVWithValueFromAnotherTSV(inputPath+"\\"+"projects.tsv", outputPath+"\\"+temp_project1_watchers, 
				outputPath+"\\"+temp_project2_idReplacedByNumberOfWatchers, "id", 4, title1_projectId, 2, title2_numWatchers, title2_numWatchers, 
				true, showProgressInterval, indentationLevel, testOrReal, "3");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);
		//Now, using this #ofWatchers, get sum(numberOfWatchers) for each "userId" and save it as "usersAndNumberOfUsersWatchingTheirProjects.tsv":
		String numberOfWatchersFieldName = "#ofWatchersOfProjectsOfThisUser";
		String watchersOutputFileName = "usersAndNumberOfUsersWatchingTheirProjects.tsv";
		fCR = TSVManipulations.runGroupBy_sum_andSaveResultToTSV(outputPath, temp_project2_idReplacedByNumberOfWatchers, outputPath, watchersOutputFileName, 
				"ownerId", title2_numWatchers, 4, "userId", numberOfWatchersFieldName, SortOrder.ASCENDING_INTEGER, 
				true, indentationLevel, testOrReal, showProgressInterval, "4");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);

		//3-Forks:
		//First, count the number of forks from each project (and save it in "temp-projects2-Forks.tsv"):
		String temp_project3_forks = "temp-projects3-Forks.tsv", title3_projectId = "projectId", title4_numForks = "#ofProjectsForkedFromThis";
		fCR = TSVManipulations.runGroupBy_count_andSaveResultToTSV(inputPath, "projects.tsv", outputPath, temp_project3_forks, "forkedFrom", 4, 
				title3_projectId, title4_numForks, SortOrder.ASCENDING_INTEGER, 
				true, indentationLevel, testOrReal, showProgressInterval, "5");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);
		//Then, using this number of forks, convert <projectId, userId> in "projects.tsv" to <#OfForks, userId> and save it in "temp-projects3-idReplacedByNumberOfForks.tsv":
		String temp_project4_idReplacedByNumberOfForks = "temp-projects3-idReplacedByNumberOfForks.tsv";
		fCR = TSVManipulations.replaceForeignKeyInTSVWithValueFromAnotherTSV(inputPath+"\\"+"projects.tsv", outputPath+"\\"+temp_project3_forks, 
				outputPath+"\\"+temp_project4_idReplacedByNumberOfForks, "id", 4, title3_projectId, 2, title4_numForks, title4_numForks, 
				true, showProgressInterval, indentationLevel, testOrReal, "6");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);
		//Now, using this #ofProjectsForkedFromThis, get sum(#ofProjectsForkedFromThis) for each "userId" and save it as "usersAndNumberOfForksFromTheirProjects.tsv":
		String forksOutputFileName = "usersAndNumberOfForksFromTheirProjects.tsv";
		String numberOfForksFieldName = "#ofProjectsForkedFromProjectsOfThisUser";
		fCR = TSVManipulations.runGroupBy_sum_andSaveResultToTSV(outputPath, temp_project4_idReplacedByNumberOfForks, outputPath, forksOutputFileName, 
				"ownerId", title4_numForks, 4, "userId", numberOfForksFieldName, SortOrder.ASCENDING_INTEGER, 
				true, indentationLevel, testOrReal, showProgressInterval, "7");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);

		//4-Merging the followers with Watchers:
		String followers_joinedWith_watchers = "temp-join_followers_watchers.tsv";
		fCR = TSVManipulations.joinTwoTSV(outputPath,followersOutputFileName, outputPath, watchersOutputFileName, outputPath, followers_joinedWith_watchers, 
				"userId", "userId", JoinType.FULL_JOIN, "#ofFollowers", numberOfWatchersFieldName,
				SortOrder.ASCENDING_INTEGER, "0", 
				true, indentationLevel, testOrReal, showProgressInterval, "8");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);

		//5-Merging the <followers and Watchers> with forks:
		String followersAndWatchers_joinedWith_forks = "followers_watchers_forks.tsv";
		fCR = TSVManipulations.joinTwoTSV(outputPath,followers_joinedWith_watchers, outputPath, forksOutputFileName, outputPath, followersAndWatchers_joinedWith_forks, 
				"userId", "userId", JoinType.FULL_JOIN, 
				numberOfFollowersFieldName+Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE+numberOfWatchersFieldName, 
				numberOfForksFieldName, SortOrder.ASCENDING_INTEGER, "0", 
				true, indentationLevel, testOrReal, showProgressInterval, "9");
		totalFCR = MyUtils.addFileManipulationResults(totalFCR, fCR);
		//#ofFollowers	#ofWatchersOfProjectsOfThisUser
		//Summary:
		System.out.println("-----------------------------------");
		System.out.println(totalFCR.processed + " files processed.");
		System.out.println(totalFCR.DoneSuccessfully + " files summaried / converted to TSV successfully.");
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
//		aggregateMetrics(Constants.DATASET_EXTERNAL_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS, Constants.DATASET_EXTERNAL_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS__AGGREGATED, 1, Constants.THIS_IS_REAL, 500000);
	}

}
