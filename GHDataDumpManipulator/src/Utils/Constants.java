package Utils;


import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Date;
//import java.util.HashSet;
import java.util.List;
import java.util.Map;
//import java.util.Random;


import com.google.common.collect.ImmutableMap;
//import com.google.gwt.thirdparty.guava.common.collect.ImmutableMap;

public class Constants { 
	//GH Dataset:
    //SQL files:
	public static final String DATASET_DIRECTORY_GH_MySQL = "C:\\2-Study\\Influentials\\Data Set\\MySQL-20150401\\SQL";
    public static final String DATASET_EXTERNAL_DIRECTORY_GH_MySQL = "E:\\Influentials\\DataSet\\MySQL-20150401\\SQL";
    //TSV files:
    public static final String DATASET_DIRECTORY_GH_TSV = "C:\\2-Study\\Influentials\\Data Set\\MySQL-20150401\\TSV";
    public static final String DATASET_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS = "C:\\2-Study\\Influentials\\Data Set\\MySQL-20150401\\TSV\\JustNumericFields";
    public static final String DATASET_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS__Aggregated = "C:\\2-Study\\Influentials\\Data Set\\MySQL-20150401\\TSV\\JustNumericFields\\Aggregated";
    public static final String DATASET_DIRECTORY_GH_TSV__LANGUAGE_STUDY = "C:\\2-Study\\Influentials\\Data Set\\MySQL-20150401\\TSV\\LanguagesStudy";

    public static final String DATASET_EXTERNAL_DIRECTORY_GH_TSV = "E:\\Influentials\\DataSet\\MySQL-20150401\\TSV";
    public static final String DATASET_EXTERNAL_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS = "E:\\Influentials\\DataSet\\MySQL-20150401\\TSV\\JustNumericFields";
    public static final String DATASET_EXTERNAL_DIRECTORY_GH_TSV__JUST_NUMERIC_FIELDS__AGGREGATED = "E:\\Influentials\\DataSet\\MySQL-20150401\\TSV\\JustNumericFields\\Aggregated";
    public static final String DATASET_EXTERNAL_DIRECTORY_GH_TSV__LANGUAGE_STUDY = "E:\\Influentials\\DataSet\\MySQL-20150401\\TSV\\LanguagesStudy";
  
  //JustNumberFields (No text field, no dateTime field):
    public static final Map<String, List<String>> DUMP_TABLES_AND_THEIR_FIELDS = ImmutableMap.<String, List<String>> builder() 
	        .put("commit_comments:neededFields", Arrays.asList(new String[] { "id", "commit_id", "user_id", "line", "position", "comment_id" }))
	        .put("commit_comments:titlesForNeededFields", Arrays.asList(new String[] { "id", "commitId", "userId", "line", "position", "commentId" }))
	        .put("commit_comments:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { "body" }))
	        
//"commit_parents" is not needed:
//	        .put("commit_parents:neededFields", Arrays.asList(new String[] { "commit_id", "parent_id" }))
//	        .put("commit_parents:titlesForNeededFields", Arrays.asList(new String[] { "commit_id", "parent_id" }))
//	        .put("commit_parents:FieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("commits:neededFields", Arrays.asList(new String[] { "id", "author_id", "committer_id", "project_id" }))
	        .put("commits:titlesForNeededFields", Arrays.asList(new String[] { "id", "authorId", "committerId", "projectId" }))
	        .put("commits:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("followers:neededFields", Arrays.asList(new String[] { "follower_id", "user_id" }))
	        .put("followers:titlesForNeededFields", Arrays.asList(new String[] { "followerId", "userId" }))
	        .put("followers:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//Probably "issue_coments" is not needed:	        
	        .put("issue_comments:neededFields", Arrays.asList(new String[] { "issue_id", "user_id", "comment_id" }))
	        .put("issue_comments:titlesForNeededFields", Arrays.asList(new String[] { "issueId", "userId", "commentId" }))
	        .put("issue_comments:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//Probably "issue_events" is not needed:	        
	        .put("issue_events:neededFields", Arrays.asList(new String[] { "event_id", "issue_id", "actor_id" }))
	        .put("issue_events:titlesForNeededFields", Arrays.asList(new String[] { "eventId", "issueId", "actorId" }))
	        .put("issue_events:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//"issue_labels" is not needed:	        
//	        .put("issue_labels:neededFields", Arrays.asList(new String[] { "label_id", "issue_id" }))
//	        .put("issue_labels:titlesForNeededFields", Arrays.asList(new String[] { "labelId", "issueId" }))
//	        .put("issue_labels:FieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//Probably "issues" are not needed:	        
	        .put("issues:neededFields", Arrays.asList(new String[] { "id", "repo_id", "reporter_id", "assignee_id" }))
	        .put("issues:titlesForNeededFields", Arrays.asList(new String[] { "id", "repoId", "reporterId", "assigneeId" }))
	        .put("issues:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("organization_members:neededFields", Arrays.asList(new String[] { "org_id", "user_id" }))
	        .put("organization_members:titlesForNeededFields", Arrays.asList(new String[] { "orgId", "userId" }))
	        .put("organization_members:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("project_commits:neededFields", Arrays.asList(new String[] { "project_id", "commit_id" }))
	        .put("project_commits:titlesForNeededFields", Arrays.asList(new String[] { "projectId", "commitId" }))
	        .put("project_commits:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("project_members:neededFields", Arrays.asList(new String[] { "repo_id", "user_id" }))
	        .put("project_members:titlesForNeededFields", Arrays.asList(new String[] { "repoId", "userId" }))
	        .put("project_members:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))

	        .put("projects:neededFields", Arrays.asList(new String[] { "id", "owner_id", "forked_from", "deleted" }))
	        .put("projects:titlesForNeededFields", Arrays.asList(new String[] { "id", "ownerId", "forkedFrom", "deleted" }))
	        .put("projects:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("projects_with_forks:neededFields", Arrays.asList(new String[] { "id" }))
	        .put("projects_with_forks:titlesForNeededFields", Arrays.asList(new String[] { "id" }))
	        .put("projects_with_forks:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("pull_request_comments:neededFields", Arrays.asList(new String[] { "pull_request_id", "user_id", "comment_id", "position", "body", "commit_id" }))
	        .put("pull_request_comments:titlesForNeededFields", Arrays.asList(new String[] { "pullRequestId", "userId", "commentId", "position", "body", "commitId" }))
	        .put("pull_request_comments:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { "body" }))
	        
	        .put("pull_request_commits:neededFields", Arrays.asList(new String[] { "pull_request_id", "commit_id" }))
	        .put("pull_request_commits:titlesForNeededFields", Arrays.asList(new String[] { "pullRequestId", "commitId" }))
	        .put("pull_request_commits:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("pull_request_history:neededFields", Arrays.asList(new String[] { "id", "pull_request_id", "actor_id" }))
	        .put("pull_request_history:titlesForNeededFields", Arrays.asList(new String[] { "id", "pullRequestId", "actorId" }))
	        .put("pull_request_history:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("pull_requests:neededFields", Arrays.asList(new String[] { "id", "head_repo_id", "base_repo_id", "head_commit_id", "base_commit_id", "pullreq_id", "intra_branch" }))
	        .put("pull_requests:titlesForNeededFields", Arrays.asList(new String[] { "id", "headRepoId", "baseRepoId", "headCommitId", "baseCommitId", "pullreqId", "intraBranch" }))
	        .put("pull_requests:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//"repo_labels" is not needed:	        
//	        .put("repo_labels:neededFields", Arrays.asList(new String[] { "id", "repo_id" }))
//	        .put("repo_labels:titlesForNeededFields", Arrays.asList(new String[] { "id", "repoId" }))
//	        .put("repo_labels:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//"repo_milestones" is empty:	        
//	        .put("repo_milestones:neededFields", Arrays.asList(new String[] { "id", "repo_id" }))
//	        .put("repo_milestones:titlesForNeededFields", Arrays.asList(new String[] { "id", "repoId" }))
//	        .put("repo_milestones:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//"schema_info" is empty:	        
//	        .put("schema_info:neededFields", Arrays.asList(new String[] { "version" }))
//	        .put("schema_info:titlesForNeededFields", Arrays.asList(new String[] { "version" }))
//	        .put("schema_info:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("users:neededFields", Arrays.asList(new String[] { "id" }))
	        .put("users:titlesForNeededFields", Arrays.asList(new String[] { "id" }))
	        .put("users:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] {  }))
	        
	        .put("watchers:neededFields", Arrays.asList(new String[] { "repo_id", "user_id" }))
	        .put("watchers:titlesForNeededFields", Arrays.asList(new String[] { "repoId", "userId" }))
	        .put("watchers:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//	        .put(":neededFields", Arrays.asList(new String[] { "", "", "", "" }))
//	        .put(":titlesForNeededFields", Arrays.asList(new String[] { "", "", "", "" }))
//	        .put(":fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .build();


/*
//AllFields (includingTextualFields):
    public static final  Map<String, List<String>> DUMP_TABLES_AND_THEIR_FIELDS = ImmutableMap.<String, List<String>> builder() 
	        .put("commit_comments:neededFields", Arrays.asList(new String[] { "id", "commit_id", "user_id", "body", "line", "position", "comment_id" }))
	        .put("commit_comments:titlesForNeededFields", Arrays.asList(new String[] { "id", "commitIdd", "userId", "body", "line", "position", "commentId" }))
	        .put("commit_comments:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { "body" }))
	        
//"commit_parents" is not needed:
//	        .put("commit_parents:neededFields", Arrays.asList(new String[] { "commit_id", "parent_id" }))
//	        .put("commit_parents:titlesForNeededFields", Arrays.asList(new String[] { "commit_id", "parent_id" }))
//	        .put("commit_parents:FieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("commits:neededFields", Arrays.asList(new String[] { "id", "author_id", "committer_id", "project_id" }))
	        .put("commits:titlesForNeededFields", Arrays.asList(new String[] { "id", "authorId", "committerId", "projectId" }))
	        .put("commits:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("followers:neededFields", Arrays.asList(new String[] { "follower_id", "user_id", "created_at" }))
	        .put("followers:titlesForNeededFields", Arrays.asList(new String[] { "followerId", "userId", "createdAt" }))
	        .put("followers:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//Probably "issue_coments" is not needed:	        
	        .put("issue_comments:neededFields", Arrays.asList(new String[] { "issue_id", "user_id", "comment_id", "created_at" }))
	        .put("issue_comments:titlesForNeededFields", Arrays.asList(new String[] { "issueId", "userId", "commentId", "createdAt" }))
	        .put("issue_comments:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//Probably "issue_events" is not needed:	        
	        .put("issue_events:neededFields", Arrays.asList(new String[] { "event_id", "issue_id", "actor_id", "action", "created_at" }))
	        .put("issue_events:titlesForNeededFields", Arrays.asList(new String[] { "eventId", "issueId", "actorId", "action", "createdAt" }))
	        .put("issue_events:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//"issue_labels" is not needed:	        
//	        .put("issue_labels:neededFields", Arrays.asList(new String[] { "label_id", "issue_id" }))
//	        .put("issue_labels:titlesForNeededFields", Arrays.asList(new String[] { "labelId", "issueId" }))
//	        .put("issue_labels:FieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//Probably "issues" are not needed:	        
	        .put("issues:neededFields", Arrays.asList(new String[] { "id", "repo_id", "reporter_id", "assignee_id", "created_at" }))
	        .put("issues:titlesForNeededFields", Arrays.asList(new String[] { "id", "repoId", "reporterId", "assigneeId", "createdAt" }))
	        .put("issues:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("organization_members:neededFields", Arrays.asList(new String[] { "org_id", "user_id", "created_at" }))
	        .put("organization_members:titlesForNeededFields", Arrays.asList(new String[] { "orgId", "userId", "createdAt" }))
	        .put("organization_members:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("project_commits:neededFields", Arrays.asList(new String[] { "project_id", "commit_id" }))
	        .put("project_commits:titlesForNeededFields", Arrays.asList(new String[] { "projectId", "commitId" }))
	        .put("project_commits:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("project_members:neededFields", Arrays.asList(new String[] { "repo_id", "user_id", "created_at" }))
	        .put("project_members:titlesForNeededFields", Arrays.asList(new String[] { "repoId", "userId", "createdAt" }))
	        .put("project_members:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))

	        .put("projects:neededFields", Arrays.asList(new String[] { "id", "url", "owner_id", "name", "description", "language", "created_at", "forked_from", "deleted" }))
	        .put("projects:titlesForNeededFields", Arrays.asList(new String[] { "id", "url", "owner_id", "name", "description", "language", "createdAt", "forkedFrom", "deleted" }))
	        .put("projects:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("projects_with_forks:neededFields", Arrays.asList(new String[] { "id" }))
	        .put("projects_with_forks:titlesForNeededFields", Arrays.asList(new String[] { "id" }))
	        .put("projects_with_forks:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("pull_request_comments:neededFields", Arrays.asList(new String[] { "pull_request_id", "user_id", "comment_id", "position", "body", "commit_id", "created_at" }))
	        .put("pull_request_comments:titlesForNeededFields", Arrays.asList(new String[] { "pullRequestId", "userId", "commentId", "position", "body", "commitId", "createdAt" }))
	        .put("pull_request_comments:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { "body" }))
	        
	        .put("pull_request_commits:neededFields", Arrays.asList(new String[] { "pull_request_id", "commit_id" }))
	        .put("pull_request_commits:titlesForNeededFields", Arrays.asList(new String[] { "pull_requestId", "commitId" }))
	        .put("pull_request_commits:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("pull_request_history:neededFields", Arrays.asList(new String[] { "id", "pull_request_id", "created_at", "action", "actor_id" }))
	        .put("pull_request_history:titlesForNeededFields", Arrays.asList(new String[] { "id", "pullRequestId", "createdAt", "action", "actorId" }))
	        .put("pull_request_history:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("pull_requests:neededFields", Arrays.asList(new String[] { "id", "head_repo_id", "base_repo_id", "head_commit_id", "base_commit_id", "pullreq_id", "intra_branch" }))
	        .put("pull_requests:titlesForNeededFields", Arrays.asList(new String[] { "id", "headRepoId", "baseRepoId", "headCommitId", "baseCommitId", "pullreqId", "intraBranch" }))
	        .put("pull_requests:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//"repo_labels" is not needed:	        
//	        .put("repo_labels:neededFields", Arrays.asList(new String[] { "id", "repo_id", "name", "ext_ref_id" }))
//	        .put("repo_labels:titlesForNeededFields", Arrays.asList(new String[] { "id", "repoId", "name", "extRefId" }))
//	        .put("repo_labels:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//"repo_milestones" is empty:	        
//	        .put("repo_milestones:neededFields", Arrays.asList(new String[] { "id", "repo_id", "name", "ext_ref_id" }))
//	        .put("repo_milestones:titlesForNeededFields", Arrays.asList(new String[] { "id", "repoId", "name", "extRefId" }))
//	        .put("repo_milestones:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//"schema_info" is empty:	        
//	        .put("schema_info:neededFields", Arrays.asList(new String[] { "version" }))
//	        .put("schema_info:titlesForNeededFields", Arrays.asList(new String[] { "version" }))
//	        .put("schema_info:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .put("users:neededFields", Arrays.asList(new String[] { "id", "login", "name", "company", "location", "email", "created_at", "type", "fake", "deleted" }))
	        .put("users:titlesForNeededFields", Arrays.asList(new String[] { "id", "login", "name", "company", "location", "email", "createdAt", "type", "fake", "deleted" }))
	        .put("users:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { "email" }))
	        
	        .put("watchers:neededFields", Arrays.asList(new String[] { "repo_id", "user_id", "created_at" }))
	        .put("watchers:titlesForNeededFields", Arrays.asList(new String[] { "repoId", "userId", "createdAt" }))
	        .put("watchers:fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
//	        .put(":neededFields", Arrays.asList(new String[] { "", "", "", "" }))
//	        .put(":titlesForNeededFields", Arrays.asList(new String[] { "", "", "", "" }))
//	        .put(":fieldsToRemoveInvalidCharacters", Arrays.asList(new String[] { }))
	        
	        .build();
*/

    public static final String allValidCharactersInGH_Descriptions_ForRegEx = "a-zA-Z0-9\\.\\#\\+\\-\\_\\@\\(\\)\\[\\]\\{\\}\\*\\!\\,\\:\\;";  
	public static final String SEPARATOR_FOR_TABLE_AND_FIELD = ":";
	public static final String SEPARATOR_FOR_FIELDS_IN_TSV_FILE = "\t";
	public static final DecimalFormat integerFormatter = new DecimalFormat("###,###");
	public static final DecimalFormat floatFormatter = new DecimalFormat("###,###.#");
	public static final DecimalFormat highPrecisionFloatFormatter = new DecimalFormat("###,###.######");
	public static final int NUMBER_OF_TAB_CHARACTERS = 4;
	public static final int NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY = 10;
//	public static final int NUMBER_OF_LANGUAGES_TO_CONSIDER_IN_LANGUAGES_STUDY = 1;

	public static final String ALL = "ALL";
	
	public static final long THIS_IS_A_TEST = 10000;
	public static final long THIS_IS_REAL = -1;//unlimited!
	public static final int ERROR = -1;
	public static final long AN_EXTREMELY_NEGATIVE_LONG = Long.MIN_VALUE; 
	public static final long AN_EXTREMELY_POSITIVE_LONG = Long.MAX_VALUE; 
	//Sort order:
	public enum SortOrder{ 
		ASCENDING_INTEGER, DESCENDING_INTEGER, DEFAULT_FOR_STRING
	}

	public enum FieldType{
		LONG, STRING, NOT_IMPORTANT
	}

	public enum LogicalOperand { 
		NO_CONDITION, AND, OR, IGNORE_THE_SECOND_OPERAND
	}

	public enum ConditionType {
		EQUALS, NOT_EQUALS, NOTHING, GREATER_OR_EQUAL
	}
	
	public enum JoinType{
		INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN
	}
	
	public static void main(String[] args) {
		String s = "abcdeababcdecdejhgabcde";
		System.out.println(s);
		s = s.replaceAll("abcde", "");
		s = s.replaceAll("abcde", "");
		System.out.println(s);
//		s = s.replaceAll("\\\\'", "  ");
//		System.out.println(s);
////		System.out.println("asfadf".substring(0, "asfadf".indexOf(",")));
//		System.out.println("---------------- Constants.java: ----------------");
//		String s1 = "2013-05-25T21:50:02Z";
//		String s2 = "2011-07-25T21:50:02Z";
//		if (s1.compareTo(s2) > 0) //if s1 > s2 ==> returns positive value.
//			System.out.println("1111");
//		else
//			System.out.println("2222");
		
		Date d1 = new Date();
		s= "";
		for (int i=0; i<7000; i++)
			s = s + "a";
		Date d2 = new Date();
		System.out.println((d2.getTime()-d1.getTime())/1000);

		double a = 0.0000004;
		System.out.println(highPrecisionFloatFormatter.format(0.0000004));
		System.out.println(Constants.highPrecisionFloatFormatter.format(a));
		
		
	
	}

}
//Iterating:
//	for (Map.Entry<String, List<String>> entry : Constants.USEFUL_FIELDS_IN_JSON_FILES.entrySet())
//		System.out.println(entry.getKey() + "    ------>    " + entry.getValue());

