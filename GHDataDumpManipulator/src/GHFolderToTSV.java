import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Date;

import Utils.Constants;
import Utils.FieldAndType;
import Utils.FileManipulationResult;
import Utils.MyUtils;

//import JOptionPane.showMessageDialog;

public class GHFolderToTSV {
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	private static FileManipulationResult aggregateResults(FileManipulationResult fcr1, FileManipulationResult fcr2){
		FileManipulationResult result = new FileManipulationResult ();
		result.processed = fcr1.processed + fcr2.processed;
		result.DoneSuccessfully = fcr1.DoneSuccessfully + fcr2.DoneSuccessfully;
		result.errors = fcr1.errors + fcr2.errors;
		return result;
	}
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static int numberOfASpecificCharacterBeforeAStringBeforeEnd(String s, char c, String endingString){
		if (s == null || s.length()<3 || !s.endsWith(endingString))
			return -1; //means error; this is not what we were expecting.
		int counter = 0;
		for (int i=s.length()-endingString.length()-1; i>0; i--)
			if (s.charAt(i) == c)
				counter++;
			else
				break;
		return counter;
	}
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	private static boolean isIncompleteSubstring(String aValue, String s, int endingIndex_commaOrSemiColon){ 
		return ( aValue.startsWith("'") && 
					(
						( 
							!aValue.endsWith("'") && 
							( !aValue.endsWith("')") || 
									( (s.charAt(endingIndex_commaOrSemiColon)!=';') && (s.charAt(endingIndex_commaOrSemiColon+1)!='(') ) ) 
						) 
						|| 
						(numberOfASpecificCharacterBeforeAStringBeforeEnd(aValue, '\\', "'")%2 == 1)
						||
						(numberOfASpecificCharacterBeforeAStringBeforeEnd(aValue, '\\', "')")%2 == 1)
						||
						( aValue.length() ==1)//it means that the string is like "',abcd'"; a comma after opening quote.
					)
				);
	}
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	private static String trimCommaAndParenthesisAndConvertNullToEmptyString(String aValue){
		if (aValue.startsWith("'") && aValue.endsWith("'"))
			aValue = aValue.substring(1, aValue.length()-1);
		else
			if (aValue.startsWith("'") && aValue.endsWith("')"))
				aValue = aValue.substring(1, aValue.length()-2);
			else
				if (aValue.endsWith(")")) //for the numbers followed by parenthesis.
					aValue = aValue.substring(0, aValue.length()-1);
				else
					if (aValue.equals("NULL"))
						aValue = "";
		return aValue;
	}
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	private static FileManipulationResult convertToTSV(File inputFile, String outputPath, int fileCounter, int indentationLevel, 
			int showProgressInterval){
		FileManipulationResult result = new FileManipulationResult();
		String outputPathAndFileName = outputPath + "\\" + inputFile.getName();
		outputPathAndFileName = MyUtils.removeFromEnd(outputPathAndFileName, 3) + "tsv";
		String tableName = inputFile.getName().substring(0, inputFile.getName().indexOf(".sql"));
		System.out.println(fileCounter + ") \"" + tableName + "\"; " + inputFile.getPath() + "\"\t ----> \"" + outputPathAndFileName + "\"");
		String tabSeparatedRecord = "", s = ""; 
		int startingIndex=0, endingIndex = 0;
		long recordsRead = 0;
		int test = 0;
		try{
			BufferedReader br = new BufferedReader(new FileReader(inputFile.getPath())); 
			System.out.println(MyUtils.indent(indentationLevel) + fileCounter + "-1- Started ...");
			//Reading field names and their types:
			String createTableLine = MyUtils.moveTheBufferedReaderCursorToTheLineAfter(br, "CREATE TABLE `");
			//Fixing tableName (just in case the fileName is different from the one in the "Create Table" command):
			tableName = createTableLine.substring("CREATE TABLE `".length(), createTableLine.indexOf("`", "CREATE TABLE `".length()+1));
			if (Constants.DUMP_TABLES_AND_THEIR_FIELDS.containsKey(tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"neededFields")){//:To proceed only if we need the table.
				FileWriter writer = new FileWriter(outputPathAndFileName);
				ArrayList<FieldAndType> fields = new ArrayList<FieldAndType>();																test = 1;
				while( ((s = br.readLine()) != null) && (s.startsWith("  `")) ){
					FieldAndType field = new FieldAndType();
					field.name = s.substring(3, s.indexOf("`", 3));
					field.type = s.substring(s.indexOf("`", 3)+2, s.length());
					if (field.type.startsWith("int") || (field.type.startsWith("tinyint")))
						field.type = "int";
					else
						if (field.type.startsWith("varchar") || (field.type.startsWith("timestamp")))
							field.type = "varchar";
					fields.add(field);
				} /*while.*/																												test = 2;
				//Writing the field names in the TSV file:
				String header = "";
				int i;
				int numberOfFieldTitlesHaveBeenRead = 0;
				for (i=0; i<fields.size(); i++)
					if (Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"neededFields").contains(fields.get(i).name)){
						int indexOfALabel = Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(
								tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"neededFields").indexOf(fields.get(i).name);
						header = header + Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(
								tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"titlesForNeededFields").get(indexOfALabel);
						numberOfFieldTitlesHaveBeenRead++;
						if (numberOfFieldTitlesHaveBeenRead < Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"titlesForNeededFields").size())
							header = header + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE;
					}
				if (numberOfFieldTitlesHaveBeenRead != Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"titlesForNeededFields").size()){
					System.out.println("error 1!!!!!!!!!!!!!!!!!! (in number of titles)");			result.errors++;			}//if (numb....
				header = header + "\n";
				writer.append(header);																										test = 3;
				//Reading "insert into" lines and writing the values to the TSV:
				MyUtils.moveTheBufferedReaderCursorToTheLineAfter(br, "LOCK TABLES `");
				br.readLine(); //Ignoring the line containing: "/*!40000 ALTER TABLE `users` DISABLE KEYS */;"
				while( ((s = br.readLine()) != null) && (s.startsWith("INSERT INTO `")) ){
					if (!s.endsWith(");")){
						System.out.println("error 2!!!!!!!!!!!!!!!!!! (in end of line)");			result.errors++;			}//if (!s....
					startingIndex = ("INSERT INTO `" + tableName + "` VALUES (").length();													test = 4;
					while (startingIndex < s.length()-1){
						tabSeparatedRecord = "";																							test = 5;
						int numberOfFieldsHaveBeenReadInThisRecord = 0;
						for (i=0; i<fields.size(); i++){																					test = 6;
							endingIndex = MyUtils.indexOf_ifExists_LengthIfDoNotExist(s, ",", startingIndex);
							String aValue = s.substring(startingIndex, endingIndex);														test = 7;
							while (isIncompleteSubstring(aValue, s, endingIndex)) { /*: it means if the string is incompletely identified and stopped by a "," in the middle.*/ 				test = 8; 								
								endingIndex = MyUtils.indexOf_ifExists_LengthIfDoNotExist(s, ",", endingIndex+1);							test = 9;
								aValue = s.substring(startingIndex, endingIndex);															test = 10;
							}//while.
							if (aValue.length()<1){
								System.out.println("error 3!!!!!!!!!!!!!!!!!! (in value lengths)");			result.errors++;			}/*if (aVal....*/										test = 11;
							//Trimming the "'" from the two sides (or "'" and "')" from beginning and end), and converting NULL to empty string:
							aValue = trimCommaAndParenthesisAndConvertNullToEmptyString(aValue);											test = 12;
							if (Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"neededFields").contains(fields.get(i).name)){
								aValue = aValue.replaceAll(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE, " ");
								if (Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"fieldsToRemoveInvalidCharacters").contains(fields.get(i).name))
									aValue = MyUtils.applyRegexOnString(Constants.allValidCharactersInGH_Descriptions_ForRegEx, aValue);
								tabSeparatedRecord = tabSeparatedRecord + aValue;
								numberOfFieldsHaveBeenReadInThisRecord++;
								if (numberOfFieldsHaveBeenReadInThisRecord < Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"neededFields").size())
									tabSeparatedRecord = tabSeparatedRecord + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE;
							}//if (Constants.DUMP_TABLES_AND_THEIR_FIELDS....
							startingIndex = endingIndex+1;
						}/*for.*/																											test = 13;
						if (numberOfFieldsHaveBeenReadInThisRecord != Constants.DUMP_TABLES_AND_THEIR_FIELDS.get(tableName+Constants.SEPARATOR_FOR_TABLE_AND_FIELD+"neededFields").size()){
							System.out.println("error 4!!!!!!!!!!!!!!!!!! (in number of fields)");			result.errors++;			}/*if (numb....*/
						tabSeparatedRecord = MyUtils.removeExtraCharactersFromTheEndOfRecord(tabSeparatedRecord) + "\n";					test = 14;
						writer.append(tabSeparatedRecord);
						recordsRead++;
						if (recordsRead % showProgressInterval == 0)
							System.out.println(MyUtils.indent(indentationLevel+1) +  Constants.integerFormatter.format(recordsRead));
						startingIndex++; /*Ignore the opening parenthesis.*/																test = 15;
					}/*while.*/																												test = 16;
				} /*while.*/																												test = 17;
				writer.flush(); writer.close();
				result.DoneSuccessfully++;
			}//if (Constants.DUMP_TABLES_AND_THEIR_FIELDS....
			System.out.println(MyUtils.indent(indentationLevel) + fileCounter + "-1- Finished.");
			result.processed++;
			return result;
		}catch(Exception e){
			e.printStackTrace();										System.out.println("error 5!!!!!!!!!!!!!!!!!! (in catch)");
			System.out.println("<<< test = " + test + " >>>");			System.out.println(s);			System.out.println(tabSeparatedRecord);
			result.errors++;											return result;			//JOptionPane.showMessageDialog(null, "An error occured in ....");
		}
	}//convertToTSV().
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	//This method converts all files in a folder (and its sub-folders) to csv files. It automatically reads the field names from the first lines of the file. It assumes all are GH MySQL data dump format:
	private static void convertAllFilseInFolderToTSV(File inputPath, String outputPath, int showProgressInterval) {
		Date d1 = new Date();
		File[] filesList = inputPath.listFiles();
        int i = 1;
        FileManipulationResult fcr = new FileManipulationResult();
		for(File f : filesList){
        	if(f.isDirectory())
        		convertAllFilseInFolderToTSV(f, outputPath, showProgressInterval);
        	if(f.isFile()){
                fcr = aggregateResults(fcr, convertToTSV(f, outputPath, i, 2, showProgressInterval));
                System.out.println("-----------------------------------");
                i++;
            }
        }
		//Summary:
        System.out.println("-----------------------------------");
        System.out.println("-----------------------------------");
		System.out.println(fcr.processed + " files processed.");
		System.out.println(fcr.DoneSuccessfully + " files converted to TSV.");
		if (fcr.errors == 0){
			System.out.println("Done successfully!");
		}
		else
			System.out.println(fcr.errors + " errors!");
		Date d2 = new Date();
		System.out.println("Total time: " + (float)(d2.getTime()-d1.getTime())/1000  + " seconds.");
        System.out.println("-----------------------------------");
        System.out.println("-----------------------------------");
	}//convertAllFilseInFolderToTSV().
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static void main(String[] args) {
//		File path = new File(Constants.DATASET_DIRECTORY_GH_MySQL);
//		convertAllFilseInFolderToTSV(path, Constants.DATASET_DIRECTORY_GH_TSV, 10000);

		File path = new File(Constants.DATASET_EXTERNAL_DIRECTORY_GH_MySQL);
		convertAllFilseInFolderToTSV(path, Constants.DATASET_EXTERNAL_DIRECTORY_GH_TSV, 1000000);
		
//		System.out.println(numberOfASpecificCharacterBeforeAStringBeforeEnd("'dtype=[(str(\\'a\\'),\\'i\\')", '\\', "')"));
	}//main().
}

