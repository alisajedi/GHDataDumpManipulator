package Utils;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import Utils.Constants.FieldType;
import Utils.Constants.JoinType;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
import Utils.Constants.LogicalOperand;
import Utils.Constants.ConditionType;
import Utils.Constants.SortOrder;

//import Constants.SortOrder;

public class TSVManipulations {
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static TreeMap<String, String[]> readUniqueKeyAndItsValueFromTSV(String inputPathAndFileName, Set<String> keySetToCheckExistenceOfKeyField, 
			int keyFieldNumber, int totalFieldsCount, String fieldNumbersToBeRead_separatedByDollar, 
			LogicalOperand logicalOperand, 
			int field1Number, ConditionType condition1Type, String field1Value, FieldType field1Type, 
			int field2Number, ConditionType condition2Type, String field2Value, FieldType field2Type, 
			boolean wrapOutputInLines, int showProgressInterval, int indentationLevel, long testOrReal, String writeMessageStep){//This method reads TSV lines into HashMap. The key is a unique field and value is a String[] containing all the values of that row. 
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		TreeMap<String, String[]> tsvRecordsHashMap = new TreeMap<String, String[]>();
		try{ 
			BufferedReader br;
			//Reading posts and adding <repoId, totalNumberOfMembers> in a hashMap:
			br = new BufferedReader(new FileReader(inputPathAndFileName)); 
			System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Parsing " + inputPathAndFileName + ":");
			System.out.println(MyUtils.indent(indentationLevel) + "Started ...");
			int error1 = 0, error2 = 0, unmatchedRecords = 0;;
			int i=0, matchedRec = 0;
			String s, keyField;
			boolean recordShouldBeRead;
			br.readLine(); //header.
			while ((s=br.readLine())!=null){
				String[] fields = s.split("\t");
				if (fields.length == totalFieldsCount){
					recordShouldBeRead = MyUtils.runLogicalComparison(logicalOperand, fields[field1Number], condition1Type, field1Value, field1Type, fields[field2Number], condition2Type, field2Value, field2Type);
					if (recordShouldBeRead){
						keyField = fields[keyFieldNumber];
						if (keySetToCheckExistenceOfKeyField == null || keySetToCheckExistenceOfKeyField.contains(keyField)){
							matchedRec++;
							if (!tsvRecordsHashMap.containsKey(keyField)){
								if (fieldNumbersToBeRead_separatedByDollar.equals("ALL"))//means that all the fields are needed.
									tsvRecordsHashMap.put(keyField, fields);
								else{//means that only some of the fields are needed.
									String[] neededFields = fieldNumbersToBeRead_separatedByDollar.split("\\$");
									for (int k=0; k<neededFields.length; k++)
										neededFields[k] = fields[Integer.parseInt(neededFields[k])];
									tsvRecordsHashMap.put(keyField, neededFields);
								}//else.
							}//if.
							else
								error2++;	//System.out.println("----" + keyField + "----" + fields[0] + "\t" + fields[1] + "\t" + fields[2]); //It should have been unique though!
						}//if (KeyS....					
					}
					else
						unmatchedRecords++;
				}//if.
				else
					error1++;
				i++;
				if (testOrReal > Constants.THIS_IS_REAL)
					if (i >= testOrReal)
						break;
				if (i % showProgressInterval == 0)
					System.out.println(MyUtils.indent(indentationLevel) + Constants.integerFormatter.format(i));
			}//while ((s=br....
			if (error1>0)
				System.out.println(MyUtils.indent(indentationLevel) + "Error) Number of records with !=" + totalFieldsCount + " fields: " + Constants.integerFormatter.format(error1));
			if (error2>0)
				System.out.println(MyUtils.indent(indentationLevel) + "Error) Number of records with repeated keyField: " + Constants.integerFormatter.format(error2));

			if (logicalOperand == LogicalOperand.NO_CONDITION)
				System.out.println(MyUtils.indent(indentationLevel) + "Number of records read: " + Constants.integerFormatter.format(matchedRec));
			else{
				System.out.println(MyUtils.indent(indentationLevel) + "Number of records read (matched with the provided conditions): " + Constants.integerFormatter.format(matchedRec));
				if (unmatchedRecords == 0)
					System.out.println(MyUtils.indent(indentationLevel) + ":-) No unmatched records with the conditions provided.");
				else
					System.out.println(MyUtils.indent(indentationLevel) + "Number of ignored records (unmatched with the provided conditions): " + Constants.integerFormatter.format(unmatchedRecords));
			}//if (cond....
			System.out.println(MyUtils.indent(indentationLevel) + "Finished.");
//			System.out.println();
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return tsvRecordsHashMap;
	}
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static HashSet<String> readNonUniqueFieldFromTSV_OnlyRepeatEachEntryOnce(String inputPathAndFileName,
			int keyFieldNumber, int totalFieldsCount,
			LogicalOperand logicalOperand, 
			int field1Number, ConditionType condition1Type, String field1Value, FieldType field1Type, 
			int field2Number, ConditionType condition2Type, String field2Value, FieldType field2Type, 
			boolean wrapOutputInLines, int indentationLevel, int showProgressInterval, long testOrReal, int writeMessageStep){//This method reads only one field in TSV lines into HashSet. The key is a non-unique field. If it sees repeats of that field, just ignores it. 
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		HashSet<String> tsvfieldHashSet = new HashSet<String>();
		try{ 
			BufferedReader br;
			//Reading posts and adding <repoId, totalNumberOfMembers> in a hashMap:
			br = new BufferedReader(new FileReader(inputPathAndFileName)); 
			System.out.println(writeMessageStep + MyUtils.indent(indentationLevel) + "- Parsing " + inputPathAndFileName + ":");
			System.out.println(MyUtils.indent(indentationLevel) + "Started ...");
			int error1 = 0, unmatchedRecords = 0;
			String[] fields;
			int i=0; 
			int matchedRec = 0;
			String s, keyField;
			boolean recordShouldBeRead;
			br.readLine(); //header.
			while ((s=br.readLine())!=null){
				fields = s.split("\t");
				if (fields.length != totalFieldsCount)
					error1++;
				else{
					recordShouldBeRead = MyUtils.runLogicalComparison(logicalOperand, fields[field1Number], condition1Type, field1Value, field1Type, fields[field2Number], condition2Type, field2Value, field2Type);
					if (recordShouldBeRead){
						keyField = fields[keyFieldNumber];
						if (!tsvfieldHashSet.contains(keyField))
							tsvfieldHashSet.add(keyField);
						matchedRec++;
					}//if (reco....
					else
						unmatchedRecords++;
				}//else.
				i++;
				if (testOrReal > Constants.THIS_IS_REAL)
					if (i >= testOrReal)
						break;
				if (i % showProgressInterval == 0)
					System.out.println(MyUtils.indent(indentationLevel) + Constants.integerFormatter.format(i));
			}//while ((s=br....
			if (error1>0)
				System.out.println(MyUtils.indent(indentationLevel) + "Error) Number of records with !=" + totalFieldsCount + " fields: " + error1);
			if (logicalOperand == LogicalOperand.NO_CONDITION)
				System.out.println(MyUtils.indent(indentationLevel) + "Number of records read: " + Constants.integerFormatter.format(matchedRec));
			else{
				System.out.println(MyUtils.indent(indentationLevel) + "Number of records read (matched with the provided conditions): " + Constants.integerFormatter.format(matchedRec));
				if (unmatchedRecords == 0)
					System.out.println(MyUtils.indent(indentationLevel) + ":-) No unmatched records with the conditions provided.");
				else{
					System.out.println(MyUtils.indent(indentationLevel) + "Number of ignored records (unmatched with the provided conditions): " + Constants.integerFormatter.format(unmatchedRecords));
				}//else.
			}//else.
			System.out.println(MyUtils.indent(indentationLevel) + "Finished.");
			System.out.println();
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return tsvfieldHashSet;
	}
	//--------------------------------------------------------------------------------------------------------------------------------------------	
	public static HashSet<String> readUniqueFieldFromTSV(String inputPathAndFileName, 
			int keyFieldNumber, int totalFieldsCount,
			LogicalOperand logicalOperand, 
			int field1Number, ConditionType condition1Type, String field1Value, FieldType field1Type, 
			int field2Number, ConditionType condition2Type, String field2Value, FieldType field2Type, 
			boolean wrapOutputInLines, int indentationLevel, int showProgressInterval, long testOrReal, int writeMessageStep){//This method reads only one field in TSV lines into HashSet. The key is a unique field. If it sees repeats of that field, just ignores it but increments the number of errors and finally reports it. 
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		HashSet<String> tsvfieldHashSet = new HashSet<String>();
		try{ 
			BufferedReader br;
			//Reading posts and adding <repoId, totalNumberOfMembers> in a hashMap:
			br = new BufferedReader(new FileReader(inputPathAndFileName)); 
			System.out.println(writeMessageStep + MyUtils.indent(indentationLevel) + "- Parsing " + inputPathAndFileName  + ":");
			System.out.println(MyUtils.indent(indentationLevel) + "Started ...");
			int error1 = 0, error2 = 0, unmatchedRecords = 0;
			String[] fields;
			int i=0; 
			int matchedRec = 0;
			String s, keyField;
			boolean recordShouldBeRead;
			br.readLine(); //header.
			while ((s=br.readLine())!=null){
				fields = s.split("\t");
				if (fields.length != totalFieldsCount)
					error1++;
				else{
					recordShouldBeRead = MyUtils.runLogicalComparison(logicalOperand, fields[field1Number], condition1Type, field1Value, field1Type, fields[field2Number], condition2Type, field2Value, field2Type);
					if (recordShouldBeRead){
						keyField = fields[keyFieldNumber];
						if (!tsvfieldHashSet.contains(keyField))
							tsvfieldHashSet.add(keyField);
						else
							error2++;
						matchedRec++;
					}//if (reco....
					else
						unmatchedRecords++;
				}//else.
				i++;
				if (testOrReal > Constants.THIS_IS_REAL)
					if (i >= testOrReal)
						break;
				if (i % showProgressInterval == 0)
					System.out.println(MyUtils.indent(indentationLevel) + Constants.integerFormatter.format(i));
			}//while ((s=br....
			if (error1>0)
				System.out.println(MyUtils.indent(indentationLevel) + "Error) Number of records with != " + totalFieldsCount + " fields: " + error1);
			if (error2>0)
				System.out.println(MyUtils.indent(indentationLevel) + "Error) Number of records with duplicate keyfield: " + error2);
			if (logicalOperand == LogicalOperand.NO_CONDITION)
				System.out.println(MyUtils.indent(indentationLevel) + "Number of records read: " + Constants.integerFormatter.format(matchedRec));
			else{
				System.out.println(MyUtils.indent(indentationLevel) + "Number of records read (matched with the provided conditions): " + Constants.integerFormatter.format(matchedRec));
				if (unmatchedRecords == 0)
					System.out.println(MyUtils.indent(indentationLevel) + ":-) No unmatched records with the conditions provided.");
				else{
					System.out.println(MyUtils.indent(indentationLevel) + "Number of ignored records (unmatched with the provided conditions): " + Constants.integerFormatter.format(unmatchedRecords));
				}//else.
			}//else.
			System.out.println(MyUtils.indent(indentationLevel) + "Finished.");
			System.out.println();
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return tsvfieldHashSet;
	}
	//--------------------------------------------------------------------------------------------------------------------------------------------	
	//This method 
	public static void mergeTwoTSVFieldsTogether(String inputPathAndFileName,  
			String outputPathAndFileName, 
			int field1Number, int field2Number, String delimiter, int totalFieldsNumber,
			boolean wrapOutputInLines, int indentationLevel, int showProgressInterval, long testOrReal, int writeMessageStep){
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		try{ 
			BufferedReader br;
			//Reading posts and adding <repoId, totalNumberOfMembers> in a hashMap:
			br = new BufferedReader(new FileReader(inputPathAndFileName)); 
			System.out.println(writeMessageStep + "- Parsing " + inputPathAndFileName + ", merging two columns and writing in " + outputPathAndFileName + ":");
			System.out.println(MyUtils.indent(indentationLevel) + "Started ...");
			FileWriter writer = new FileWriter(outputPathAndFileName);
			int error = 0;
			String[] fields;
			int i=0;
			String s, output;
			while ((s=br.readLine())!=null){
				fields = s.split("\t");
				if (fields.length == totalFieldsNumber){
					fields[field1Number] = fields[field1Number] + "/" + fields[field2Number];
					output = fields[0];
					for (int j=1; j<totalFieldsNumber; j++)
						if (j != field2Number)
							output = output + "\t" + fields[j];
					output = output + "\n";
					writer.append(output);
				}
				else
					error++;
				i++;
				if (testOrReal > Constants.THIS_IS_REAL)
					if (i >= testOrReal)
						break;
				if (i % showProgressInterval == 0)
					System.out.println(MyUtils.indent(indentationLevel) + Constants.integerFormatter.format(i));
			}
			writer.flush();writer.close();
			br.close();
			System.out.println(MyUtils.indent(indentationLevel) +  Constants.integerFormatter.format(i) + " records have been read.");
			if (error>0)
				System.out.println(MyUtils.indent(indentationLevel) + "Error) Number of records with !=" + totalFieldsNumber + " fields: " + error);
			System.out.println(MyUtils.indent(indentationLevel) + "Finished.");
		}
		catch(Exception e){
			e.printStackTrace();
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
	}//mergeTwoTSVFieldsTogether().
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static TreeMap<String, ArrayList<String[]>> readNonUniqueKeyAndItsValueFromTSV(String inputPathAndFileName, Set<String> keySetToCheckExistenceOfKeyField, 
			int keyFieldNumber, Constants.SortOrder sortOrder, int totalFieldsCount, String fieldNumbersToBeRead_separatedByDollar, 
			Constants.LogicalOperand logicalOperand, 
			int field1Number, ConditionType condition1Type, String field1Value, FieldType field1Type, 
			int field2Number, ConditionType condition2Type, String field2Value, FieldType field2Type, 
			boolean wrapOutputInLines, int showProgressInterval, int indentationLevel, 
			long testOrReal, String writeMessageStep){//This method reads TSV lines into HashMap. 
		//The key is a non-unique field (#keyfieldNumber) and value is an ArrayList<String[]> containing all the values of all the rows that have the same keyFieldNumber. Values of each row is stored in a String[].
		//		TreeMap<String, ArrayList<String[]>> tsvRecordsHashMap = new TreeMap<String, ArrayList<String[]>>();
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		TreeMap<String, ArrayList<String[]>> tsvRecordsTreeMap;
		if (sortOrder == Constants.SortOrder.DEFAULT_FOR_STRING)//means that keyfield is not integer.
			tsvRecordsTreeMap = new TreeMap<String, ArrayList<String[]>>();
		else
			if (sortOrder == Constants.SortOrder.ASCENDING_INTEGER){//means that keyfield is an integer.
				tsvRecordsTreeMap = new TreeMap<String, ArrayList<String[]>>(new Comparator<String>(){
					public int compare(String s1, String s2){//We want the ascending order of number:
//Uncomment these lines if you have empty (or space) values:
//						if (s1.equals("") || s1.equals(" "))
//							s1 = Integer.toString(Constants.AN_EXTREMELY_NEGATIVE_INT);
//						if (s2.equals("") || s2.equals(" "))
//							s2 = Integer.toString(Constants.AN_EXTREMELY_NEGATIVE_INT);
						if (Integer.parseInt(s1) > Integer.parseInt(s2))
							return 1;
						else
							if (Integer.parseInt(s1) < Integer.parseInt(s2))
								return -1;
							else
								return 0;
					}
				});
			}//if.
			else{
				tsvRecordsTreeMap = new TreeMap<String, ArrayList<String[]>>(new Comparator<String>(){
					public int compare(String s1, String s2){//We want the descending order of number:
//						if (s1.equals("") || s1.equals(" "))
//							s1 = Integer.toString(Constants.AN_EXTREMELY_NEGATIVE_INT);
//						if (s2.equals("") || s2.equals(" "))
//							s2 = Integer.toString(Constants.AN_EXTREMELY_NEGATIVE_INT);
						if (Integer.parseInt(s1) < Integer.parseInt(s2))
							return 1;
						else
							if (Integer.parseInt(s1) > Integer.parseInt(s2))
								return -1;
							else
								return 0;
					}
				});
			}//else.
		try{ 
			BufferedReader br;
			//Reading posts and adding <repoId, totalNumberOfMembers> in a hashMap:
			br = new BufferedReader(new FileReader(inputPathAndFileName)); 
			System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Parsing " + inputPathAndFileName + ":");
			System.out.println(MyUtils.indent(indentationLevel) + "Started ...");
			int error = 0, unmatchedRecords = 0;
			String[] fields;
			boolean isNew, recordShouldBeRead;
			ArrayList<String[]> aTSVItemRelatedToTheRepeatedKey; 
			int i=0, equalObjectsFound = 0, matchedRec = 0;
			String s, keyField;
			br.readLine(); //header.
			while ((s=br.readLine())!=null){
				fields = s.split("\t");
				if (fields.length != totalFieldsCount)
					error++;
				else{
					recordShouldBeRead = MyUtils.runLogicalComparison(logicalOperand, fields[field1Number], condition1Type, field1Value, field1Type, fields[field2Number], condition2Type, field2Value, field2Type);
					if (recordShouldBeRead){
						keyField = fields[keyFieldNumber];
						if (keySetToCheckExistenceOfKeyField == null || keySetToCheckExistenceOfKeyField.contains(keyField)){
							if (tsvRecordsTreeMap.containsKey(keyField)){
								aTSVItemRelatedToTheRepeatedKey = tsvRecordsTreeMap.get(keyField);
								isNew = true;
								for (String[] stringArray: aTSVItemRelatedToTheRepeatedKey)
									if (MyUtils.compareTwoStringArrays(fields, stringArray)){ //Note:   if (fields.equals(stringArray))   does not work! Just the used notation is correct.
										equalObjectsFound++;
										isNew = false;
										break;
									}//if.					
								if (isNew){
									if (fieldNumbersToBeRead_separatedByDollar.equals("ALL"))//means that all the fields are needed.
										aTSVItemRelatedToTheRepeatedKey.add(fields);
									else{//means that only some of the fields are needed.
										String[] neededFields = fieldNumbersToBeRead_separatedByDollar.split("\\$");
										for (int k=0; k<neededFields.length; k++)
											neededFields[k] = fields[Integer.parseInt(neededFields[k])];
										aTSVItemRelatedToTheRepeatedKey.add(neededFields);
									}//else.
								}//if
							}//if.
							else{
								ArrayList<String[]> aTSVItemRelatedToANewKey = new ArrayList<String[]>();
								if (fieldNumbersToBeRead_separatedByDollar.equals("ALL"))//means that all the fields are needed.
									aTSVItemRelatedToANewKey.add(fields);
								else{//means that only some of the fields are needed.
									String[] neededFields = fieldNumbersToBeRead_separatedByDollar.split("\\$");
									for (int k=0; k<neededFields.length; k++)
										neededFields[k] = fields[Integer.parseInt(neededFields[k])];
									aTSVItemRelatedToANewKey.add(neededFields);
								}//else.
								tsvRecordsTreeMap.put(keyField, aTSVItemRelatedToANewKey);
							}//else.
							matchedRec++;
						}//if (tsvRecordsHashMap.containsKey(keyField)).
					}//if (reco....
					else
						unmatchedRecords++;
				}//else.
				i++;
				if (testOrReal > Constants.THIS_IS_REAL)
					if (i >= testOrReal)
						break;
				if (i % showProgressInterval == 0)
					System.out.println(MyUtils.indent(indentationLevel) + Constants.integerFormatter.format(i));
			}//while ((s=br....

			if (error>0)
				System.out.println(MyUtils.indent(indentationLevel) + "Error) Number of records with !=" + totalFieldsCount + " fields: " + error);

			if (equalObjectsFound>0)
				System.out.println(MyUtils.indent(indentationLevel) + "Hint) Number of repeated TSV records (ignored): " + equalObjectsFound);

			if (logicalOperand == LogicalOperand.NO_CONDITION)
				System.out.println(MyUtils.indent(indentationLevel) + "Number of records read: " + Constants.integerFormatter.format(matchedRec));
			else
			{
				System.out.println(MyUtils.indent(indentationLevel) + "Number of records read (matched with the provided conditions): " + Constants.integerFormatter.format(matchedRec));
				if (unmatchedRecords == 0)
					System.out.println(MyUtils.indent(indentationLevel) + ":-) No unmatched records with the conditions provided.");
				else
					System.out.println(MyUtils.indent(indentationLevel) + "Number of ignored records (unmatched with the provided conditions): " + Constants.integerFormatter.format(unmatchedRecords));
			}//if (cond....
			System.out.println(MyUtils.indent(indentationLevel) + "Finished.");
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return tsvRecordsTreeMap;
	}
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static FileManipulationResult saveKeyAndValuesAsTSVFile(String outputPathAndFileName, TreeMap<String, Long> counts, 
			int totalFieldsCount, String[] titles,
			boolean wrapOutputInLines, int showProgressInterval, int indentationLevel, long testOrReal, String writeMessageStep){
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		FileManipulationResult fCR = new FileManipulationResult();
		try{
			System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Writing file \"" + outputPathAndFileName + "\"");
			System.out.println(MyUtils.indent(indentationLevel+1) +  "Started ...");
			int i = 0;
			FileWriter writer = new FileWriter(outputPathAndFileName);
			writer.append(titles[0] + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + titles[1] + "\n");
			for(Map.Entry<String,Long> entry : counts.entrySet()) {
				  String key = entry.getKey();
				  Long value = entry.getValue();
				  writer.append(key + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + value + "\n");
				  i++;
				  if (i % showProgressInterval == 0)
					  System.out.println(MyUtils.indent(indentationLevel+1) +  Constants.integerFormatter.format(i));
				}
			writer.flush();
			writer.close();
			System.out.println(MyUtils.indent(indentationLevel+1) + "Number of records written: " + Constants.integerFormatter.format(i) + ".");
			System.out.println(MyUtils.indent(indentationLevel+1) +  "Finished.");
			fCR.DoneSuccessfully = 1;
		}catch (Exception e){
			e.printStackTrace();
			fCR.errors = 1;
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return fCR;
	}//saveCountsAsTSVFile().
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static FileManipulationResult saveTreeMapToTSVFile(String outputPathAndFileName, TreeMap<String, ArrayList<String[]>> tm, 
			String titles,
			boolean wrapOutputInLines, int showProgressInterval, int indentationLevel, long testOrReal, String writeMessageStep){
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		FileManipulationResult fCR = new FileManipulationResult();
		try{
			System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Writing file \"" + outputPathAndFileName + "\"");
			System.out.println(MyUtils.indent(indentationLevel) +  "Started ...");
			int i = 0;
			FileWriter writer = new FileWriter(outputPathAndFileName);
			writer.append(titles + "\n");
			for(Map.Entry<String,ArrayList<String[]>> entry : tm.entrySet()) {
				for (int j=0; j<entry.getValue().size(); j++){
					String[] s = entry.getValue().get(j);
					String aLine = "";
					if (s.length>0){
						aLine = s[0];
						for (int k=1; k<s.length; k++)
							aLine = aLine + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + s[k];
					}
					writer.append(entry.getKey() + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + aLine + "\n");
				}//for.
				i++;
				if (i % showProgressInterval == 0)
					System.out.println(MyUtils.indent(indentationLevel+1) +  Constants.integerFormatter.format(i));
			}
			writer.flush();
			writer.close();
			System.out.println(MyUtils.indent(indentationLevel) + "Number of records written: " + Constants.integerFormatter.format(i) + ".");
			System.out.println(MyUtils.indent(indentationLevel) +  "Finished.");
			fCR.DoneSuccessfully = 1;
		}catch (Exception e){
			e.printStackTrace();
			fCR.errors = 1;
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return fCR;
	}//saveCountsAsTSVFile().
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static TreeMap<String, Long> groupBy_count_fromTSV(String inputPathAndFileName, Set<String> keySetToCheckExistenceOfKeyField, 
			String keyField, Constants.SortOrder sortOrder, int totalFieldsCount, FileManipulationResult[] fCRArray, 
			boolean wrapOutputInLines, int showProgressInterval, int indentationLevel,
			long testOrReal, String writeMessageStep){//This method reads TSV lines and counts the records for each key (something like getting count(x) after group by key in SQL). 
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		TreeMap<String, Long> result = new TreeMap<String, Long>();
		if (sortOrder == Constants.SortOrder.DEFAULT_FOR_STRING)//means that keyfield is not integer.
			result = new TreeMap<String, Long>();
		else
			if (sortOrder == Constants.SortOrder.ASCENDING_INTEGER){//means that keyfield is an integer.
				result = new TreeMap<String, Long>(new Comparator<String>(){
					public int compare(String s1, String s2){//We want the ascending order of number:
						//You can comment these (4) lines if you don't have empty (or space) values:
						if (s1.equals("") || s1.equals(" "))
							s1 = Long.toString(Constants.AN_EXTREMELY_NEGATIVE_LONG);
						if (s2.equals("") || s2.equals(" "))
							s2 = Long.toString(Constants.AN_EXTREMELY_NEGATIVE_LONG);
						//Up to here.
						if (Long.parseLong(s1) > Long.parseLong(s2))
							return 1;
						else
							if (Long.parseLong(s1) < Long.parseLong(s2))
								return -1;
							else
								return 0;
					}
				}); //result = new Tree...
			}//if.
			else{
				result = new TreeMap<String, Long>(new Comparator<String>(){
					public int compare(String s1, String s2){//We want the descending order of number:
						//You can comment these (4) lines if you don't have empty (or space) values:
						if (s1.equals("") || s1.equals(" "))
							s1 = Long.toString(Constants.AN_EXTREMELY_POSITIVE_LONG);
						if (s2.equals("") || s2.equals(" "))
							s2 = Long.toString(Constants.AN_EXTREMELY_POSITIVE_LONG);
						//Up to here.
						if (Long.parseLong(s1) < Long.parseLong(s2))
							return 1;
						else
							if (Long.parseLong(s1) > Long.parseLong(s2))
								return -1;
							else
								return 0;
					}
				});
			}//else.
		try{ 
			BufferedReader br;
			br = new BufferedReader(new FileReader(inputPathAndFileName)); 
			System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Counting values for \"" + keyField + "\" (in \"" + inputPathAndFileName + "\"):");
			System.out.println(MyUtils.indent(indentationLevel+1) +  "Started ...");
			int error1=0, error2 = 0;
			String[] fields, titles;
			int i=0, keyFieldNumber = Constants.ERROR;
			String s, keyFieldValue;
			s = br.readLine(); //header.
			titles = s.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
			for (int j=0; j< titles.length; j++)
				if (titles[j].equals(keyField))
					keyFieldNumber = j;
			if (keyFieldNumber == Constants.ERROR)
				error1++;
			else
				while ((s=br.readLine())!=null){
					if (!s.equals("")){
						fields = s.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
						if (fields.length != totalFieldsCount)
							error2++;
						else{
							keyFieldValue = fields[keyFieldNumber];
							if (keySetToCheckExistenceOfKeyField == null || keySetToCheckExistenceOfKeyField.contains(keyFieldValue)){
								long count;
								if (result.containsKey(keyFieldValue))
									count = result.get(keyFieldValue)+1;
								else
									count = 1;
								result.put(keyFieldValue, count);
							}						
						}//else.
						i++;
						if (i % showProgressInterval == 0)
							System.out.println(MyUtils.indent(indentationLevel+1) +  Constants.integerFormatter.format(i));
						if (testOrReal > Constants.THIS_IS_REAL)
							if (i >= testOrReal)
								break;
					}//if.
				}//while ((s=br....
			System.out.println(MyUtils.indent(indentationLevel+1) + "Number of records read: " + Constants.integerFormatter.format(i) + ".");
			if (error1>0){
				System.out.println(MyUtils.indent(indentationLevel+1) + "Error) \"" + keyField + "\" was not identified!");
				fCRArray[0].errors = 1;
			}
			if (error2>0){
				System.out.println(MyUtils.indent(indentationLevel+1) + "Error) Number of records with != " + totalFieldsCount + " fields: " + Constants.integerFormatter.format(error2));
				fCRArray[0].errors = 1;
			}
			System.out.println(MyUtils.indent(indentationLevel+1) + "Finished.");
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			fCRArray[0].errors = 1;
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return result;
	}
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static TreeMap<String, Long> groupBy_sum_fromTSV(String inputPathAndFileName, Set<String> keySetToCheckExistenceOfKeyField, 
			String keyField, String summingField, Constants.SortOrder sortOrder, int totalFieldsCount, FileManipulationResult[] fCRArray, 
			boolean wrapOutputInLines, int showProgressInterval, int indentationLevel,
			long testOrReal, String writeMessageStep){//This method reads TSV lines and counts the records for each key (something like getting count(x) after group by key in SQL). 
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		TreeMap<String, Long> result = new TreeMap<String, Long>();
		if (sortOrder == Constants.SortOrder.DEFAULT_FOR_STRING)//means that keyfield is not integer.
			result = new TreeMap<String, Long>();
		else
			if (sortOrder == Constants.SortOrder.ASCENDING_INTEGER){//means that keyfield is an integer.
				result = new TreeMap<String, Long>(new Comparator<String>(){
					public int compare(String s1, String s2){//We want the ascending order of number:
						//You can comment these (4) lines if you don't have empty (or space) values:
						if (s1.equals("") || s1.equals(" "))
							s1 = Long.toString(Constants.AN_EXTREMELY_NEGATIVE_LONG);
						if (s2.equals("") || s2.equals(" "))
							s2 = Long.toString(Constants.AN_EXTREMELY_NEGATIVE_LONG);
						//Up to here.
						if (Long.parseLong(s1) > Long.parseLong(s2))
							return 1;
						else
							if (Long.parseLong(s1) < Long.parseLong(s2))
								return -1;
							else
								return 0;
					}
				}); //result = new Tree...
			}//if.
			else{
				result = new TreeMap<String, Long>(new Comparator<String>(){
					public int compare(String s1, String s2){//We want the descending order of number:
						//You can comment these (4) lines if you don't have empty (or space) values:
						if (s1.equals("") || s1.equals(" "))
							s1 = Long.toString(Constants.AN_EXTREMELY_POSITIVE_LONG);
						if (s2.equals("") || s2.equals(" "))
							s2 = Long.toString(Constants.AN_EXTREMELY_POSITIVE_LONG);
						//Up to here.
						if (Long.parseLong(s1) < Long.parseLong(s2))
							return 1;
						else
							if (Long.parseLong(s1) > Long.parseLong(s2))
								return -1;
							else
								return 0;
					}
				});
			}//else.
		try{ 
			BufferedReader br;
			br = new BufferedReader(new FileReader(inputPathAndFileName)); 
			System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Sum(" + summingField + ") after grouping by \"" + keyField + "\" (in \"" + inputPathAndFileName + "\"):");
			System.out.println(MyUtils.indent(indentationLevel+1) +  "Started ...");
			int error = 0;
			String[] fields, titles;
			int i=0, keyFieldNumber = Constants.ERROR, summingFieldNumber = Constants.ERROR;
			String s, keyFieldValue, summingFieldValue;
			s = br.readLine(); //header.
			titles = s.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
			for (int j=0; j< titles.length; j++){
				if (titles[j].equals(keyField))
					keyFieldNumber = j;
				if (titles[j].equals(summingField))
					summingFieldNumber = j;
			}//for.
			if (keyFieldNumber == Constants.ERROR || summingFieldNumber == Constants.ERROR)
				error++;
			else
				while ((s=br.readLine())!=null){
					fields = s.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
					if (fields.length != totalFieldsCount)
						error++;
					else{
						keyFieldValue = fields[keyFieldNumber];
						summingFieldValue = fields[summingFieldNumber];
						if (keySetToCheckExistenceOfKeyField == null || keySetToCheckExistenceOfKeyField.contains(keyFieldValue)){
							long theNewValue;
							if (summingFieldValue.equals(""))
								theNewValue = 0;
							else
								theNewValue = Integer.parseInt(summingFieldValue);
							long theNewValuePlusSumOfAllOtherValues;
							if (result.containsKey(keyFieldValue))
								theNewValuePlusSumOfAllOtherValues = result.get(keyFieldValue)+theNewValue;
							else
								theNewValuePlusSumOfAllOtherValues = theNewValue;
							result.put(keyFieldValue, theNewValuePlusSumOfAllOtherValues);
						}						
					}//else.
					i++;
					if (i % showProgressInterval == 0)
						System.out.println(MyUtils.indent(indentationLevel+1) +  Constants.integerFormatter.format(i));
					if (testOrReal > Constants.THIS_IS_REAL)
						if (i >= testOrReal)
							break;
				}//while ((s=br....
			System.out.println(MyUtils.indent(indentationLevel+1) + "Number of records read: " + Constants.integerFormatter.format(i) + ".");
			if (error>0){
				System.out.println(MyUtils.indent(indentationLevel+1) + "Error) Number of records with != " + totalFieldsCount + " fields: " + Constants.integerFormatter.format(error));
				fCRArray[0].errors = 1;
			}
			System.out.println(MyUtils.indent(indentationLevel+1) + "Finished.");
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			fCRArray[0].errors = 1;
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return result;
	}
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//This file replaces a foreign key (e.g., userId) with its value from another TSV (provided that the relation is 1:1 or 1:n)
	public static FileManipulationResult replaceForeignKeyInTSVWithValueFromAnotherTSV(String foreignKeyInputPathAndFileName,  
			String primaryKeyInputPathAndFileName, 
			String outputPathAndFileName, 
			String fKField, int foreignKeyTotalFieldsNumber,
			String pKField, int primaryKeyTotalFieldsNumber, 
			String pKSubstituteField, //this is the field that is written instead of foreign key.
			String substituteNewTitle,//under this title.
			boolean wrapOutputInLines, int showProgressInterval, int indentationLevel, long testOrReal, String writeMessageStep
			){
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		FileManipulationResult fCR = new FileManipulationResult();
		try{ 
			System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Foreign Key replacement (\"" + fKField + "\") in \"" + foreignKeyInputPathAndFileName + "\"):");
			int error = 0;
			String[] titles;
			int i=0, PrimaryKeyFieldNumber = Constants.ERROR, foreignKeyFieldNumber = Constants.ERROR, primaryKeySubstituteFieldNumber = Constants.ERROR;
			String s, header, outputLine;

			BufferedReader br;
			br = new BufferedReader(new FileReader(primaryKeyInputPathAndFileName)); 
			s = br.readLine();
			titles = s.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
			for (int j=0; j< titles.length; j++){
				if (titles[j].equals(pKField))
					PrimaryKeyFieldNumber = j;
				if (titles[j].equals(pKSubstituteField))
					primaryKeySubstituteFieldNumber = j;
			}//for.
			br.close();
			
			TreeMap<String, String[]> primaryKeyRecords = readUniqueKeyAndItsValueFromTSV(
					primaryKeyInputPathAndFileName, null, PrimaryKeyFieldNumber, primaryKeyTotalFieldsNumber, "ALL", 
					LogicalOperand.NO_CONDITION, 0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
					false, showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-1");
			//TreeMap<String, String[]> foreignKeyRecords = readUniqueKeyAndItsValueFromTSV(foreignKeyInputTSVPath, foreignKeyInputTSVFile, ForeignKeyFieldNumber, foreignKeyTotalFieldsNumber, ConditionType.NO_CONDITION, 0, "", 0, "", 100000, testOrReal, 2);
			System.out.println(MyUtils.indent(indentationLevel+1) + writeMessageStep+"-2- Producing output (foreign key replaced by value) ...");
			System.out.println(MyUtils.indent(indentationLevel+1) + "Started ...");
			String[] theFKRecord, aPKRecord;

			//Reading the header and substituting the foreign key field title:
			br = new BufferedReader(new FileReader(foreignKeyInputPathAndFileName)); 
			s = br.readLine();
			titles = s.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
			for (int j=0; j< titles.length; j++){
				if (titles[j].equals(fKField))
					foreignKeyFieldNumber = j;
			}//for.

			if (foreignKeyFieldNumber == 0)
				header = substituteNewTitle;
			else
				header = titles[0];
			for (int j=1; j<foreignKeyTotalFieldsNumber; j++)
				if (j == foreignKeyFieldNumber)
					header = header + "\t" + substituteNewTitle;
				else
					header = header + "\t" + titles[j];
			header = header + "\n";
			FileWriter writer = new FileWriter(outputPathAndFileName);
			writer.append(header);
			//Now, replacing the FK with values from PK file:
			while ((s=br.readLine())!=null){
				theFKRecord = s.split("\t");
				if (theFKRecord.length == foreignKeyTotalFieldsNumber){
					aPKRecord = primaryKeyRecords.get(theFKRecord[foreignKeyFieldNumber]);
					if (aPKRecord == null)//:means that the foreign key points to something that does not exist. return "" (NULL) in this case.
						theFKRecord[foreignKeyFieldNumber] = "";
					else
						theFKRecord[foreignKeyFieldNumber] = aPKRecord[primaryKeySubstituteFieldNumber];
					outputLine = theFKRecord[0];
					for (int j=1; j<theFKRecord.length; j++)
						outputLine = outputLine + "\t" + theFKRecord[j];
					outputLine = outputLine + "\n";
					writer.append(outputLine);
				}
				else
					error++;
				i++;
				if (testOrReal > Constants.THIS_IS_REAL)
					if (i >= testOrReal)
						break;
				if (i % showProgressInterval == 0)
					System.out.println(MyUtils.indent(indentationLevel) + Constants.integerFormatter.format(i));

			}//for (Stri....
			writer.flush();writer.close();
			br.close();
			System.out.println(MyUtils.indent(indentationLevel+2) + "Number of records read: " + Constants.integerFormatter.format(i));
			fCR.processed = 1;
			if (error>0){
				System.out.println(MyUtils.indent(indentationLevel+2) + "Error) Number of FK records with !=" + foreignKeyTotalFieldsNumber + " fields: " + error);
				fCR.errors = 1;
			}//if.
			else
				fCR.DoneSuccessfully = 1;
			System.out.println(MyUtils.indent(indentationLevel+1) + "Finished.");
		}catch(Exception e){
			e.printStackTrace();
			fCR.errors = 1;
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return fCR;
	}//replaceForeignKeyInTSVWithValueFromAnotherTSV().
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static FileManipulationResult runGroupBy_count_andSaveResultToTSV(String inputPath, String inputTSVFileName, String outputPath, String outputTSVFileName,  
			String groupByField, int totalFieldsCount, String titleOfGroupByFieldInOutputFile, String titleOfCountedFieldForOutputFile, SortOrder sortOrder, 
			boolean wrapOutputInLines, int indentationLevel, long testOrReal, int showProgressInterval, String writeMessageStep) {
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Groupping by \"" + groupByField + "\" (in \"" + inputTSVFileName + "\"):");
		System.out.println(MyUtils.indent(indentationLevel+1) + "Started ...");
		FileManipulationResult result = new FileManipulationResult();	

		FileManipulationResult[] fCRArray = new FileManipulationResult[1]; 
		fCRArray[0] = new FileManipulationResult();//: this variable is for making a call-by-reference variable.
		TreeMap<String, Long> usersAndTheirFollowers = groupBy_count_fromTSV(
			inputPath+"\\"+inputTSVFileName, null, groupByField, sortOrder , totalFieldsCount, fCRArray, 
			false, showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-1");
		result = MyUtils.addFileManipulationResults(result, fCRArray[0]);
		
		String[] titles = new String[]{titleOfGroupByFieldInOutputFile, titleOfCountedFieldForOutputFile};
		fCRArray[0] = saveKeyAndValuesAsTSVFile(outputPath+"\\"+outputTSVFileName, usersAndTheirFollowers,  
				totalFieldsCount, titles, 
				false, showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-2");
		result = MyUtils.addFileManipulationResults(result, fCRArray[0]);
		
		System.out.println(MyUtils.indent(indentationLevel+1) + "Finished.");
		result.processed = 1;
		if (result.errors == 0)
			result.DoneSuccessfully = 1;
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return result;
	}//convertAllFilseInFolderToTSV().
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	public static FileManipulationResult runGroupBy_sum_andSaveResultToTSV(String inputPath, String inputTSVFileName, String outputPath, String outputTSVFileName,  
			String groupByField, String summingField, int totalFieldsCount, String titleOfGroupByFieldInOutputFile, String titleOfSummedFieldForOutputFile, SortOrder sortOrder, 
			boolean wrapOutputInLines, int indentationLevel, long testOrReal, int showProgressInterval, String writeMessageStep) {
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Groupping by \"" + groupByField + "\" (in \"" + inputTSVFileName + "\"):");
		System.out.println(MyUtils.indent(indentationLevel+1) + "Started ...");
		FileManipulationResult result = new FileManipulationResult();	

		FileManipulationResult[] fCRArray = new FileManipulationResult[1]; 
		fCRArray[0] = new FileManipulationResult();//: this variable is for making a call-by-reference variable.
		TreeMap<String, Long> usersAndTheirFollowers = groupBy_sum_fromTSV(
			inputPath+"\\"+inputTSVFileName, null, groupByField, summingField, sortOrder , totalFieldsCount, fCRArray, 
			false, showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-1");
		result = MyUtils.addFileManipulationResults(result, fCRArray[0]);
		
		String[] titles = new String[]{titleOfGroupByFieldInOutputFile, titleOfSummedFieldForOutputFile};
		fCRArray[0] = saveKeyAndValuesAsTSVFile(outputPath+"\\"+outputTSVFileName, usersAndTheirFollowers,  
				totalFieldsCount, titles, 
				false, showProgressInterval, indentationLevel+1, testOrReal, writeMessageStep+"-2");
		result = MyUtils.addFileManipulationResults(result, fCRArray[0]);
		
		System.out.println(MyUtils.indent(indentationLevel+1) + "Finished.");
		result.processed = 1;
		if (result.errors == 0)
			result.DoneSuccessfully = 1;
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return result;
	}//groupBy_sum().
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	//T1Fields and T2Fields are lists of fields separated by Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE (i.e. "\t")
	public static FileManipulationResult joinTwoTSV(String inputPath1, String inputTSV1, String inputPath2, String inputTSV2, String outputPath, String outputTSV, 
			String t1Key, String t2Key, Constants.JoinType joinType, String t1NeededFields, String t2NeededFields, 
			Constants.SortOrder sortOrder, String substituteForNullValuesInJoin, //:these two fields are for sorting the records while processing. If the key fields are "integer" then it's better to select ASCENDING_INTEGER (otherwise DEFAULT_FOR_STRING).
			boolean wrapOutputInLines, int indentationLevel, long testOrReal, int showProgressInterval, String writeMessageStep
			){//Join condition: inputTSV1.T1Key = inputTSV2.T2Key
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		FileManipulationResult result = new FileManipulationResult();	
		try{ 
			System.out.println(MyUtils.indent(indentationLevel) + writeMessageStep + "- Joining (\"" + inputTSV1 + "\" and \"" + inputTSV2 + "\"):");
			System.out.println(MyUtils.indent(indentationLevel+1) + "Started ...");
			int t1KeyNumber = Constants.ERROR, t2KeyNumber = Constants.ERROR;
			String s;
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "-1- Reading needed fields from \"" + inputTSV1 + "\" ...");
			//Determining needed fields from TSV1: 
			BufferedReader br1;
			br1 = new BufferedReader(new FileReader(inputPath1 + "\\" + inputTSV1)); 
			s = br1.readLine();
			String[] tSVTitles1 = s.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
			String[] neededTitles1 = t1NeededFields.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
			String neededFieldNumbers1 = "";
			for (int j=0; j<neededTitles1.length; j++)//:iterate over all needed titles from TSV1.
				for (int k=0; k< tSVTitles1.length; k++){//:iterate over all titles in TSV1.
					if (tSVTitles1[k].equals(neededTitles1[j]))
						if (neededFieldNumbers1.equals(""))
							neededFieldNumbers1 = Integer.toString(k);
						else
							neededFieldNumbers1 = neededFieldNumbers1 + "$" + k;//:as a field separator (to prepare a string containing all needed field numbers separated by dollar).
					if (tSVTitles1[k].equals(t1Key))
						t1KeyNumber = k;
			}//for.
			br1.close();
			if (t1KeyNumber == Constants.ERROR)
				result.errors = 1;
			//Reading needed fields from TSV1:
			TreeMap<String, ArrayList<String[]>> t1Records = readNonUniqueKeyAndItsValueFromTSV(
					inputPath1 + "\\" + inputTSV1, null, t1KeyNumber, SortOrder.DEFAULT_FOR_STRING, tSVTitles1.length, neededFieldNumbers1, 
					LogicalOperand.NO_CONDITION, 
					0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
					0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
					false, showProgressInterval, indentationLevel+3, testOrReal, writeMessageStep+"-1-1");

			//Determining needed fields from TSV2: 
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "-2- Reading needed fields from \"" + inputTSV2 + "\" ...");
			BufferedReader br2;
			br2 = new BufferedReader(new FileReader(inputPath2 + "\\" + inputTSV2)); 
			s = br2.readLine();
			String[] tSVTitles2 = s.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
			String[] neededTitles2 = t2NeededFields.split(Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE);
			String neededFieldNumbers2 = "";
			for (int j=0; j<neededTitles2.length; j++)//:iterate over all needed titles from TSV1.
				for (int k=0; k< tSVTitles2.length; k++){//:iterate over all titles in TSV1.
					if (tSVTitles2[k].equals(neededTitles2[j]))
						if (neededFieldNumbers2.equals(""))
							neededFieldNumbers2 = Integer.toString(k);
						else
							neededFieldNumbers2 = neededFieldNumbers2 + "$" + k;//:as a field separator (to prepare a string containing all needed field numbers separated by dollar).
					if (tSVTitles2[k].equals(t2Key))
						t2KeyNumber = k;
			}//for.
			br2.close();
			if (t2KeyNumber == Constants.ERROR)
				result.errors = 1;
			//Reading needed fields from TSV1:
			TreeMap<String, ArrayList<String[]>> t2Records = readNonUniqueKeyAndItsValueFromTSV(
					inputPath2 + "\\" + inputTSV2, null, t2KeyNumber, SortOrder.DEFAULT_FOR_STRING, tSVTitles2.length, neededFieldNumbers2, 
					LogicalOperand.NO_CONDITION, 
					0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
					0, ConditionType.NOTHING, "", FieldType.NOT_IMPORTANT, 
					false, showProgressInterval, indentationLevel+3, testOrReal, writeMessageStep+"-2-1");

			//Now joining T1Records and T2Records:
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "-3- Joining \"" + inputTSV1 + "\" with \"" + inputTSV2 + "\" (part 1; inner join) ...");
			Map.Entry<String, ArrayList<String[]>> entry1, entry2;
			//Inner join:
			TreeMap<String, ArrayList<String[]>> resultingRecords;
			if (sortOrder == Constants.SortOrder.ASCENDING_INTEGER){//means that keyfield is an integer.
				resultingRecords = new TreeMap<String, ArrayList<String[]>>(new Comparator<String>(){
					public int compare(String s1, String s2){//We want the ascending order of number:
//Uncomment these lines if you have empty (or space) values:
//						if (s1.equals("") || s1.equals(" "))
//							s1 = Integer.toString(Constants.AN_EXTREMELY_NEGATIVE_INT);
//						if (s2.equals("") || s2.equals(" "))
//							s2 = Integer.toString(Constants.AN_EXTREMELY_NEGATIVE_INT);
						if (Integer.parseInt(s1) > Integer.parseInt(s2))
							return 1;
						else
							if (Integer.parseInt(s1) < Integer.parseInt(s2))
								return -1;
							else
								return 0;
					}
				});
			}//if.
			else{
				resultingRecords = new TreeMap<String, ArrayList<String[]>>(new Comparator<String>(){
					public int compare(String s1, String s2){//We want the descending order of number:
//						if (s1.equals("") || s1.equals(" "))
//							s1 = Integer.toString(Constants.AN_EXTREMELY_NEGATIVE_INT);
//						if (s2.equals("") || s2.equals(" "))
//							s2 = Integer.toString(Constants.AN_EXTREMELY_NEGATIVE_INT);
						if (Integer.parseInt(s1) < Integer.parseInt(s2))
							return 1;
						else
							if (Integer.parseInt(s1) > Integer.parseInt(s2))
								return -1;
							else
								return 0;
					}
				});
			}//else.
			int i = 0;
			int innerJoinCounter = 0;
			System.out.println(t1Records.size());
			System.out.println(t2Records.size());
			for(Iterator<Map.Entry<String, ArrayList<String[]>>> iter1 = t1Records.entrySet().iterator(); iter1.hasNext();){
				entry1 = iter1.next();
				i++;
				if (i % showProgressInterval == 0)
					System.out.println(MyUtils.indent(indentationLevel+2) + Constants.integerFormatter.format(i));
				for(Iterator<Map.Entry<String, ArrayList<String[]>>> iter2 = t2Records.entrySet().iterator(); iter2.hasNext();){
					entry2 = iter2.next();
					if (entry1.getKey().equals(entry2.getKey())){//:this is the join condition.
						ArrayList<String[]> joinedValues = new ArrayList<String[]>();
						for (int j=0; j<entry1.getValue().size(); j++)
							for (int k=0; k<entry2.getValue().size(); k++){
								//Making records of <entry1Values[j], entry2Values[k]>
								String[] aResultingRecord = MyUtils.concatTwoStringArrays(entry1.getValue().get(j), entry2.getValue().get(k));
								joinedValues.add(aResultingRecord);
							}//for k.
						resultingRecords.put(entry1.getKey(), joinedValues);//Adding the joining key, entry1.getKey() to the start of each record, records of <entry1Values[j], entry2Values[k]> will be complete.
						iter2.remove();
						innerJoinCounter++;
					}//if.
				}//for.
				iter1.remove();
			}//for.
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "Number of records added in inner join: " + innerJoinCounter);
			//Left join (also part of full join):
			System.out.println(t1Records.size());
			System.out.println(t2Records.size());
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "-4- Joining \"" + inputTSV1 + "\" with \"" + inputTSV2 + "\" (part 2; left join) ...");
			int leftJoinCounter = 0;
			if (joinType == JoinType.LEFT_JOIN || joinType == JoinType.FULL_JOIN){
				for(Iterator<Map.Entry<String, ArrayList<String[]>>> iter1 = t1Records.entrySet().iterator(); iter1.hasNext();){
					entry1 = iter1.next();
					ArrayList<String[]> joinedValues = new ArrayList<String[]>();
					for (int j=0; j<entry1.getValue().size(); j++){
						//Making records of <entry1Values[j], NULL>             *Note: In fact, NULL is replaced by "".
						String[] arrayOfEmptyStrings = new String[neededTitles2.length]; //neededTitles2.length is the number of fields from t2; in fact, we need this number of NULL values.
						for (int k=0; k<neededTitles2.length; k++)
							arrayOfEmptyStrings[k] = substituteForNullValuesInJoin;
						String[] aResultingRecord = MyUtils.concatTwoStringArrays(entry1.getValue().get(j), arrayOfEmptyStrings);
						joinedValues.add(aResultingRecord);
					}//for j.
					resultingRecords.put(entry1.getKey(), joinedValues);//Adding the joining key, entry1.getKey() to the start of each record, records of <entry1Values[j], entry2Values[k]> will be complete.
					iter1.remove();
					leftJoinCounter++;
				}//for.
			}//if.
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "Number of records added in left join: " + leftJoinCounter);
			//Right join (also part of full join):
			System.out.println(t1Records.size());
			System.out.println(t2Records.size());
			int rightJoinCounter = 0;
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "-5- Joining \"" + inputTSV1 + "\" with \"" + inputTSV2 + "\" (part 3; right join) ...");
			if (joinType == JoinType.RIGHT_JOIN || joinType == JoinType.FULL_JOIN){
				for(Iterator<Map.Entry<String, ArrayList<String[]>>> iter2 = t2Records.entrySet().iterator(); iter2.hasNext();){
					entry2 = iter2.next();
					ArrayList<String[]> joinedValues = new ArrayList<String[]>();
					for (int j=0; j<entry2.getValue().size(); j++){
						//Making records of <NULL, entry1Values[j]>             *Note: In fact, NULL is replaced by "".
						String[] arrayOfEmptyStrings = new String[neededTitles1.length]; //neededTitles1.length is the number of fields from t1; in fact, we need this number of NULL values.
						for (int k=0; k<neededTitles1.length; k++)
							arrayOfEmptyStrings[k] = substituteForNullValuesInJoin;
						String[] aResultingRecord = MyUtils.concatTwoStringArrays(arrayOfEmptyStrings, entry2.getValue().get(j));
						joinedValues.add(aResultingRecord);
					}//for j.
					resultingRecords.put(entry2.getKey(), joinedValues);//Adding the joining key, entry2.getKey() to the start of each record, records of <entry1Values[j], entry2Values[k]> will be complete.
					iter2.remove();
					rightJoinCounter++;
				}//for.
			}//if.
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "Number of records added in right join: " + rightJoinCounter);
			//Finally, saving the results in the output file:
			System.out.println(MyUtils.indent(indentationLevel+2) + writeMessageStep + "-6- Saving the results");
			String titles = t1Key + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + t1NeededFields + Constants.SEPARATOR_FOR_FIELDS_IN_TSV_FILE + t2NeededFields;
			saveTreeMapToTSVFile(outputPath+"\\"+outputTSV, resultingRecords, titles,
					false, showProgressInterval, indentationLevel+3, testOrReal, writeMessageStep+"-6-1");
			
			System.out.println(MyUtils.indent(indentationLevel+1) + "Finished.");
			result.processed=1;
			result.DoneSuccessfully = 1;
		}catch(Exception e){
			e.printStackTrace();
			result.errors = 1;
		}
		if (wrapOutputInLines)
			System.out.println("-----------------------------------");
		return result;
	}//joinTwoTSV().
	//----------------------------------------------------------------------------------------------------------------------------------------
	//----------------------------------------------------------------------------------------------------------------------------------------
	
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//--------------------------------------------------------------------------------------------------------------------------------------------
	//--------------------------------------------------------------------------------------------------------------------------------------------
	public static void main(String[] args) { 
		// TODO Auto-generated method stub

//		//Was not run yet: 
		//		mergeTwoTSVFieldsTogether(Constants.DATASET_DIRECTORY_GH_MongoDB_TSV, "repos.tsv",
		//				Constants.DATASET_DIRECTORY_GH_MongoDB_TSV, "repos-mergedTwoColumns.tsv",
		//				1, 2, "/", 6, 
		//				500000,
		//				Constants.THIS_IS_REAL, 1);

//		//Was not run yet: 
//				mergeTwoTSVFieldsTogether(Constants.DATASET_DIRECTORY_GH_MongoDB_TSV, "issues-Assigned.tsv",
//						Constants.DATASET_DIRECTORY_GH_MongoDB_TSV, "issues-Assigned-mergedTwoColumns.tsv",
//						1, 2, "/", 12, 
//						500000,
//						Constants.THIS_IS_REAL, 1);

//		//Was not run yet: 
//				replaceForeignKeyInTSVWithValueFromAnotherTSV(Constants.DATASET_DIRECTORY_GH_MySQL_TSV, "projects2 - Cleaned.tsv",
//						Constants.DATASET_DIRECTORY_GH_MySQL_TSV, "users3 (only important fields)-fixed.tsv", 
//						Constants.DATASET_DIRECTORY_GH_MySQL_TSV, "projects2-Cleaned-ownerIdReplacedWithLogin.tsv",
//						2, 10,
//						0, 3, 
//						1,
//						"ownerLogin",
//						500000,
//						Constants.THIS_IS_REAL, 1);

	}//main().
}//Class.
