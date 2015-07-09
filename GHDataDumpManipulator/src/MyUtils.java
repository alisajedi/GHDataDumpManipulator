import java.io.BufferedReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyUtils {
	//----------------------------------------------------------------------------------------------------------------
	public static String moveTheBufferedReaderCursorToTheLineAfter(BufferedReader br, String startingString){
		String s;
		try{
		while( ((s = br.readLine()) != null) && (! s.startsWith(startingString)) ){
			//do nothing.
		} //while.
		return s;
		}catch(Exception e){
			e.printStackTrace();
			return "";
		}
	}
	//----------------------------------------------------------------------------------------------------------------
	public static String indent(int indentationLevel){
		String s = "", ss = "";
		for (int i=0; i<Constants.NUMBER_OF_TAB_CHARACTERS; i++)
			s = s + " ";
		for (int i=0; i<indentationLevel-1; i++)
			ss = ss + s;
		return ss;
	}
	public static void println(String s, int indentationLevel){
		System.out.println(indent(indentationLevel) + s);
	}
	//----------------------------------------------------------------------------------------------------------------
	public static int indexOf_ifExists_LengthIfDoNotExist(String s1, String s2, int startingIndex){
		int result = 0;
		if (s1 != null){
			result = s1.indexOf(s2, startingIndex);
			if (result < 0) //: it means that there is no s2 (",") up to the end of the string, which means that we are at the end of the line and have an extra character (";").
				result = s1.length()-2;
		}
		return result;
	}
	//----------------------------------------------------------------------------------------------------------------
	public static String removeExtraCharactersFromTheEndOfRecord(String tabSeparatedRecord){//Removes ")\t" or ");\t" from the end (if there is).
		if (tabSeparatedRecord.endsWith(")\t"))
			return tabSeparatedRecord.substring(0, tabSeparatedRecord.length()-2);
		else
			if (tabSeparatedRecord.endsWith(");\t"))
				return tabSeparatedRecord.substring(0, tabSeparatedRecord.length()-3);
			else
				return tabSeparatedRecord;
	}
	//----------------------------------------------------------------------------------------------------------------
	public static String removeFromEnd(String s, int num){
		return s.substring(0, s.length()-num);
	}
	//----------------------------------------------------------------------------------------------------------------
	public static String applyRegexOnString(String regex, String value){
		Pattern pattern = Pattern.compile("[^"+regex+"]+");
		Matcher matcher = pattern.matcher(value);
		if (matcher.find())
			value = value.replaceAll("[^"+regex+"]+", " ");
		if (value.equals(""))
			value = " ";
		return value;
	}
	//----------------------------------------------------------------------------------------------------------------
}
