package Esercizio1.IntroBigData.esercizio2.part2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import generics.LineMap;
import generics.Separator;
import generics.UtilityLineMap;

public class Map_Es_2_part2 extends Mapper<LongWritable, Text, Text, Text> implements UtilityLineMap{

	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		LineMap lineMap = getLineMap(value.toString(),Separator.SPACE);
		if(lineMap!=null) {
			Text textProduct = new Text(lineMap.getProduct());
			Text textValue = new Text(lineMap.getDataString() + Separator.DOUBLE_POINTS + String.valueOf(lineMap.getIntValue()));
			//example acqua 1/2015:62 
			context.write(textProduct, textValue);
		}
		
		
	}

	@Override
	public LineMap getLineMap(String lineString, String separatorString) {
		LineMap lineMap = new LineMap();
		try {
			// 20015-01   acqua 62 -> 20015-01 acqua 62 
			lineString = removeTabSpace(lineString);
			String[] lineStringArray = lineString.split(separatorString);
			lineMap.setProduct(lineStringArray[1]);
			lineMap.setDataString(reformatDate(lineStringArray[0], Separator.DASH));
			lineMap.setIntValue(Integer.parseInt(lineStringArray[2]));
		}catch(Exception exception) {
			exception.printStackTrace();
			lineMap = null;
		}
		return lineMap;
	}

	/**
	 * Remove tab space from string and replace with space
	 * 
	 * @param lineString is string from remove tab
	 * @return string another tab space
	 */
	private String removeTabSpace(String lineString) {
		String newLineString = "";
		String separatorTabString = Separator.TAB;
		if(lineString.contains(separatorTabString)) {
			newLineString = lineString.replace(separatorTabString, Separator.SPACE);
		}
		return newLineString;
	}
	
	/**
	 * reformat String from 2015-01 to 1/2015
	 * 
	 * @param dateString
	 * @param separatorString
	 * @return
	 * @throws Exception
	 */
	private String reformatDate(String dateString, String separatorString) throws Exception{
		String[] dateStringArray = dateString.split(separatorString);
		return dateStringArray[1]+ Separator.SLASH + dateStringArray[0];
	}
}
