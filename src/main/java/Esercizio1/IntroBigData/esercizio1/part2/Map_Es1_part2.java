package Esercizio1.IntroBigData.esercizio1.part2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import generics.LineMap;
import generics.Separator;
import generics.UtilityLineMap;

/**
 * Map class job1 part 2
 * 
 * @author Nicola Carlucci
 *
 */
public class Map_Es1_part2 extends Mapper<LongWritable, Text, Text, Text> implements UtilityLineMap {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		LineMap lineMap = getLineMap(value.toString(), Separator.SPACE);
		Text dateText = new Text(lineMap.getDataString());
		Text valueText = new Text(lineMap.getValue().toString());
		context.write(dateText, valueText);

	}

	/**
	 * Get lineMap object
	 */
	@Override
	public LineMap getLineMap(String lineString, String separatorString) {
		LineMap lineMap = new LineMap();
		try {
			lineString = removeSpaceWhiteLong(lineString);
			String[] lineStringArray = lineString.split(separatorString);
			// set data
			lineMap.setDataString(lineStringArray[0]);

			// set value
			List<String> stringList = new ArrayList<String>();
			stringList.add(lineStringArray[1] + Separator.SPACE + lineStringArray[2]);
			lineMap.setValue(stringList);
		} catch (Exception exception) {
			exception.printStackTrace();
			lineMap = null;
		}
		return lineMap;
	}

	/**
	 * Remove tab space from lineString
	 * 
	 * @param lineString
	 * @return
	 */
	private String removeSpaceWhiteLong(String lineString) {
		String newLineString = "";
		if (lineString.contains("\t")) {
			newLineString = lineString.replace("\t", " ");
		}
		return newLineString;
	}

}
