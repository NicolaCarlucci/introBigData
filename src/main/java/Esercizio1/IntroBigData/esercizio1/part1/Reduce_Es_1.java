package Esercizio1.IntroBigData.esercizio1.part1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import generics.LineMap;
import generics.Separator;
import generics.UtilityLineMap;

/**
 * Reduce class job1 part1
 * 
 * @author nikola
 *
 */
public class Reduce_Es_1 extends Reducer<Text, IntWritable, Text, Text> implements UtilityLineMap {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Reducer<Text, IntWritable, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable intWritable : arg1) {
			sum = sum + intWritable.get();
		}
		LineMap lineMap = getLineMap(arg0.toString(), Separator.SPACE);
		if (lineMap != null) {
			Text textDate = new Text(lineMap.getDataString());
			Text textValue = new Text(lineMap.getValue().get(0) + Separator.SPACE + sum);
			arg2.write(textDate, textValue);

			// 2015-01 acqua 31
			// 2015-01 birra 30
		}

	}

	/**
	 * Create lineMap object from string
	 * 
	 */
	@Override
	public LineMap getLineMap(String lineString, String separatorString) {
		LineMap lineMap = new LineMap();
		try {
			String[] values = lineString.split(separatorString);
			String date = values[0];
			String value = values[1];
			lineMap.setDataString(date);
			List<String> listValue = new ArrayList<>();
			listValue.add(value);
			lineMap.setValue(listValue);
		} catch (Exception exception) {
			exception.printStackTrace();
			lineMap = null;
		}
		return lineMap;
	}

}
