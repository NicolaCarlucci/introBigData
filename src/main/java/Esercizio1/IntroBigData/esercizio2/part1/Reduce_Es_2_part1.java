package Esercizio1.IntroBigData.esercizio2.part1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import generics.LineMap;
import generics.Separator;
import generics.UtilityLineMap;

/**
 * Reduce class job2 part 1
 * 
 * @author nikola
 *
 */
public class Reduce_Es_2_part1 extends Reducer<Text, IntWritable, Text, Text> implements UtilityLineMap {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Reducer<Text, IntWritable, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		// re-check sum
		int total_product = 0;
		for (IntWritable intWritable : arg1) {
			total_product = total_product + intWritable.get();
		}
		LineMap lineMap = getLineMap(arg0.toString(), Separator.SPACE);
		if (lineMap != null) {
			Text textKey = new Text(lineMap.getDataString());
			Text textValue = new Text(lineMap.getProduct() + Separator.SPACE + total_product);
			arg2.write(textKey, textValue);
			// 2015-05 -> acqua 35
		}

	}

	@Override
	public LineMap getLineMap(String lineString, String separatorString) {
		LineMap lineMap = new LineMap();
		try {
			String[] lineStringArray = lineString.split(separatorString);
			lineMap.setDataString(lineStringArray[0]);
			lineMap.setProduct(lineStringArray[1]);
		} catch (Exception exception) {
			exception.printStackTrace();
			lineMap = null;
		}
		return lineMap;
	}
}
