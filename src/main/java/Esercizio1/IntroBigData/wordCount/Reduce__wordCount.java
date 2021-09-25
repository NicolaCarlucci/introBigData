package Esercizio1.IntroBigData.wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce__wordCount extends Reducer<Text, IntWritable, Text, Text>{
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Reducer<Text, IntWritable, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable intWritable : arg1) {
			sum = sum + intWritable.get();
		}
		arg2.write(new Text(arg0.toString()), new Text(String.valueOf(sum)));

	}

}
