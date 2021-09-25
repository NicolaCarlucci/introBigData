package Esercizio1.IntroBigData.create.wordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce_create_wordCount extends Reducer<Text, IntWritable, Text, Text>{
	
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Reducer<Text, IntWritable, Text, Text>.Context arg2)
			throws IOException, InterruptedException {

		String product = arg0.toString();
		arg2.write(new Text(product), new Text(""));

	}

}
