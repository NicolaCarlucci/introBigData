package Esercizio1.IntroBigData.esercizio1.part1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

/**
 * Combiner class job1 part 1
 * 
 * @author Nicola Carlucci
 *
 */
public class Comb_Es_1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	private final static IntWritable TOTAL = new IntWritable();

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {

		int tot = 0;
		try {
			for (IntWritable intWritable : arg1) {
				tot = tot + intWritable.get();
				// added 1 to element
				// example i = 0 2015-01- acqua 1
				// example i = 1 2015-01- acqua 2
			}
		} catch (Exception exception) {
			tot = -1;
		}
		Text text = new Text();
		text.set(arg0.toString());
		TOTAL.set(tot);
		arg2.write(text, TOTAL);
	}

}
