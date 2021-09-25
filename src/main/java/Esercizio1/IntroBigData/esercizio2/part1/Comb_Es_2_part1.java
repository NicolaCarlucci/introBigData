package Esercizio1.IntroBigData.esercizio2.part1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner class job2 part 1
 * 
 * @author Nicola Carlucci
 *
 */
public class Comb_Es_2_part1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {

		int sum_prod = 0;
		try {
			for (IntWritable intWritable : arg1) {
				// add value int to key
				sum_prod = sum_prod + intWritable.get();

				// 2015-05 acqua -> total
			}
		} catch (Exception exception) {
			exception.printStackTrace();
			sum_prod = -1;
		}

		Text textKey = new Text(arg0.toString());
		arg2.write(textKey, new IntWritable(sum_prod));

	}

}
