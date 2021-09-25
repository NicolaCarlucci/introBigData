package Esercizio1.IntroBigData.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Esercizio1.IntroBigData.esercizio3.Comb_Es_3;
import Esercizio1.IntroBigData.esercizio3.Map_Es_3;
import Esercizio1.IntroBigData.esercizio3.Reduce_Es_3;
import generics.Utility;

/**
 * Main job3
 * 
 * @author Nicola Carlucci
 *
 */
public class Main_Es_3 extends Configured implements Tool {

	public static void main(String[] args) {

		int res_es3;
		try {
			System.out.println("Start job 3 --------->");
			res_es3 = ToolRunner.run(new Configuration(), new Main_Es_3(), args);
			System.exit(res_es3);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("<----------------End job 3 --------->");
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job;
		Configuration configuration = new Configuration();
		String tempName = Utility.getMathRandom();
		try {
			job = new Job(configuration, "job3");
			job.setJarByClass(Main_Es_3.class);

			job.setMapperClass(Map_Es_3.class);
			job.setCombinerClass(Comb_Es_3.class);
			job.setReducerClass(Reduce_Es_3.class);
			// math random name file

			System.out.println("File name is : Es_3_" + tempName + ".txt");
			FileInputFormat.addInputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/input/food.txt"));
			FileOutputFormat.setOutputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/output/Esercizio3/"
							+ tempName + "/Es_3_" + tempName + ".txt"));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			if (job.waitForCompletion(true)) {
				return 0;
			} else {
				// error job 1
				System.out.println("error job 1");
				return -1;
			}

		} catch (Exception exception) {
			exception.printStackTrace();
			return -1;
		} finally {
			System.out.println("File name is : Es_3_" + tempName + ".txt");
		}
	}
}
