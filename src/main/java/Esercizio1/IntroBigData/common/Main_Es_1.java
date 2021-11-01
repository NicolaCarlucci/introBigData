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

import Esercizio1.IntroBigData.esercizio1.part1.Comb_Es_1;
import Esercizio1.IntroBigData.esercizio1.part1.Map_Es_1;
import Esercizio1.IntroBigData.esercizio1.part1.Reduce_Es_1;
import Esercizio1.IntroBigData.esercizio1.part2.Map_Es1_part2;
import Esercizio1.IntroBigData.esercizio1.part2.Reduce_Es1_part2;
import generics.Utility;

/**
 * Main Job1
 * 
 * @author Nicola Carlucci
 *
 */
public class Main_Es_1 extends Configured implements Tool {

	public static void main(String[] args) {

		int res_job1;
		try {
			System.out.println("Start job 1 --------->");
			res_job1 = ToolRunner.run(new Configuration(), new Main_Es_1(), args);
			System.exit(res_job1);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("<----------------End job 1 --------->");
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job;
		Configuration configuration = new Configuration();
		String tempName = Utility.getMathRandom();
		try {
			job = new Job(configuration, "job1_part1");
			job.setJarByClass(Main_Es_1.class);

			job.setMapperClass(Map_Es_1.class);
			job.setCombinerClass(Comb_Es_1.class);
			job.setReducerClass(Reduce_Es_1.class);
			// math random name file

			System.out.println("File name is : Es_1_" + tempName + ".txt");

			FileInputFormat.addInputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/input/food.txt"));
			FileOutputFormat.setOutputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/output/Esercizio1/job1/"
							+ tempName + "/Es_1_output_temp_" + tempName + ".txt"));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			if (job.waitForCompletion(true)) {
				Job job2 = new Job(configuration, "job1_part2");

				FileInputFormat.setInputPaths(job2,
						new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/output/Esercizio1/job1/" + tempName
								+ "/Es_1_output_temp_" + tempName + ".txt"));
				FileOutputFormat.setOutputPath(job2,
						new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/output/Esercizio1/job1/"
								+ tempName + "/Es_1_output_" + tempName + ".txt"));

				// job2.setJarByClass(Job2Main.class);

				job2.setMapperClass(Map_Es1_part2.class);
				job2.setReducerClass(Reduce_Es1_part2.class);

				job2.setInputFormatClass(TextInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);

				job2.setMapOutputKeyClass(Text.class);
				job2.setMapOutputValueClass(Text.class);

				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);

				job2.setNumReduceTasks(1);

				if (job2.waitForCompletion(true)) {
					return 0;
				} else {
					// error job 2
					System.out.println("error job 2");
					return -1;
				}

			} else {
				// error job 1
				System.out.println("error job 1");
				return -1;
			}

		} catch (Exception exception) {
			exception.printStackTrace();
			return -1;
		} finally {
			System.out.println("File name is : Es_1_" + tempName + ".txt");
		}
	}

}
