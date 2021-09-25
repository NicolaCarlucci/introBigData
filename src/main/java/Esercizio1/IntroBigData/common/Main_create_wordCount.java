package Esercizio1.IntroBigData.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Esercizio1.IntroBigData.create.wordCount.Map_create_wordCount;

import generics.Utility;

public class Main_create_wordCount extends Configured implements Tool{

	public static void main(String[] args) {

		int res_job1;
		try {
			System.out.println("Start job create word count --------->");
			res_job1 = ToolRunner.run(new Configuration(), new Main_create_wordCount(), args);
			System.exit(res_job1);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("<----------------End job create word count --------->");
		}

	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Job job;
		Configuration configuration = new Configuration();
		String tempName = Utility.getMathRandom();
		try {
			job = new Job(configuration, "job create word count");
			job.setJarByClass(Main_create_wordCount.class);

			job.setMapperClass(Map_create_wordCount.class);
			//job.setReducerClass(Reduce_create_wordCount.class);
			// math random name file

			System.out.println("File name is : wordcount_create_" + tempName + ".txt");

			FileInputFormat.addInputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/input/food.txt"));
			FileOutputFormat.setOutputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/output/createWordCount/"
							+ tempName + "/wordcount_create_" + tempName + ".txt"));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			if (job.waitForCompletion(true)) {
			  return 0;	

			} else {
				// error job 
				System.out.println("error job create word count");
				return -1;
			}

		} catch (Exception exception) {
			exception.printStackTrace();
			return -1;
		} finally {
			System.out.println("File name is : wordcount_create_" + tempName + ".txt");
		}
	}

	
}
