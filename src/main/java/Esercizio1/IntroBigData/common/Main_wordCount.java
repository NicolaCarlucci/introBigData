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

import Esercizio1.IntroBigData.wordCount.Map__wordCount;
import Esercizio1.IntroBigData.wordCount.Reduce__wordCount;
import generics.Utility;

public class Main_wordCount extends Configured implements Tool{

	public static void main(String[] args) {

		int res_job1;
		try {
			System.out.println("Start job word count --------->");
			res_job1 = ToolRunner.run(new Configuration(), new Main_wordCount(), args);
			System.exit(res_job1);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("<----------------End job word count --------->");
		}

	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Job job;
		Configuration configuration = new Configuration();
		String tempName = Utility.getMathRandom();
		try {
			job = new Job(configuration, "job word count");
			job.setJarByClass(Main_wordCount.class);

			job.setMapperClass(Map__wordCount.class);
			job.setReducerClass(Reduce__wordCount.class);
			// math random name file

			System.out.println("File name is : wordcount_" + tempName + ".txt");

			FileInputFormat.addInputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/input/wordcount.txt"));
			FileOutputFormat.setOutputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/output/WordCount/"
							+ tempName + "/wordcount" + tempName + ".txt"));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			if (job.waitForCompletion(true)) {
			  return 0;	

			} else {
				// error job 
				System.out.println("error job word count");
				return -1;
			}

		} catch (Exception exception) {
			exception.printStackTrace();
			return -1;
		} finally {
			System.out.println("File name is : wordcount_ "+ tempName + ".txt");
		}
	}

	
}

