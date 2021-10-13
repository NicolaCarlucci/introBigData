package ECG.IntroBigData.common;

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


import ECG.IntroBigData.job3.Combiner_Job3_ECG;
import ECG.IntroBigData.job3.Map_Job3_ECG;
import ECG.IntroBigData.job3.Reduce_Job3_ECG;
import generics.Utility;

public class Main_Job3ECG extends Configured implements Tool{

	public static void main(String[] args) {

		int res_job1;
		try {
			System.out.println("Start job 3 ECG --------->");
			res_job1 = ToolRunner.run(new Configuration(), new Main_Job3ECG(), args);
			System.exit(res_job1);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("<----------------End job 3 ECG--------->");
		}

	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Job job;
		Configuration configuration = new Configuration();
		String tempName = Utility.getMathRandom();
		try {
			job = new Job(configuration, "job3 ECG");
			job.setJarByClass(Main_Job3ECG.class);

			job.setMapperClass(Map_Job3_ECG.class);
			job.setCombinerClass(Combiner_Job3_ECG.class);
			job.setReducerClass(Reduce_Job3_ECG.class);
			// math random name file

			System.out.println("File name is : Job3ECG_" + tempName + ".txt");

			FileInputFormat.addInputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/input/ecg.csv"));
			FileOutputFormat.setOutputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/output/ECG/job3/"
							+ tempName + "/Job3ECG_" + tempName + ".txt"));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			if(job.waitForCompletion(true))
			return 1;
			else return -1;
			
		} catch (Exception exception) {
			exception.printStackTrace();
			return -1;
		} finally {
			System.out.println("File name is : Job3ECG_" + tempName + ".txt");
		}
	}
}

