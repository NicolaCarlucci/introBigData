package ECG.IntroBigData.common;

import java.util.Date;

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

import ECG.IntroBigData.job2.Map_Job2_ECG;
import ECG.IntroBigData.job2.Reduce_Job2_ECG;
import generics.Utility;

public class Main_Job2ECG extends Configured implements Tool{

	public static void main(String[] args) {

		int res_job1;
		try {
			Date startTime = new Date();
			System.out.println("Start job 2 ECG --------->");
			res_job1 = ToolRunner.run(new Configuration(), new Main_Job2ECG(), args);
			Date endTime = new Date();
			long timeDone = (endTime.getTime() - startTime.getTime());
			System.out.println("job 2 ECG done in  ---------> " + timeDone + "seconds");
			System.exit(res_job1);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("<----------------End job 2 ECG--------->");
		}

	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Job job;
		Configuration configuration = new Configuration();
		String tempName = Utility.getMathRandom();
		try {
			job = new Job(configuration, "job2 ECG");
			job.setJarByClass(Main_Job2ECG.class);

			job.setMapperClass(Map_Job2_ECG.class);
			//job.setCombinerClass(Comb_Es_1.class);
			job.setReducerClass(Reduce_Job2_ECG.class);
			// math random name file

			System.out.println("File name is : Job2ECG_" + tempName + ".txt");

			FileInputFormat.addInputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/input/ecg.csv"));
			FileOutputFormat.setOutputPath(job,
					new Path("/home/nikola/Scrivania/workspaceIntroBigData/IntroBigData/src/data/output/ECG/job2/"
							+ tempName + "/Job1ECG_" + tempName + ".txt"));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

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
			System.out.println("File name is : Job2ECG_" + tempName + ".txt");
		}
	}
}
