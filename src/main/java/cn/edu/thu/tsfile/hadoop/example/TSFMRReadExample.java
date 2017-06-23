package cn.edu.thu.tsfile.hadoop.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.edu.thu.tsfile.hadoop.TSFInputFormat;

/**
 * 
 * select count(*) from tsfiletable group by deltaObjectId<br>
 * 
 * Count the number of each deltaObject from one tsfile.
 * 
 * @author liukun
 *
 */
public class TSFMRReadExample {

	public static class TSMapper extends Mapper<NullWritable, ArrayWritable, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);

		@Override
		protected void map(NullWritable key, ArrayWritable value,
				Mapper<NullWritable, ArrayWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			Text deltaObjectId = (Text) value.get()[1];
			context.write(deltaObjectId, one);
		}
	}

	public static class TSReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> iterator,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable intWritable : iterator) {
				sum = sum + intWritable.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException{

		if (args.length != 3) {
			System.out.println("Please give hdfs url, input path, output path");
			return;
		}
		// like: hfds://ip:port
		String HDFSURL = args[0];
		String inputPath = args[1];
		String outputPaht = args[2];
		// TsFileHelper.writeTsFile(inputPath);
		
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", HDFSURL);
		Job job = Job.getInstance(configuration);
		job.setJobName("TsFile read jar");
		job.setJarByClass(TSFMRReadExample.class);
		// set mapper and reducer
		job.setMapperClass(TSMapper.class);
		job.setReducerClass(TSReducer.class);
		// set inputformat and outputformat
		job.setInputFormatClass(TSFInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// set mapper output key and value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// set reducer output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// set input file path
		TSFInputFormat.setInputPaths(job, new Path(inputPath));
		// set output file path
		TextOutputFormat.setOutputPath(job, new Path(outputPaht));
		/**
		 * set configuration for TSFInputFormat
		 */
		TSFInputFormat.setReadTime(job, true); // read time data
		TSFInputFormat.setReadDeltaObjectId(job, true); // read deltaObjectId
		// TSFInputFormat.setReadColumns(job, value); // set read columns
		boolean isSuccess = false;
		try {
			isSuccess = job.waitForCompletion(true);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (isSuccess) {
			System.out.println("Execute successfully");
		} else {
			System.out.println("Execute unsuccessfully");
		}
	}
}
