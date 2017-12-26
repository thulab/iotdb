package cn.edu.thu.tsfile.hadoop.example;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.hadoop.TSFHadoopException;
import cn.edu.thu.tsfile.hadoop.TSFOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
			System.out.println("Map:" + deltaObjectId);
			context.write(deltaObjectId, one);
		}
	}

	public static class TSReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable intWritable : values) {
				sum = sum + intWritable.get();
			}
			System.out.println("Reduce:" + key + "\t" + sum);
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, TSFHadoopException, URISyntaxException {

//		if (args.length != 3) {
//			System.out.println("Please give hdfs url, input path, output path");
//			return;
//		}
//		// like: hfds://ip:port
//		String HDFSURL = args[0];
//		String inputPath = args[1];
//		String outputPaht = args[2];
//		// TsFileHelper.writeTsFile(inputPath);

		String HDFSURL = "hdfs://localhost:8020";
		Path inputPath = new Path("/east/tsfile");
//		String inputPath = "/Users/East/tsfile";
		Path outputPath = new Path("/east/output");
		
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
//		job.setOutputFormatClass(TSFOutputFormat.class);
		// set mapper output key and value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// set reducer output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// set input file path
		TSFInputFormat.setInputPaths(job, inputPath);
		// set output file path
		TSFOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(job.getConfiguration()).deleteOnExit(outputPath);

		/**
		 * set configuration for TSFInputFormat
		 */
		TSFInputFormat.setReadTime(job, true); // read time data
		TSFInputFormat.setReadDeltaObjectId(job, true); // read deltaObjectId
		String[] devices = {"root.car.d1"};
//		TSFInputFormat.setReadDevices(job, devices);
		String[] sensors = { "s1", "s2", "s3", "s4"};
//		TSFInputFormat.setReadSensors(job, sensors);
		boolean isSuccess = false;
		try {
			System.out.println("Start to complete job~");
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
