package cn.edu.thu.tsfile.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

public class Main {

	public static void main(String[] args) throws IOException {
		Job job = Job.getInstance();

		TSFInputFormat inputFormat = new TSFInputFormat();
		String filePath = "1479247862728-1497343997566";
		TSFInputFormat.setInputPaths(job, filePath);
		System.out.println(inputFormat.getSplits(job));
		
	}

}
