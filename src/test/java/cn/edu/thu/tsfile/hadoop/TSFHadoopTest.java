package cn.edu.thu.tsfile.hadoop;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.timeseries.FileFormat.TsFile;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;

public class TSFHadoopTest {

	private TSFInputFormat inputformat = null;

	private String tsfilePath = "tsfile";

	@Before
	public void setUp() throws Exception {

		TsFileTestHelper.deleteTsFile(tsfilePath);
		inputformat = new TSFInputFormat();

	}

	@After
	public void tearDown() throws Exception {

		TsFileTestHelper.deleteTsFile(tsfilePath);
	}

	@Test
	public void staticMethodTest() {
		Job job = null;
		try {
			job = Job.getInstance();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		//
		// columns
		//
		String[] value = { "s1", "s2", "s3" };
		try {
			TSFInputFormat.setReadColumns(job, value);
			String[] getValue = TSFInputFormat.getReadColumns(job.getConfiguration());
			assertArrayEquals(value, getValue);
		} catch (TSFHadoopException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		//
		// deviceid
		//
		TSFInputFormat.setReadDeviceId(job, true);
		assertEquals(true, TSFInputFormat.getReadDeviceId(job.getConfiguration()));

		//
		// time
		//

		TSFInputFormat.setReadTime(job, true);
		assertEquals(true, TSFInputFormat.getReadTime(job.getConfiguration()));

		//
		// filter
		//
		TSFInputFormat.setHasFilter(job, true);
		assertEquals(true, TSFInputFormat.getHasFilter(job.getConfiguration()));

		String filterType = "singleFilter";
		TSFInputFormat.setFilterType(job, filterType);
		assertEquals(filterType, TSFInputFormat.getFilterType(job.getConfiguration()));

		String filterExpr = "s1>100";
		TSFInputFormat.setFilterExp(job, filterExpr);
		assertEquals(filterExpr, TSFInputFormat.getFilterExp(job.getConfiguration()));
	}

	@Test
	public void InputFormatTest() {

		//
		// test getinputsplit method
		//
		TsFileTestHelper.writeTsFile(tsfilePath);
		try {
			Job job = Job.getInstance();
			// set input path to the job
			TSFInputFormat.setInputPaths(job, tsfilePath);
			List<InputSplit> inputSplits = inputformat.getSplits(job);
			TSRandomAccessFileReader reader = new LocalFileInput(tsfilePath);
			TsFile tsFile = new TsFile(reader);
			System.out.println(tsFile.getDeltaObjectRowGroupCount());
			assertEquals(tsFile.getRowGroupPosList().size(), inputSplits.size());
			for (InputSplit inputSplit : inputSplits) {
				System.out.println(inputSplit);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void RecordReaderTest() {
		TsFileTestHelper.writeTsFile(tsfilePath);
		try {
			Job job = Job.getInstance();
			// set input path to the job
			TSFInputFormat.setInputPaths(job, tsfilePath);
			String[] columns = { "s1", "s2", "s3", "s4" };
			TSFInputFormat.setReadColumns(job, columns);
			List<InputSplit> inputSplits = inputformat.getSplits(job);
			TSRandomAccessFileReader reader = new LocalFileInput(tsfilePath);
			TsFile tsFile = new TsFile(reader);
			System.out.println(tsFile.getDeltaObjectRowGroupCount());
			assertEquals(tsFile.getRowGroupPosList().size(), inputSplits.size());
			for (InputSplit inputSplit : inputSplits) {
				System.out.println(inputSplit);
			}
			reader.close();
			// read one split
			TSFRecordReader recordReader = new TSFRecordReader();
			TaskAttemptContextImpl attemptContextImpl = new TaskAttemptContextImpl(job.getConfiguration(),
					new TaskAttemptID());
			recordReader.initialize(inputSplits.get(0), attemptContextImpl);
			while (recordReader.nextKeyValue()) {
				System.out.println(recordReader.getCurrentValue().toStrings());
			}
			recordReader.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (TSFHadoopException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
