package cn.edu.tsinghua.tsfile;

import cn.edu.tsinghua.tsfile.timeseries.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.QueryFilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.basis.ReadOnlyTsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryExpression;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The class is to show how to read TsFile file named "test.tsfile".
 * The TsFile file "test.tsfile" is generated from class TsFileWrite2 or class TsFileWrite,
 * they generate the same TsFile file by two different ways
 * <p>
 * Run TsFileWrite1 or TsFileWrite to generate the test.tsfile first
 */
public class TsFileRead {

	public static void main(String[] args) throws IOException {

		// file path
		String path = "test.tsfile";

		// read example : no filter
		TsFileSequenceReader reader = new TsFileSequenceReader(path);
		ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
		ArrayList<Path> paths = new ArrayList<>();
//
		// time filter : 4 <= time <= 10, value filter : device_1.sensor_3 >= 20


		reader = new TsFileSequenceReader(path);
		readTsFile = new ReadOnlyTsFile(reader);
		paths = new ArrayList<>();
		paths.add(new Path("device_1.sensor_1"));
		paths.add(new Path("device_1.sensor_2"));

//        QueryFilter timeFilter = QueryFilterFactory.or(new GlobalTimeFilter(TimeFilter.gtEq(10000L)),
//                new GlobalTimeFilter(TimeFilter.ltEq(3000L)));
		QueryFilter valueFilter = new SeriesFilter<>(new Path("device_1.sensor_1"), ValueFilter.gtEq(5L));
//        QueryFilter finalFilter = QueryFilterFactory.and(new GlobalTimeFilter(TimeFilter.gtEq(5L)), valueFilter);
		QueryExpression queryExpression = QueryExpression.create(paths, valueFilter);

		long start = System.currentTimeMillis();
		QueryDataSet queryDataSet = readTsFile.query(queryExpression);
		long i = 0;
		while (queryDataSet.hasNext()) {
			i++;
//            if ((i % 10) == 0) {
			System.out.println(queryDataSet.next());
//                continue;
//            }
//            queryDataSet.next();
//			return;
		}
		start = System.currentTimeMillis() - start;
		System.out.println(i + " time cost: " + start + "ms");


//		queryExpression = QueryExpression.create(paths, null);
//
//		start = System.currentTimeMillis();
//		queryDataSet = readTsFile.query(queryExpression);
//		i =0;
//		while (queryDataSet.hasNext()) {
//			i++;
//			queryDataSet.next();
//		}
//		start = System.currentTimeMillis() - start;
//		System.out.println(i + "time cost: " + start + "ms");
//		reader.close();
	}

}