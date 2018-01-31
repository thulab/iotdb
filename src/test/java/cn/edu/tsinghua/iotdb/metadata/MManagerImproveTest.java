package cn.edu.tsinghua.iotdb.metadata;

import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MManagerImproveTest {

    private static MManager mManager = null;
    private final int timeseriesNum = 10;

    @Before
    public void setUp() throws Exception {
        mManager = MManager.getInstance();
        for(int i = 0;i < 10;i++){
            for(int j = 0;j < 10;j++){
                mManager.setStorageLevelToMTree("root.t" + i + ".v" + j);
            }
        }

        for(int i = 0;i < 10;i++){
            for(int j = 0;j < 10;j++){
                for(int k = 0;k < 10;k++){
                    for(int l = 0;l < timeseriesNum;l++){
                        String p = new StringBuilder().append("root.t").append(i).append(".v").append(j).append(".d").append(k).append(".s").append(l).toString();
                        mManager.addPathToMTree(p, "TEXT", "RLE", new String[0]);
                    }
                }
            }
        }

        mManager.flushObjectToFile();
    }

    @After
    public void after() throws IOException {
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void checkSetUp(){
        mManager = MManager.getInstance();

        assertEquals(true, mManager.pathExist("root.t0.v0.d0.s0"));
        assertEquals(true, mManager.pathExist("root.t9.v9.d9.s" + (timeseriesNum-1)));
        assertEquals(false, mManager.pathExist("root.t10"));
    }

    @Test
    public void analyseTimeCost() throws PathErrorException, ProcessorException {
        mManager = MManager.getInstance();

        long startTime, endTime;
        long string_combine, path_exist, list_init, check_filelevel, get_seriestype;
        string_combine = path_exist = list_init = check_filelevel = get_seriestype = 0;

        String deltaObject = "root.t1.v2.d3";
        String measurement = "s5";

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            String path = deltaObject + "." + measurement;
        }
        endTime = System.currentTimeMillis();
        string_combine += endTime - startTime;
        String path = deltaObject + "." + measurement;

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            assertEquals(true, mManager.pathExist(path));
        }
        endTime = System.currentTimeMillis();
        path_exist += endTime - startTime;

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            List<Path> paths = new ArrayList<>();
            paths.add(new Path(path));
        }
        endTime = System.currentTimeMillis();
        list_init += endTime - startTime;
        List<Path> paths = new ArrayList<>();
        paths.add(new Path(path));

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            assertEquals(true, mManager.checkFileLevel(paths));
        }
        endTime = System.currentTimeMillis();
        check_filelevel += endTime - startTime;

        startTime = System.currentTimeMillis();
        for(int i = 0;i < 100000;i++) {
            TSDataType dataType = mManager.getSeriesType(path);
            assertEquals(TSDataType.TEXT, dataType);
        }
        endTime = System.currentTimeMillis();
        get_seriestype += endTime - startTime;

        System.out.println("string combine:\t" + string_combine);
        System.out.println("path exist:\t" + path_exist);
        System.out.println("list init:\t" + list_init);
        System.out.println("check file level:\t" + check_filelevel);
        System.out.println("get series type:\t" + get_seriestype);
    }

    public void doOriginTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        for(String measurement : measurementList){
            String path = deltaObject + "." + measurement;
            assertEquals(true, mManager.pathExist(path));
            List<Path> paths = new ArrayList<>();
            paths.add(new Path(path));
            assertEquals(true, mManager.checkFileLevel(paths));
            TSDataType dataType = mManager.getSeriesType(path);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doPathLoopOnceTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        for(String measurement : measurementList){
            String path = deltaObject + "." + measurement;
            List<Path> paths = new ArrayList<>();
            paths.add(new Path(path));
            assertEquals(true, mManager.checkFileLevel(paths));
            TSDataType dataType = mManager.getSeriesTypeWithCheck(path);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doDealDeltaObjectOnceTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        boolean isFileLevelChecked;
        List<Path> tempList = new ArrayList<>();
        tempList.add(new Path(deltaObject));
        try{
            isFileLevelChecked = mManager.checkFileLevel(tempList);
        } catch (PathErrorException e){
            isFileLevelChecked = false;
        }
        MNode node = mManager.getNodeByPath(deltaObject);

        for(String measurement : measurementList){
            assertEquals(true, mManager.pathExist(node, measurement));
            List<Path> paths = new ArrayList<>();
            paths.add(new Path(measurement));
            if(!isFileLevelChecked)isFileLevelChecked = mManager.checkFileLevel(node, paths);
            assertEquals(true, isFileLevelChecked);
            TSDataType dataType = mManager.getSeriesType(node, measurement);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doRemoveListTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        for(String measurement : measurementList){
            String path = deltaObject + "." + measurement;
            assertEquals(true, mManager.pathExist(path));
            assertEquals(true, mManager.checkFileLevel(path));
            TSDataType dataType = mManager.getSeriesType(path);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    public void doAllImproveTest(String deltaObject, List<String> measurementList)
            throws PathErrorException, ProcessorException {
        boolean isFileLevelChecked;
        try{
            isFileLevelChecked = mManager.checkFileLevel(deltaObject);
        } catch (PathErrorException e){
            isFileLevelChecked = false;
        }
        MNode node = mManager.getNodeByPathWithCheck(deltaObject);

        for(String measurement : measurementList){
            if(!isFileLevelChecked)isFileLevelChecked = mManager.checkFileLevelWithCheck(node, measurement);
            assertEquals(true, isFileLevelChecked);
            TSDataType dataType = mManager.getSeriesTypeWithCheck(node, measurement);
            assertEquals(TSDataType.TEXT, dataType);
        }
    }

    @Test
    public void improveTest() throws PathErrorException, ProcessorException {
        mManager = MManager.getInstance();

        long startTime, endTime;
        String[] deltaObjectList = new String[100];
        for(int i = 0;i < 10;i++){
            for(int j = 0;j < 10;j++){
                deltaObjectList[i*10+j] = "root.t1.v" + i + ".d" + j;
            }
        }
        List<String> measurementList = new ArrayList<>();
        for(int i = 0;i < timeseriesNum;i++){
            measurementList.add("s" + i);
        }

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doOriginTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("origin:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doPathLoopOnceTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("path loop once:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doDealDeltaObjectOnceTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("deal deltaObject once:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doRemoveListTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("remove list:\t" + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for(String deltaObject : deltaObjectList) {
            doAllImproveTest(deltaObject, measurementList);
        }
        endTime = System.currentTimeMillis();
        System.out.println("improve all:\t" + (endTime - startTime));
    }

//    @After
//    public void tearDown() throws Exception {
//        EnvironmentUtils.cleanEnv();
//    }

    @org.junit.Test
    public void test() {

//		try {
//			// test file name
//			List<String> fileNames = mManager.getAllFileNames();
//			assertEquals(2, fileNames.size());
//			if (fileNames.get(0).equals("root.vehicle.d0")) {
//				assertEquals(fileNames.get(1), "root.vehicle.d1");
//			} else {
//				assertEquals(fileNames.get(1), "root.vehicle.d0");
//			}
//			// test filename by path
//			assertEquals("root.vehicle.d0", mManager.getFileNameByPath("root.vehicle.d0.s1"));
//			HashMap<String, ArrayList<String>> map = mManager.getAllPathGroupByFileName("root.vehicle.d1.*");
//			assertEquals(1, map.keySet().size());
//			assertEquals(6, map.get("root.vehicle.d1").size());
//			ArrayList<String> paths = mManager.getPaths("root.vehicle.d0");
//			assertEquals(6, paths.size());
//			paths = mManager.getPaths("root.vehicle.d2");
//			assertEquals(0, paths.size());
//		} catch (PathErrorException e) {
//			e.printStackTrace();
//			fail(e.getMessage());
//		}
    }

}
