package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.*;
import java.util.Collection;

import static org.junit.Assert.*;


public class OFSeriesListMetadataTest {


    private final String path = "OFSeriesListMetadataTest";


    @Before
    public void setUp() throws Exception {


    }

    @After
    public void tearDown() throws Exception {
        File file = new File(path);
        if(file.exists()){
            file.delete();
        }
    }


    @Test
    public void testOfSeriesListMetadataSerDe() throws Exception {
        OFSeriesListMetadata ofSeriesListMetadata = TestHelper.createOFSeriesListMetadata();
        serialized(ofSeriesListMetadata);
        OFSeriesListMetadata deOfSeriesListMetadata = deSerialized();
        // assert
        Utils.isOFSeriesListMetadataEqual(ofSeriesListMetadata,deOfSeriesListMetadata);
    }

    private void serialized(OFSeriesListMetadata obj) throws FileNotFoundException {
            FileOutputStream fileOutputStream = new FileOutputStream(path);
            try {
                obj.serializeTo(fileOutputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
    }

    private OFSeriesListMetadata deSerialized() throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(path);
        try {
            OFSeriesListMetadata ofSeriesListMetadata = OFSeriesListMetadata.deserializeFrom(fileInputStream);
            return ofSeriesListMetadata;
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                fileInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}