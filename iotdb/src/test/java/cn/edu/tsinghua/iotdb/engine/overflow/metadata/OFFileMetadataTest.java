package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;


public class OFFileMetadataTest {


    private String path = "OFFileMetadataTest";

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
    public void testOFFileMetadata() throws Exception {
        OFFileMetadata ofFileMetadata = TestHelper.createOFFileMetadata();
        serialize(ofFileMetadata);
        OFFileMetadata deOFFileMetadata = deSerialize();
        // assert
        Utils.isOFFileMetadataEqual(ofFileMetadata,deOFFileMetadata);
    }

    private void serialize(OFFileMetadata ofFileMetadata) throws FileNotFoundException {
        FileOutputStream outputStream = new FileOutputStream(path);
        try {
            ofFileMetadata.serializeTo(outputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (outputStream!=null){
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private OFFileMetadata deSerialize() throws FileNotFoundException {
        FileInputStream  inputStream = new FileInputStream(path);
        try {
            return OFFileMetadata.deserializeFrom(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(inputStream!=null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}