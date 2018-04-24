package cn.edu.tsinghua.iotdb.engine.tombstone;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LocalTombstoneFileTest {

    @Test
    public void test() throws IOException {
        File file = new File("tempTombstone");
        LocalTombstoneFile tombstoneFile = new LocalTombstoneFile(file.getPath());
        try {
            Tombstone[] tombstoneArray = new Tombstone[] {
                    new Tombstone("d0", "s0", 100, System.currentTimeMillis()),
                    new Tombstone("d0", "s1", 150, System.currentTimeMillis()),
                    new Tombstone("d0", "s2", 200, System.currentTimeMillis()),
                    new Tombstone("d1", "s0", 250, System.currentTimeMillis()),
                    new Tombstone("d1", "s1", 300, System.currentTimeMillis()),
                    new Tombstone("d1", "s2", 350, System.currentTimeMillis())
            };
            List<Tombstone> tombstones = new ArrayList<>();
            tombstones.addAll(Arrays.asList(tombstoneArray));

            for (Tombstone tombstone : tombstones) {
                tombstoneFile.append(tombstone);
            }
            List<Tombstone> tombstoneList = tombstoneFile.getTombstones();
            assertEquals(tombstones.size(), tombstoneList.size());
            for(int i = 0; i < tombstones.size(); i++) {
                assertEquals(tombstones.get(i), tombstoneList.get(i));
            }
            tombstoneFile.close();

            tombstoneFile = new LocalTombstoneFile(file.getPath());
            Tombstone tombstone = new Tombstone("d2", "s0", 5444, System.currentTimeMillis());
            tombstoneFile.append(tombstone);
            tombstones.add(tombstone);
            tombstoneList = tombstoneFile.getTombstones();
            assertEquals(tombstones.size(), tombstoneList.size());
            for(int i = 0; i < tombstones.size(); i++) {
                assertEquals(tombstones.get(i), tombstoneList.get(i));
            }
            tombstoneFile.close();
        }finally {
            file.delete();
        }
    }
}
