package org.apache.iotdb.db.engine.modification.io;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.junit.Test;

import static org.junit.Assert.*;

public class LocalTextModificationAccessorTest {

  @Test
  public void readMyWrite() {
    String tempFileName = "mod.temp";
    Modification[] modifications = new Modification[] {
            new Deletion("p1", 1, 1),
            new Deletion("p2", 2, 2),
            new Deletion("p3", 3, 3),
            new Deletion("p4", 4, 4),
    };
    try {
      LocalTextModificationAccessor accessor = new LocalTextModificationAccessor(tempFileName);
      for (int i = 0; i < 2; i++) {
        accessor.write(modifications[i]);
      }
      List<Modification> modificationList = (List<Modification>) accessor.read();
      for (int i = 0; i < 2; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }

      for (int i = 2; i < 4; i++) {
        accessor.write(modifications[i]);
      }
      modificationList = (List<Modification>) accessor.read();
      for (int i = 0; i < 4; i++) {
        assertEquals(modifications[i], modificationList.get(i));
      }
      accessor.close();
    } catch (IOException e) {
      fail(e.getMessage());
    } finally {
      new File(tempFileName).delete();
    }
  }

  @Test
  public void readNull() throws IOException {
    String tempFileName = "mod.temp";
    LocalTextModificationAccessor accessor = null;
    try {
      accessor = new LocalTextModificationAccessor(tempFileName);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    new File(tempFileName).delete();
    Collection<Modification> modifications = accessor.read();
    assertNull(modifications);
  }
}