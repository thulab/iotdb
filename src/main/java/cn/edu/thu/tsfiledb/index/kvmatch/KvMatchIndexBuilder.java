package cn.edu.thu.tsfiledb.index.kvmatch;

import java.nio.file.Path;

/**
 * This is the class actually build the KV-match index for specific data series.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexBuilder {

    private Path file;

    public KvMatchIndexBuilder(Path file) {
        this.file = file;
    }

    public boolean build() {
        // Step 1: scan data and extract window features

        // Step 2: make up index structure

        // Step 3: store to disk

        return false;
    }
}
