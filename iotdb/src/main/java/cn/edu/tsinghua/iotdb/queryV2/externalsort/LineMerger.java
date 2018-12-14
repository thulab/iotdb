//package cn.edu.tsinghua.iotdb.queryV2.externalsort;
//
//import cn.edu.tsinghua.iotdb.queryV2.externalsort.serialize.TimeValuePairDeserializer;
//import cn.edu.tsinghua.iotdb.queryV2.externalsort.serialize.TimeValuePairSerializer;
//import cn.edu.tsinghua.iotdb.queryV2.externalsort.serialize.impl.FixLengthTimeValuePairDeserializer;
//import cn.edu.tsinghua.iotdb.queryV2.externalsort.serialize.impl.FixLengthTimeValuePairSerializer;
//import cn.edu.tsinghua.iotdb.queryV2.reader.merge.PriorityMergeReader;
//import cn.edu.tsinghua.iotdb.queryV2.reader.merge.PrioritySeriesReader;
//
//import java.io.IOException;
//import java.util.List;
//
//
//public class LineMerger {
//
//    private String tmpFilePath;
//
//    public LineMerger(String tmpFilePath) {
//        this.tmpFilePath = tmpFilePath;
//    }
//
//    public PrioritySeriesReader merge(List<PrioritySeriesReader> prioritySeriesReaders) throws IOException {
//        TimeValuePairSerializer serializer = new FixLengthTimeValuePairSerializer(tmpFilePath);
//        PriorityMergeReader reader = new PriorityMergeReader(prioritySeriesReaders);
//        while (reader.hasNext()) {
//            serializer.write(reader.next());
//        }
//        reader.close();
//        serializer.close();
//        TimeValuePairDeserializer deserializer = new FixLengthTimeValuePairDeserializer(tmpFilePath);
//        return new PrioritySeriesReader(deserializer, prioritySeriesReaders.get(0).getPriority());
//    }
//}
