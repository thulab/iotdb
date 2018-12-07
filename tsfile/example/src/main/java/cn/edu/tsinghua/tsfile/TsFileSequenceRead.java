package cn.edu.tsinghua.tsfile;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.MetaMarker;
import cn.edu.tsinghua.tsfile.file.footer.ChunkGroupFooter;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.page.PageReader;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TsFileSequenceRead {

    public static void main(String[] args) throws IOException {
        TsFileSequenceReader reader = new TsFileSequenceReader("test.tsfile");
        System.out.println("position: " + reader.getChannel().position());
        System.out.println(reader.readHeadMagic());
        System.out.println(reader.readTailMagic());
        TsFileMetaData metaData = reader.readFileMetadata();
        // Sequential reading of one ChunkGroup now follows this order:
        // first SeriesChunks (headers and data) in one ChunkGroup, then the ChunkGroupFooter
        // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the marker) ahead and
        // judge accordingly.
        byte marker;
        while ((marker = reader.readMarker()) != MetaMarker.Separator) {
            switch (marker) {
                case MetaMarker.ChunkHeader:
                    ChunkHeader header = reader.readChunkHeader();
                    System.out.println("position: " + reader.getChannel().position());
                    System.out.println("chunk: " + header.getMeasurementID());
                    Decoder defaultTimeDecoder = Decoder.getDecoderByType(TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder),
                            TSDataType.INT64);
                    Decoder valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
                    for (int j = 0; j < header.getNumOfPages(); j++) {
                        PageHeader pageHeader = reader.readPageHeader(header.getDataType());
                        System.out.println("position: " + reader.getChannel().position());
                        System.out.println("points in the page: " + pageHeader.getNumOfValues());
                        ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                        System.out.println("position: " + reader.getChannel().position());
                        System.out.println("page data size: " + pageHeader.getUncompressedSize() + "," + pageData.remaining());
                        PageReader reader1 = new PageReader(pageData, header.getDataType(), valueDecoder, defaultTimeDecoder);
                        while (reader1.hasNextBatch()) {
                            BatchData batchData = reader1.nextBatch();
                            while (batchData.hasNext()) {
                                System.out.println("time, value: " + batchData.getTime() + "," + batchData.getValue());
                                batchData.next();
                            }
                        }
                    }
                    break;
                case MetaMarker.ChunkGroupFooter:
                    ChunkGroupFooter chunkGroupFooter = reader.readChunkGroupFooter();
                    System.out.println("position: " + reader.getChannel().position());
                    System.out.println("chunk group: " + chunkGroupFooter.getDeviceID());
                    break;
                default:
                    MetaMarker.handleUnexpectedMarker(marker);
            }
        }
        reader.close();
    }
}
