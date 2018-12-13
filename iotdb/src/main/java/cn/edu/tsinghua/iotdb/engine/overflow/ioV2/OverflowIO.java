package cn.edu.tsinghua.iotdb.engine.overflow.ioV2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.write.io.DefaultTsFileOutput;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFRowGroupListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFSeriesListMetadata;
import cn.edu.tsinghua.iotdb.query.aggregation.AggregationConstant;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class OverflowIO extends TsFileIOWriter {
	private static final Logger LOGGER = LoggerFactory.getLogger(OverflowIO.class);
	private OverflowReadWriter overflowReadWriter;

	public OverflowIO(DefaultTsFileOutput defaultTsFileOutput, boolean isInsert) throws IOException {
		super();
		//OverflowReadWriter.cutOff(filePath, lastUpdatePosition);
		overflowReadWriter = new OverflowReadWriter(filePath);
		if (isInsert) {
			super.setIOWriter(overflowReadWriter);
		}
	}

	private ChunkMetaData startTimeSeries(OverflowSeriesImpl index) throws IOException {
		LOGGER.debug(
				"Start overflow series chunk meatadata: measurementId: {}, valueCount: {}, compressionName: {}, TSdatatype: {}.",
				index.getMeasurementId(), index.getValueCount(), CompressionTypeName.UNCOMPRESSED, index.getDataType());
		TimeSeriesChunkMetaData currentSeries;
		currentSeries = new TimeSeriesChunkMetaData(index.getMeasurementId(), TSChunkType.VALUE, this.getPos(),
				CompressionTypeName.UNCOMPRESSED);
		currentSeries.setNumRows(index.getValueCount());
		byte[] max = index.getStatistics().getMaxBytes();
		byte[] min = index.getStatistics().getMinBytes();
		VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData = new VInTimeSeriesChunkMetaData(index.getDataType());
		Map<String, ByteBuffer> minMaxMap = new HashMap<>();
		minMaxMap.put(AggregationConstant.MIN_VALUE, ByteBuffer.wrap(min));
		minMaxMap.put(AggregationConstant.MAX_VALUE, ByteBuffer.wrap(max));
		TsDigest tsDigest = new TsDigest(minMaxMap);
		vInTimeSeriesChunkMetaData.setDigest(tsDigest);
		currentSeries.setVInTimeSeriesChunkMetaData(vInTimeSeriesChunkMetaData);
		return currentSeries;
	}

	private TimeSeriesChunkMetaData endSeries(long size, TimeSeriesChunkMetaData currentSeries) throws IOException {
		currentSeries.setTotalByteSize(size);
		return currentSeries;
	}

	/**
	 * read one time-series chunk data and wrap these bytes into a input stream.
	 * 
	 * @param chunkMetaData
	 * @return one input stream contains the data of the time-series chunk
	 *         metadata.
	 * @throws IOException
	 */
	public static InputStream readOneTimeSeriesChunk(TimeSeriesChunkMetaData chunkMetaData,
			ITsRandomAccessFileReader fileReader) throws IOException {
		long begin = chunkMetaData.getProperties().getFileOffset();
		long size = chunkMetaData.getTotalByteSize();
		byte[] buff = new byte[(int) size];
		fileReader.seek(begin);
		fileReader.read(buff, 0, (int) size);
		ByteArrayInputStream input = new ByteArrayInputStream(buff);
		return input;
	}

	/**
	 * flush one overflow time-series tree to file, and get the time-series
	 * chunk meta-data.
	 * 
	 * @param index
	 * @return the time-series chunk meta-data corresponding to the overflow
	 *         time-series tree.
	 * @throws IOException
	 */
	public TimeSeriesChunkMetaData flush(OverflowSeriesImpl index) throws IOException {
		long beginPos = this.getPos();
		TimeSeriesChunkMetaData currentSeries = startTimeSeries(index);

		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		index.getOverflowIndex().toBytes(byteStream);
		byteStream.writeTo(overflowReadWriter);
		// TODO: use buff
		// flush();
		int size = (int) (this.getPos() - beginPos);
		currentSeries.setTotalByteSize(size);
		endSeries(size, currentSeries);
		return currentSeries;
	}

	public void clearRowGroupMetadatas() {
		super.rowGroupMetaDatas.clear();
	}

	public List<OFRowGroupListMetadata> flush(Map<String, Map<String, OverflowSeriesImpl>> overflowTrees)
			throws IOException {
		List<OFRowGroupListMetadata> ofRowGroupListMetadatas = new ArrayList<OFRowGroupListMetadata>();
		if (overflowTrees.isEmpty()) {
			return ofRowGroupListMetadatas;
		} else {
			for (String deltaObjectId : overflowTrees.keySet()) {
				Map<String, OverflowSeriesImpl> seriesMap = overflowTrees.get(deltaObjectId);
				OFRowGroupListMetadata rowGroupListMetadata = new OFRowGroupListMetadata(deltaObjectId);
				for (String measurementId : seriesMap.keySet()) {
					TimeSeriesChunkMetaData current = flush(seriesMap.get(measurementId));
					ArrayList<TimeSeriesChunkMetaData> timeSeriesList = new ArrayList<>();
					timeSeriesList.add(current);
					// TODO : optimize the OFSeriesListMetadata
					OFSeriesListMetadata ofSeriesListMetadata = new OFSeriesListMetadata(measurementId, timeSeriesList);
					rowGroupListMetadata.addSeriesListMetaData(ofSeriesListMetadata);
				}
				ofRowGroupListMetadatas.add(rowGroupListMetadata);
			}
		}
		flush();
		return ofRowGroupListMetadatas;
	}

	public void toTail() throws IOException {
		overflowReadWriter.toTail();
	}

	public long getPos() throws IOException {
		return overflowReadWriter.getPosition();
	}

	public void close() throws IOException {
		overflowReadWriter.close();
	}

	public void flush() throws IOException {
		overflowReadWriter.flush();
	}

	public OverflowReadWriter getReader() {
		return overflowReadWriter;
	}

	public OverflowReadWriter getWriter() {
		return overflowReadWriter;
	}

	public static class OverflowReadWriter extends OutputStream
			implements  TsFileOutput {

		private RandomAccessFile raf;
		private static final String RW_MODE = "rw";

		public OverflowReadWriter(String filepath) throws FileNotFoundException {
			this.raf = new RandomAccessFile(filepath, RW_MODE);
		}

		@Override
		public void write(int b) throws IOException {
			raf.write(b);
		}

		@Override
		public void write(byte[] b) throws IOException {
			raf.write(b);
		}

		@Override
		public void write(ByteBuffer b) throws IOException {
			throw new NotImplementedException();
		}

		@Override
		public long getPosition() throws IOException {
			return raf.getFilePointer();
		}

		@Override
		public void close() throws IOException {
			raf.close();
		}

		@Override
		public OutputStream wrapAsStream() throws IOException {
			return this;
		}

		public void toTail() throws IOException {
			long tail = raf.length();
			raf.seek(tail);
		}

		public long getPos() throws IOException {
			return raf.getFilePointer();
		}

		public OutputStream getOutputStream() {
			return this;
		}

		public void seek(long offset) throws IOException {
			raf.seek(offset);
		}

		public int read() throws IOException {
			return raf.read();
		}

		public int read(byte[] b, int off, int len) throws IOException {
			raf.readFully(b, off, len);
			return len;
		}

		public long length() throws IOException {
			return raf.length();
		}

		public int readInt() throws IOException {
			return raf.readInt();
		}

	}
}
