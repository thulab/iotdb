package cn.edu.tsinghua.iotdb.engine.cache;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;

import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadata;
import cn.edu.tsinghua.tsfile.read.reader.DefaultTsFileInput;
import cn.edu.tsinghua.tsfile.read.reader.TsFileInput;
import cn.edu.tsinghua.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;


/**
 * This class is used to read metadata(<code>TsFileMetaData</code> and
 * <code>TsRowGroupBlockMetaData</code>).
 * 
 * @author liukun
 *
 */
public class TsFileMetadataUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetadataUtils.class);
	private static final int FOOTER_LENGTH = 4;
	private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;

	public static TsFileMetaData getTsFileMetaData(String filePath) throws IOException {
		TsFileInput tsFileInput = null;
		try {
			tsFileInput = new DefaultTsFileInput(Paths.get(filePath));
			long l = tsFileInput.size();
			tsFileInput.position(l - MAGIC_LENGTH - FOOTER_LENGTH);
			int fileMetaDataLength = tsFileInput.readInt();
			tsFileInput.position(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);
			byte[] buf = new byte[fileMetaDataLength];
			tsFileInput.read(buf, 0, buf.length);
			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			TsFileMetaData fileMetaData = TsFileMetaData.deserializeFrom(bais);
			return fileMetaData;
		} catch (FileNotFoundException e) {
			LOGGER.error("Can't open the tsfile {}, {}", filePath, e.getMessage());
			throw new IOException(e);
		} catch (IOException e) {
			LOGGER.error("Read the tsfile {} error, {}", filePath, e.getMessage());
			throw e;
		} finally {
			if (tsFileInput != null) {
				try {
					tsFileInput.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static TsDeviceMetadata getTsRowGroupBlockMetaData(String filePath, String deltaObjectId,
															  TsFileMetaData fileMetaData) throws IOException {
		if (!fileMetaData.getDeviceMap().containsKey(deltaObjectId)) {
			return null;
		} else {
			TsFileInput tsFileInput = null;
			try {
				tsFileInput = new DefaultTsFileInput(Paths.get(filePath));
				long offset = fileMetaData.getDeviceMap().get(deltaObjectId).getOffset();
				tsFileInput.position(offset);
				int size = fileMetaData.getDeviceMap().get(deltaObjectId).getLen();
				byte[] buf = new byte[size];
				tsFileInput.read(buf, 0, buf.length);
				ByteArrayInputStream bais = new ByteArrayInputStream(buf);
				TsDeviceMetadata tsDeviceMetadata = TsDeviceMetadata.deserializeFrom(bais);
				return tsDeviceMetadata;
			} catch (FileNotFoundException e) {
				LOGGER.error("Can't open the tsfile {}, {}", filePath, e.getMessage());
				throw new IOException(e);
			} catch (IOException e) {
				LOGGER.error("Read the tsfile {} error, {}", filePath, e.getMessage());
				throw e;
			} finally {
				if (tsFileInput != null) {
					try {
						tsFileInput.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}
