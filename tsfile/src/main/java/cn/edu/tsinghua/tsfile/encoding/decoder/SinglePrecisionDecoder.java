package cn.edu.tsinghua.tsfile.encoding.decoder;

import java.io.IOException;
import java.nio.ByteBuffer;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;

/**
 * Decoder for value value using gorilla
 */
public class SinglePrecisionDecoder extends GorillaDecoder {
	private static final Logger LOGGER = LoggerFactory.getLogger(SinglePrecisionDecoder.class);
	private int preValue;

	public SinglePrecisionDecoder() {
	}

	@Override
	public float readFloat(ByteBuffer buffer) {
		if (!flag) {
			flag = true;
			try {
				int ch1 = ReadWriteIOUtils.read(buffer);
				int ch2 = ReadWriteIOUtils.read(buffer);
				int ch3 = ReadWriteIOUtils.read(buffer);
				int ch4 = ReadWriteIOUtils.read(buffer);
				preValue = ch1 + (ch2 << 8) + (ch3 << 16) + (ch4 << 24);
				leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
				tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
				float tmp = Float.intBitsToFloat(preValue);
				fillBuffer(buffer);
				getNextValue(buffer);
				return tmp;
			} catch (IOException e) {
				LOGGER.error("SinglePrecisionDecoder cannot read first float number because: {}", e.getMessage());
			}
		} else {
			try {
				float tmp = Float.intBitsToFloat(preValue);
				getNextValue(buffer);
				return tmp;
			} catch (IOException e) {
				LOGGER.error("SinglePrecisionDecoder cannot read following float number because: {}", e.getMessage());
			}
		}
		return Float.NaN;
	}

	/**
	 * check whether there is any value to encode left
	 * 
	 * @param buffer stream to read
	 * @throws IOException cannot read from stream
	 */
	private void getNextValue(ByteBuffer buffer) throws IOException {
		nextFlag1 = readBit(buffer);
		// case: '0'
		if (!nextFlag1) {
			return;
		}
		nextFlag2 = readBit(buffer);

		if (!nextFlag2) {
			// case: '10'
			int tmp = 0;
			for (int i = 0; i < TSFileConfig.FLOAT_LENGTH - leadingZeroNum - tailingZeroNum; i++) {
				int bit = readBit(buffer) ? 1 : 0;
				tmp |= bit << (TSFileConfig.FLOAT_LENGTH - 1 - leadingZeroNum - i);
			}
			tmp ^= preValue;
			preValue = tmp;
		} else {
			// case: '11'
			int leadingZeroNumTmp = readIntFromStream(buffer, TSFileConfig.FLAOT_LEADING_ZERO_LENGTH);
			int lenTmp = readIntFromStream(buffer, TSFileConfig.FLOAT_VALUE_LENGTH);
			int tmp = readIntFromStream(buffer, lenTmp);
			tmp <<= (TSFileConfig.FLOAT_LENGTH - leadingZeroNumTmp - lenTmp);
			tmp ^= preValue;
			preValue = tmp;
		}
		leadingZeroNum = Integer.numberOfLeadingZeros(preValue);
		tailingZeroNum = Integer.numberOfTrailingZeros(preValue);
		if(Float.isNaN(Float.intBitsToFloat(preValue))){
			isEnd = true;
		}
	}

	@Override
	public void reset() {
		super.reset();
	}
}
