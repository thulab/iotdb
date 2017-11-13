package measurement;

import java.util.Date;
import java.util.Random;

import distribution.ClampedRandomWalkDistribution;
import distribution.IDistribution;
import distribution.MonotonicRandomWalkDistribution;
import distribution.NormalDistribution;

public class DiskIOMeasurement implements ISimulatedMeasurement {
	
	private static String DiskIOByteString = "diskio";// heap optimization
	private static String SerialByteString = "serial";
	private static String[] DiskIOFieldKeys = {
		"reads",
		"writes",
		"read_bytes",
		"write_bytes",
		"read_time",
		"write_time",
		"io_time"
	};
	private Date timestamp;
	private IDistribution[] distributions;
	private String serial;
	
	public DiskIOMeasurement(Date start, Random rand){
		timestamp = start;
		distributions = new IDistribution[7];
		serial = String.format("%03d-%03d-%03d", rand.nextInt(1000), rand.nextInt(1000), rand.nextInt(1000));
		distributions[0] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(50, 1, rand), 0);
		distributions[1] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(50, 1, rand), 0);
		distributions[2] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(100, 1, rand), 0);
		distributions[3] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(100, 1, rand), 0);
		distributions[4] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(5, 1, rand), 0);
		distributions[5] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(5, 1, rand), 0);
		distributions[6] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(5, 1, rand), 0);
	}
	
	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime());  // Add(d);
		for(int i = 0; i < DiskIOFieldKeys.length; i++){
			distributions[i].Advance();
		}
	}
	
	public void ToPoint(Point p) {
		p.setMeasurementName(DiskIOByteString);
		p.setTimestamp(timestamp);
		for(int i = 0; i < DiskIOFieldKeys.length; i++){
			p.appendField(DiskIOFieldKeys[i], (long)distributions[i].Get());
		}
	}
}
