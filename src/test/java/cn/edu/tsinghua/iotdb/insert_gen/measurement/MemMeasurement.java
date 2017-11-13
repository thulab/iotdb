package measurement;

import java.util.Date;
import java.util.Random;

import distribution.ClampedRandomWalkDistribution;
import distribution.IDistribution;
import distribution.NormalDistribution;

public class MemMeasurement implements ISimulatedMeasurement {
			
	private static final String MemoryByteString = "mem"; // heap optimization
	// Choices for modeling a host's memory capacity.
	private static final long[] MemoryMaxBytesChoices = {8 << 15, 12 << 15, 16 << 15};
	private static String[] MemoryFieldKeys = {
	"total",
	"available",
	"used",
	"free",
	"cached",
	"buffered",
	"used_percent",
	"available_percent",
	"buffered_percent"
	};

	private Date timestamp;
	private long bytesTotal;
	private IDistribution bytesUsedDist, bytesCachedDist, bytesBufferedDist;

	public MemMeasurement(Date start, Random rand) {
		timestamp = start;
		bytesTotal = MemoryMaxBytesChoices[rand.nextInt(MemoryMaxBytesChoices.length)];
		bytesUsedDist = new ClampedRandomWalkDistribution(
				0.0, (double)bytesTotal, rand.nextDouble() * (double)bytesTotal,
				new NormalDistribution(0.0, 1.0 * bytesTotal / 64, rand));;
		bytesCachedDist = new ClampedRandomWalkDistribution(
				0.0, (double)bytesTotal, rand.nextDouble() * (double)bytesTotal,
				new NormalDistribution(0.0, 1.0 * bytesTotal / 64, rand));;
		bytesBufferedDist = new ClampedRandomWalkDistribution(
				0.0, (double)bytesTotal, rand.nextDouble() * (double)bytesTotal,
				new NormalDistribution(0.0, 1.0 * bytesTotal / 64, rand));;			
	}
	

	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime()); // Add(d);
		bytesUsedDist.Advance();
		bytesCachedDist.Advance();
		bytesBufferedDist.Advance();
	}

	public void ToPoint(Point p) {
		p.setMeasurementName(MemoryByteString);
		p.setTimestamp(timestamp);

		long total = bytesTotal;
		double used = bytesUsedDist.Get();
		double cached = bytesCachedDist.Get();
		double buffered = bytesBufferedDist.Get();

		p.appendField(MemoryFieldKeys[0], total);
		p.appendField(MemoryFieldKeys[1], (int)(Math.floor((double)total-used)));
		p.appendField(MemoryFieldKeys[2], (int)(Math.floor(used)));
		p.appendField(MemoryFieldKeys[3], (int)(Math.floor(cached)));
		p.appendField(MemoryFieldKeys[4], (int)(Math.floor(buffered)));
		p.appendField(MemoryFieldKeys[5], (int)(Math.floor(used)));
		p.appendField(MemoryFieldKeys[6], 100.0*(used/(1.0 * total)));
		p.appendField(MemoryFieldKeys[7], 100.0*((double)total-used)/(1.0 * total));
		p.appendField(MemoryFieldKeys[8], 100.0*((double)total-buffered)/(1.0 * total));
	}
}
