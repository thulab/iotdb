package measurement;

import java.util.Date;
import java.util.Random;

import distribution.ClampedRandomWalkDistribution;
import distribution.IDistribution;
import distribution.NormalDistribution;

public class DiskMeasurement implements ISimulatedMeasurement {
	private static String DiskByteString = "disk"; // heap optimization
	private static String TotalByteString = "total";
	private static String FreeByteString = "free";
	private static String UsedByteString = "used";
	private static String UsedPercentByteString = "used_percent";
	private static String INodesTotalByteString = "inodes_total";
	private static String INodesFreeByteString = "inodes_free";
	private static String INodesUsedByteString = "inodes_used";

	private static String[] DiskTags = { "path", "fstype" };
	private static String[] DiskFSTypeChoices = { "ext3", "ext4", "btrfs" };

	private long OneTerabyte = 1 << 40;

	private Date timestamp;
	private String path;
	private String fsType;
//	private long uptime;
	// uptime time.Duration;
	private IDistribution freeBytesDist;

	public DiskMeasurement(Date start, Random rand) {
		timestamp = start;
		path = String.format("_dev_sda%d", rand.nextInt(10));
		fsType = DiskFSTypeChoices[rand.nextInt(DiskFSTypeChoices.length)];
		freeBytesDist = new ClampedRandomWalkDistribution(0, OneTerabyte,
				OneTerabyte / 2, new NormalDistribution(50, 1, rand));
	}

	// µ¥Î»ÊÇms
	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime()); // Add(d);
		freeBytesDist.Advance();
	}

	public void ToPoint(Point p) {
		p.setMeasurementName(DiskByteString);
		p.setTimestamp(timestamp);

		p.appendTag(DiskTags[0], path);
		p.appendTag(DiskTags[1], fsType);

		// the only thing that actually changes is the free byte count:
		long free = (long) freeBytesDist.Get();
		long total = OneTerabyte;
		long used = total - free;
		long usedPercent = (long) (100.0 * used / total);

		// inodes are 4096b in size:
		long inodesTotal = total / 4096;
		long inodesFree = free / 4096;
		long inodesUsed = used / 4096;

		p.appendField(TotalByteString, total);
		p.appendField(FreeByteString, free);
		p.appendField(UsedByteString, used);
		p.appendField(UsedPercentByteString, usedPercent);
		p.appendField(INodesTotalByteString, inodesTotal);
		p.appendField(INodesFreeByteString, inodesFree);
		p.appendField(INodesUsedByteString, inodesUsed);
	}

}
