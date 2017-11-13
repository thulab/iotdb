package measurement;

import java.util.Date;
import java.util.Random;

import distribution.IDistribution;
import distribution.MonotonicRandomWalkDistribution;
import distribution.NormalDistribution;

public class KernelMeasurement implements ISimulatedMeasurement {
	private static final String KernelByteString = "kernel";// heap optimization
	private static final String BootTimeByteString = "boot_time";	
	
	private static String[] KernelFieldKeys = {
	"interrupts",
	"context_switches",
	"processes_forked",
	"disk_pages_in",
	"disk_pages_out"
	};
	
	private Date timestamp;
	private IDistribution[] distributions;
	private long uptime, bootTime;
	
	public KernelMeasurement(Date start, Random rand){
		timestamp = start;
		distributions = new IDistribution[5];	
		bootTime = rand.nextInt(240);
		for(int i = 0;i<5;i++){
			distributions[i] = new MonotonicRandomWalkDistribution(
					new NormalDistribution(5, 1, rand), 0);
		}
	}
	
	
	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime());  // Add(d);
		for(int i = 0; i < KernelFieldKeys.length; i++){
			distributions[i].Advance();
		}
	}
	
	public void ToPoint(Point p) {
		p.setMeasurementName(KernelByteString);
		p.setTimestamp(timestamp);
		for(int i = 0; i < KernelFieldKeys.length; i++){
			p.appendField(KernelFieldKeys[i], (long)distributions[i].Get());
		}
	}
}
