package measurement;

import java.util.Date;
import java.util.Random;

import distribution.ClampedRandomWalkDistribution;
import distribution.IDistribution;
import distribution.NormalDistribution;

public class CPUMeasurement implements ISimulatedMeasurement{
	
	private static String[] CPUFieldKeys = {
		"usage_user",
		"usage_system",
		"usage_idle",
		"usage_nice",
		"usage_iowait",
		"usage_irq",
		"usage_softirq",
		"usage_steal",
		"usage_guest",
		"usage_guest_nice"
	};
	private Date timestamp;
	private IDistribution[] distributions;
	
	public CPUMeasurement(Date start, Random rand){
		timestamp = start;
		distributions = new IDistribution[10];
		for(int i = 0; i < CPUFieldKeys.length; i++){
			distributions[i] = new ClampedRandomWalkDistribution(0.0, 100.0, rand.nextDouble() * 100.0,
					new NormalDistribution(0.0, 1.0, rand));
			
		}
	}
	
	//µ¥Î»ÊÇms
	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime());  // Add(d);
		for(int i = 0; i < CPUFieldKeys.length; i++){
			distributions[i].Advance();
		}
	}
	
	public void ToPoint(Point p) {
		p.setMeasurementName("cpu");
		p.setTimestamp(timestamp);
		for(int i = 0; i < CPUFieldKeys.length; i++){
			p.appendField(CPUFieldKeys[i], distributions[i].Get());
		}
	}

}
