package measurement;

import java.util.Date;
import java.util.Random;

import distribution.ClampedRandomWalkDistribution;
import distribution.IDistribution;
import distribution.MonotonicRandomWalkDistribution;
import distribution.NormalDistribution;
import distribution.RandomWalkDistribution;

public class NetMeasurement implements ISimulatedMeasurement {
	
	private static final String NetByteString = "net";// heap optimization
	private static final String[] NetTags = {"interface"};
	
	private static String[] NetFieldKeys = {
	"bytes_sent",
	"bytes_recv",
	"packets_sent",
	"packets_recv",
	"err_in",
	"err_out",
	"drop_in",
	"drop_out"
	};
	
	private Date timestamp;
	private IDistribution[] distributions;
	private String interfaceName;
	private long uptime;
	
	public NetMeasurement(Date start, Random rand){
		timestamp = start;
		distributions = new IDistribution[8];	
		interfaceName = String.format("eth%d", rand.nextInt(4));
		for(int i = 0;i<4;i++){
			distributions[i] = new MonotonicRandomWalkDistribution(
					new NormalDistribution(50, 1, rand), 0);
		}
		
		for(int i = 4;i<8;i++){
			distributions[i] = distributions[i] = new MonotonicRandomWalkDistribution(
					new NormalDistribution(5, 1, rand), 0);
		}
	}
	

	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime());  // Add(d);
		uptime += duration;
		for(int i = 0; i < NetFieldKeys.length; i++){
			distributions[i].Advance();
		}
	}
	
	public void ToPoint(Point p) {
		p.setMeasurementName(NetByteString);
		p.setTimestamp(timestamp);
		p.appendTag(NetTags[0], interfaceName);
		
		for(int i = 0; i < NetFieldKeys.length; i++){
			p.appendField(NetFieldKeys[i], (long)distributions[i].Get());
		}
	}
}
