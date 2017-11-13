package measurement;

import java.util.Date;
import java.util.Random;

import distribution.ClampedRandomWalkDistribution;
import distribution.IDistribution;
import distribution.MonotonicRandomWalkDistribution;
import distribution.NormalDistribution;


public class NginxMeasurement implements ISimulatedMeasurement {
	private static final String NginxByteString = "nginx";// heap optimization
	private static final String[] NginxTags = {"port", "server"};
	private static String[] NginxFieldKeys = {
	"accepts",
	"active",
	"handled",
	"reading",
	"requests",
	"waiting",
	"writing"
	};
	
	private Date timestamp;
	private IDistribution[] distributions;
	private String serverName;
	private String port;
	
	public NginxMeasurement(Date start, Random rand){
		timestamp = start;
		distributions = new IDistribution[7];
		serverName = String.format("nginx_%d", rand.nextInt(100000));
		port = String.format("%d", rand.nextInt(20000)+1024);
		
		for(int i = 0; i<6; i+=2){
			distributions[i] = new MonotonicRandomWalkDistribution(
					new NormalDistribution(5, 1, rand), 0);
			distributions[i+1] = new ClampedRandomWalkDistribution(
					0, 100, 0, new NormalDistribution(5, 1, rand));
		}
		distributions[6] = new ClampedRandomWalkDistribution(
				0, 100, 0, new NormalDistribution(5, 1, rand));
	}
	

	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime());  // Add(d);
		for(int i = 0; i < NginxFieldKeys.length; i++){
			distributions[i].Advance();
		}
	}
	
	public void ToPoint(Point p) {
		p.setMeasurementName(NginxByteString);
		p.setTimestamp(timestamp);
		p.appendTag(NginxTags[0], port);
		p.appendTag(NginxTags[1], serverName);
		for(int i = 0; i < NginxFieldKeys.length; i++){
			p.appendField(NginxFieldKeys[i], (long)distributions[i].Get());
		}
	}
}
