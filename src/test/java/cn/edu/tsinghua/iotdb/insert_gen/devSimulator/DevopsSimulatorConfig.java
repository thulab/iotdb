package devSimulator;

import java.util.Date;
import java.util.Random;
import measurement.Point;

public class DevopsSimulatorConfig {
	private Date start;
	private Date end;
	private long HostCount;

	private DevopsSimulator sim;
	private Point point;
	
	public DevopsSimulatorConfig(Date start, Date end, long HostCount, Random rand){
		this.start = start;
		this.end = end;
		this.HostCount = HostCount;
		sim = ToSimulator(rand);
		point = new Point();
	}
	
	private DevopsSimulator ToSimulator(Random rand)  {
		Host[] hostInfos = new Host[(int) HostCount];
		for(int i = 0; i < HostCount; i++){
			hostInfos[i] = new Host(i, start, rand);
		}

		long epochs = (end.getTime() - start.getTime()) / Host.getEpochDuration();
		long maxPoints = epochs * (HostCount * Host.getNHostSims());
		DevopsSimulator dg = new DevopsSimulator(maxPoints, hostInfos,start, end);
		return dg;
	}

	public boolean Finished(){
		return sim.Finished();
	}
	
	public String createInsertSQLStatment(){
		point.Reset();
		sim.Next(point);
		return point.creatInsertStatement();
	}
}
