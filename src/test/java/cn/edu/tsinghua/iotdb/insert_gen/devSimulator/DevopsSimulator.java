package devSimulator;


import java.util.Date;

import measurement.Point;
import measurement.ISimulatedMeasurement;

//A DevopsSimulator generates data similar to telemetry from Telegraf.
//It fulfills the Simulator interface.
public class DevopsSimulator {
	
	private long madePoints;
	private long maxPoints;

	private int simulatedMeasurementIndex;
	private int hostIndex;
	
	private Host[] hosts;

	private Date timestampNow;
	private Date timestampStart;
	private Date timestampEnd;
	
	public DevopsSimulator(long maxPoints, Host[] hostInfos,Date Start, Date End){
		madePoints = 0;
		this.maxPoints = maxPoints;
		simulatedMeasurementIndex = 0;
		hostIndex = 0;
		hosts = hostInfos;
		timestampStart = Start;
		timestampNow = Start;
		timestampEnd = End;
	}
	
	public long Seen(){
		return madePoints;
	}

	public long Total(){
		return maxPoints;
	}

	public boolean Finished(){
		return madePoints >= maxPoints;
	}
	
	// Next advances a Point to the next state in the generator.
	public void Next(Point p) {
		// switch to the next metric if needed
		if(hostIndex == hosts.length) {
			hostIndex = 0;
			simulatedMeasurementIndex++;
		}
		if (simulatedMeasurementIndex == Host.getNHostSims()) {
			simulatedMeasurementIndex = 0;
			for (int i = 0; i < hosts.length; i++){
				hosts[i].TickAll(Host.getEpochDuration());
			}
		}
		Host host = hosts[hostIndex];
		// Populate host-specific tags:
		p.appendTag(Host.MachineTagKeys[0], host.getName());
		p.appendTag(Host.MachineTagKeys[1], host.getRegion());
		p.appendTag(Host.MachineTagKeys[2], host.getDataCenter());
		p.appendTag(Host.MachineTagKeys[3], host.getRack());
		p.appendTag(Host.MachineTagKeys[4], host.getOs());
		p.appendTag(Host.MachineTagKeys[5], host.getArch());
		p.appendTag(Host.MachineTagKeys[6], host.getTeam());
		p.appendTag(Host.MachineTagKeys[7], host.getService());
		p.appendTag(Host.MachineTagKeys[8], host.getServiceVersion());
		p.appendTag(Host.MachineTagKeys[9], host.getServiceEnvironment());
		
		// Populate measurement-specific tags and fields:
		host.getSimulatedMeasurements()[simulatedMeasurementIndex].ToPoint(p);
		
		madePoints++;
		hostIndex++;
	}
	
}
