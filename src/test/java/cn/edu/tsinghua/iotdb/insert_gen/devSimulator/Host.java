package devSimulator;


import java.util.Date;
import java.util.List;
import java.util.Random;

import measurement.*;

public class Host {
	private static int NHostSims = 9;

	// Count of choices for auto-generated tag values:
	private static int MachineRackChoicesPerDatacenter = 100;
	private static int MachineServiceChoices = 20;
	private static int 	MachineServiceVersionChoices = 2;
	
	private static final String[] MachineTeamChoices = {"SF", "NYC", "LON", "CHI"};
	private static final String[] MachineOSChoices = {"Ubuntu16_10", "Ubuntu16_04LTS", "Ubuntu15_10"};
	private static final String[] MachineArchChoices = {"x64", "x86"};
	private static final String[] MachineServiceEnvironmentChoices = {"production", "staging", "test"};

	// The duration of a log epoch.
	private static int EpochDuration = 10*1000;

	// Tag fields common to all hosts:
	public static final String[] MachineTagKeys = {
			"hostname",
			"region",
			"datacenter",
			"rack",
			"os",
			"arch",
			"team",
			"service",
			"service_version",
			"service_environment"
	};
	
	private final String[] us_east_1 = {"us-east-1a", "us-east-1b", "us-east-1c", "us-east-1e"};
	private final String[] us_west_1 = {"us-west-1a", "us-west-1b"};
	private final String[] us_west_2 = {"us-west-2a", "us-west-2b", "us-west-2c"};
	private final String[] eu_west_1 = {"eu-west-1a", "eu-west-1b", "eu-west-1c"};
	private final String[] eu_central_1 = {"eu-central-1a", "eu-central-1b"};
	private final String[] ap_southeast_1 = {"ap-southeast-1a", "ap-southeast-1b"};
	private final String[] ap_southeast_2 = {"ap-southeast-2a", "ap-southeast-2b"};
	private final String[] ap_northeast_1 = {"ap-northeast-1a", "ap-northeast-1c"};
	private final String[] sa_east_1 = {"sa-east-1a", "sa-east-1b", "sa-east-1c"};
	
	// Choices of regions and their datacenters.
	private Region[] regions = {
			new Region("us-east-1", us_east_1),
			new Region("us-west-1", us_west_1),
			new Region("us-west-2", us_west_2),
			new Region("eu-west-1", eu_west_1),
			new Region("eu-central-1", eu_central_1),
			new Region("ap-southeast-1", ap_southeast_1),
			new Region("ap-southeast-2", ap_southeast_2),
			new Region("ap-northeast-1", ap_northeast_1),
			new Region("sa-east-1", sa_east_1)
	};
	
	
	private ISimulatedMeasurement[] simulatedMeasurements;
	// These are all assigned once, at Host creation:
	private String name, region, dataCenter, rack, os, arch;
	private String team, service, serviceVersion, serviceEnvironment;
	
	
	public static int getEpochDuration(){
		return EpochDuration ;//= 10 * new Date().getSeconds();
	}
	
	// TickAll advances all Distributions of a Host.
	public void TickAll(long d) {
		for (int i = 0; i < simulatedMeasurements.length; i++) {
			simulatedMeasurements[i].Tick(d);
		}
	}
	
	
	private ISimulatedMeasurement[] NewHostMeasurements(Date start, Random rand)  {
		ISimulatedMeasurement[] sm = {
			new CPUMeasurement(start, rand),
			new DiskIOMeasurement(start, rand),
			new DiskMeasurement(start, rand),
			new KernelMeasurement(start, rand),
			new MemMeasurement(start, rand),
			new NetMeasurement(start, rand),
			new NginxMeasurement(start, rand),
			new PostgresqlMeasurement(start, rand),
			new RedisMeasurement(start, rand)
		};
		return sm;
	}
	
	public Host(int i, Date start, Random rand){
		// Tag Values that are static throughout the life of a Host:
		name = String.format("host_%d", i);
		Region reg = regions[rand.nextInt(regions.length)];
		region = String.format("%s", reg.getName());
		simulatedMeasurements = NewHostMeasurements(start, rand);
		rack = "" + rand.nextInt(MachineRackChoicesPerDatacenter);
		service = "" + rand.nextInt(MachineServiceChoices);
		serviceVersion = "" + rand.nextInt(MachineServiceVersionChoices);
		
		serviceEnvironment = MachineServiceEnvironmentChoices
				[rand.nextInt(MachineServiceEnvironmentChoices.length)];
		List<String> Datacenters = reg.getDataCenters();
		dataCenter = Datacenters.get(rand.nextInt(Datacenters.size()));
		arch = MachineArchChoices[rand.nextInt(MachineArchChoices.length)];
		os = MachineOSChoices[rand.nextInt(MachineOSChoices.length)];
		team = MachineTeamChoices[rand.nextInt(MachineTeamChoices.length)];
	}
	
	public static int getNHostSims() {
		return NHostSims;
	}

	public static void setNHostSims(int nHostSims) {
		NHostSims = nHostSims;
	}
	
	public Region[] getRegions() {
		return regions;
	}

	public String getName() {
		return name;
	}

	public String getRegion() {
		return region;
	}

	public String getRack() {
		return rack;
	}

	public String getOs() {
		return os;
	}

	public String getArch() {
		return arch;
	}

	public String getTeam() {
		return team;
	}

	public String getService() {
		return service;
	}

	public String getServiceVersion() {
		return serviceVersion;
	}

	public String getServiceEnvironment() {
		return serviceEnvironment;
	}

	public String getDataCenter() {
		return dataCenter;
	}

	public ISimulatedMeasurement[] getSimulatedMeasurements() {
		return simulatedMeasurements;
	}

}
