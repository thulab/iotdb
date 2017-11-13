package measurement;

import java.util.Date;
import java.util.Random;

import distribution.ClampedRandomWalkDistribution;
import distribution.IDistribution;
import distribution.MonotonicRandomWalkDistribution;
import distribution.NormalDistribution;
import distribution.RandomWalkDistribution;

public class RedisMeasurement implements ISimulatedMeasurement {
	private static final String RedisByteString = "redis";// heap optimization
	private static final String RedisUptime = "uptime_in_seconds";
	private final double SixteenGB = 16 * 1024 * 1024 * 1024;
	private static final String[] RedisTags = {"port", "server"};
	private static String[] RedisFieldKeys = {
	"total_connections_received",
	"expired_keys",
	"evicted_keys",
	"keyspace_hits",
	"keyspace_misses",
	
	"instantaneous_ops_per_sec",
	"instantaneous_input_kbps",
	"instantaneous_output_kbps",
	"connected_clients",
	"used_memory",
	"used_memory_rss",
	"used_memory_peak",
	"used_memory_lua",
	"rdb_changes_since_last_save",
	
	"sync_full",
	"sync_partial_ok",
	"sync_partial_err",
	"pubsub_channels",
	"pubsub_patterns",
	"latest_fork_usec",
	"connected_slaves",
	"master_repl_offset",
	"repl_backlog_active",
	"repl_backlog_size",
	"repl_backlog_histlen",
	"mem_fragmentation_ratio",
	"used_cpu_sys",
	"used_cpu_user",
	"used_cpu_sys_children",
	"used_cpu_user_children"
	};
	
	private Date timestamp;
	private IDistribution[] distributions;
	private String serverName;
	private String port;
	private long uptime;
	
	public RedisMeasurement(Date start, Random rand){
		timestamp = start;
		distributions = new IDistribution[30];
		serverName = String.format("redis_%d", rand.nextInt(100000));
		port = String.format("%d", rand.nextInt(20000)+1024);
		
		distributions[0] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(5, 1, rand), 0);
		distributions[1] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(50, 1, rand), 0);
		distributions[2] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(50, 1, rand), 0);
		distributions[3] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(50, 1, rand), 0);
		distributions[4] = new MonotonicRandomWalkDistribution(
				new NormalDistribution(50, 1, rand), 0);
		
		distributions[5] = new RandomWalkDistribution(
				new NormalDistribution(1, 1, rand), 0);
		distributions[6] = new RandomWalkDistribution(
				new NormalDistribution(1, 1, rand), 0);
		distributions[7] = new RandomWalkDistribution(
				new NormalDistribution(1, 1, rand), 0);
		distributions[8] = new ClampedRandomWalkDistribution(
				0, 10000, 0, new NormalDistribution(50, 1, rand));
		for(int i = 0;i<4;i++){
			distributions[9 + i] = new ClampedRandomWalkDistribution(
					0, SixteenGB, SixteenGB/2, new NormalDistribution(50, 1, rand));
		}
		
		for(int i = 13;i<30;i++){
			distributions[i] = new ClampedRandomWalkDistribution(
					0, 1000, 0, new NormalDistribution(5, 1, rand));
		}
	}
	

	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime());  // Add(d);
		uptime += duration;
		for(int i = 0; i < RedisFieldKeys.length; i++){
			distributions[i].Advance();
		}
	}
	
	public void ToPoint(Point p) {
		p.setMeasurementName(RedisByteString);
		p.setTimestamp(timestamp);
		p.appendTag(RedisTags[0], port);
		p.appendTag(RedisTags[1], serverName);
		p.appendField(RedisUptime, (long) new Date(uptime).getSeconds());
		for(int i = 0; i < RedisFieldKeys.length; i++){
			p.appendField(RedisFieldKeys[i], (long)distributions[i].Get());
		}
	}
}
