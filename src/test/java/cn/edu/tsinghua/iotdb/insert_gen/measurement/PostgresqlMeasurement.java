package measurement;

import java.util.Date;
import java.util.Random;

import distribution.ClampedRandomWalkDistribution;
import distribution.IDistribution;
import distribution.NormalDistribution;

public class PostgresqlMeasurement implements ISimulatedMeasurement {
	private static final String PostgresqlByteString = "postgresl"; // heap optimization
	private static final String[] PostgresqlFieldKeys = {
	"numbackends",
	"xact_commit",
	"xact_rollback",
	"blks_read",
	"blks_hit",
	"tup_returned",
	"tup_fetched",
	"tup_inserted",
	"tup_updated",
	"tup_deleted",
	"conflicts",
	"temp_files",
	"temp_bytes",
	"deadlocks",
	"blk_read_time",
	"blk_write_time"
	};
	private Date timestamp;
	private IDistribution[] distributions;
	
	public PostgresqlMeasurement(Date start, Random rand){
		timestamp = start;
		distributions = new IDistribution[16];
		for(int i = 0; i < 12; i++){
			distributions[i] = new ClampedRandomWalkDistribution(0, 1000, 0,
					new NormalDistribution(5, 1, rand));
		}
		distributions[12] = new ClampedRandomWalkDistribution(0, 1024*1024*1024, 0,
				new NormalDistribution(1024, 1, rand));
		for(int i = 13; i < 16; i++){
			distributions[i] = new ClampedRandomWalkDistribution(0, 1000, 0,
					new NormalDistribution(5, 1, rand));
		}
	}
	

	public void Tick(long duration) {
		timestamp = new Date(duration + timestamp.getTime());  // Add(d);
		for(int i = 0; i < PostgresqlFieldKeys.length; i++){
			distributions[i].Advance();
		}
	}
	
	public void ToPoint(Point p) {
		p.setMeasurementName(PostgresqlByteString);
		p.setTimestamp(timestamp);
		for(int i = 0; i < PostgresqlFieldKeys.length; i++){
			p.appendField(PostgresqlFieldKeys[i], (long)distributions[i].Get());
		}
	}
}
