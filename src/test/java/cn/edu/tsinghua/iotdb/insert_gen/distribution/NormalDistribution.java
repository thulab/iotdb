package distribution;

import java.util.Random;

//NormalDistribution models a normal distribution.
public class NormalDistribution implements IDistribution {
	
	private double mean;
	private  double stdDev;
	private  double value;
	private Random rand;
	
	public NormalDistribution(double mean, double stddev, Random rand){
		this.mean = mean;
		this.stdDev = stddev;
		this.rand = rand;
	}
	
	public NormalDistribution ND(double mean, double stddev, Random rand){
			return new NormalDistribution(mean, stddev, rand);
	}
	
	@Override
	public void Advance() {
		// TODO Auto-generated method stub
		value = rand.nextGaussian()*stdDev + mean;
	}

	@Override
	public double Get() {
		// TODO Auto-generated method stub
		return value;
	}
	
}
