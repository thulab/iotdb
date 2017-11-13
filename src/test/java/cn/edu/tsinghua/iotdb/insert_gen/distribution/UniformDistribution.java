package distribution;

import java.util.Random;

//UniformDistribution models a uniform distribution.
public class UniformDistribution implements IDistribution {
	
	private double low;
	private  double high;
	private  double value;
	private Random rand;
	
	public UniformDistribution(double low, double high, Random rand){
		this.low = low;
		this.high = high;
		this.rand = rand;
	}
	
	public UniformDistribution UD(double low, double high, Random rand){
			return new UniformDistribution(low, high, rand);
	}

	@Override
	public void Advance() {
		// TODO Auto-generated method stub
		double x = rand.nextDouble(); // uniform
		x *= high - low;
		x += low;
		value = x;
	}

	@Override
	public double Get() {
		// TODO Auto-generated method stub
		return value;
	}

}
