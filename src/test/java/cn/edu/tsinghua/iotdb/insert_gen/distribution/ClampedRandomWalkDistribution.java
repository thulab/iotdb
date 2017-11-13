package distribution;

/**
 * ClampedRandomWalkDistribution is a stateful random walk, with minimum and
	maximum bounds. Initialize it with a Min, Max, and an underlying
	distribution, which is used to compute the new step value.
*/
public class ClampedRandomWalkDistribution implements IDistribution {
	
	private IDistribution  step;
	private  double min;
	private  double max;
	private  double state;
	
	public ClampedRandomWalkDistribution(double min, double max, double state, IDistribution step){
		this.state = state;
		this.step = step;
		this.min = min;
		this.max = max;
	}
	
	public ClampedRandomWalkDistribution CWD(IDistribution step, double min, double max, double state){
		return new ClampedRandomWalkDistribution(min, max, state, step);
	}

	@Override
	public void Advance() {
		// TODO Auto-generated method stub
		step.Advance();
		state += step.Get();
		if( state > max ){
			state = max;
		}
		if( state < min ){
			state = min;
		}
	}

	@Override
	public double Get() {
		// TODO Auto-generated method stub
		return state;
	}

}
