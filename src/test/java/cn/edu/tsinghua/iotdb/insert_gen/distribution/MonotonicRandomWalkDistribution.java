package distribution;

/**
 *  MonotonicRandomWalkDistribution is a stateful random walk that only
 increases. Initialize it with a Start and an underlying distribution,
 which is used to compute the new step value. The sign of any value of the
 u.d. is always made positive.
 */
public class MonotonicRandomWalkDistribution implements IDistribution {

	private IDistribution  step;
	private  double state;
	
	public MonotonicRandomWalkDistribution(IDistribution  step, double state){
		this.step = step;
		this.state = state;
	}
	
	public MonotonicRandomWalkDistribution MWD(IDistribution  step, double state){
		return new MonotonicRandomWalkDistribution(step,state);
	}
	
	
	@Override
	public void Advance() {
		// TODO Auto-generated method stub
		step.Advance();
		state += Math.abs(step.Get());
	}

	@Override
	public double Get() {
		// TODO Auto-generated method stub
		return state;
	}

}
