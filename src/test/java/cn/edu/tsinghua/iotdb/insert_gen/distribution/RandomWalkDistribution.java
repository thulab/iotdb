package distribution;

//RandomWalkDistribution is a stateful random walk. Initialize it with an
//underlying distribution, which is used to compute the new step value.
public class RandomWalkDistribution implements IDistribution {
	
	private IDistribution  step;
	private  double state;
	
	public RandomWalkDistribution(IDistribution  step, double state){
		this.step = step;
		this.state = state;
	}
	
	public RandomWalkDistribution WD(IDistribution  step, double state){
		return new RandomWalkDistribution(step, state);
	}

	@Override
	public void Advance() {
		// TODO Auto-generated method stub
		step.Advance();
		state += step.Get();
	}

	@Override
	public double Get() {
		// TODO Auto-generated method stub
		return state;
	}

}
