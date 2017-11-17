package measurement;

public interface ISimulatedMeasurement {
	public void Tick(long duration);
	public void ToPoint(Point p);
}
