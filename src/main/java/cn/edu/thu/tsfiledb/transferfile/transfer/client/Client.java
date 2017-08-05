package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import java.util.Scanner;
import java.util.Timer;

/**
 * Created by lylw on 2017/7/17.
 */
public class Client {
	private long timeInterval = 180000L;
	private long delayTime = 0L;
	private boolean switchTiming;
	private long startTime;
	private Timer timer;
	private static boolean timerTaskRunning = false;

	public static boolean isTimerTaskRunning() {
		return timerTaskRunning;
	}

	public static void setTimerTaskRunning(boolean timerTaskRunning) {
		Client.timerTaskRunning = timerTaskRunning;
	}

	public long getStartTime() {
		return startTime;
	}

	public void clientService() {

		/** transfer files */
		Scanner in = new Scanner(System.in);
		timer = new Timer();
		startTime = System.currentTimeMillis() + delayTime;
		timer.schedule(new TransferThread(), delayTime, timeInterval);

		/** waiting for input */
		try {
			while (true) {
				System.out.print("input a command:\n");
				String cmd = in.nextLine();
				if (cmd.equals("set")) {
					System.out.print("input delay time: ");
					delayTime = in.nextLong();
					startTime = System.currentTimeMillis() + delayTime;
					System.out.print("input time interval: ");
					timeInterval = in.nextLong();

					timer.cancel();
					timer.purge();
					timer = new Timer();
					timer.schedule(new TransferThread(), delayTime, timeInterval);
				} else if (cmd.equals("transfer now")) {
					Thread thread = new Thread(new TransferThread());
					thread.start();
				} else if (cmd.equals("switch")) {
					System.out.print("set timing task on(1) or off(0):");
					int getbool = in.nextInt();
					switchTiming = (getbool == 0) ? false : true;
					if (switchTiming) {
						/** start schedule */
						timer.cancel();
						timer.purge();
						Long nowtime = System.currentTimeMillis();
						while (startTime < nowtime) {
							startTime += timeInterval;
						}
						delayTime = startTime - System.currentTimeMillis();
						timer = new Timer();
						timer.schedule(new TransferThread(), delayTime, timeInterval);
					} else if (!switchTiming) {
						timer.cancel();
						timer.purge();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			in.close();
		}
	}

	public static void main(String[] args) {
		Client transferClient = new Client();
		transferClient.clientService();
	}
}