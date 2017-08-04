package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import cn.edu.thu.tsfiledb.transferfile.transfer.configure.ClientConfigure;

import java.util.Scanner;
import java.util.Timer;

/**
 * Created by lylw on 2017/7/17.
 */
public class Client {
	private long timeInterval = 180000L;
	private long delay_time = 0L;
	private boolean switchTiming;
	private long startTime;
	private Timer timer;
	private static boolean timerTaskRunning = false;

	public static boolean isTimerTaskRunning() {
		return timerTaskRunning;
	}

	public static void setTimerTaskRunning(boolean timerTaskRunning) {
		timerTaskRunning = timerTaskRunning;
	}

	public long getStartTime() {
		return startTime;
	}

	public void clientService() {
		/** load properties for client */
		ClientConfigure.loadProperties();

		/** transfer files */
		Scanner in = new Scanner(System.in);
		timer = new Timer();
		startTime = System.currentTimeMillis() + delay_time;
		timer.schedule(new TransferThread(), delay_time, timeInterval);

		/** waiting for input */
		try {
			while (true) {
				System.out.print("input a command:\n");
				String cmd = in.nextLine();
				if (cmd.equals("set")) {
					System.out.print("input delay time: ");
					delay_time = in.nextLong();
					startTime = System.currentTimeMillis() + delay_time;
					System.out.print("input time interval: ");
					timeInterval = in.nextLong();

					timer.cancel();
					timer.purge();
					timer = new Timer();
					timer.schedule(new TransferThread(), delay_time, timeInterval);
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

						delay_time = startTime - System.currentTimeMillis();
						timer = new Timer();
						timer.schedule(new TransferThread(), delay_time, timeInterval);
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