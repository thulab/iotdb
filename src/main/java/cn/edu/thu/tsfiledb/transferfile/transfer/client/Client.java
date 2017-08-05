package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import java.util.Scanner;
import java.util.Timer;

/**
 * Created by lylw on 2017/7/17.
 */
public class Client {
	private long timeInterval = 180000L;
	private long delayTime = 0L;
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

		/** waiting for input command */
		try {
			
			while (true) {
				System.out.println("Input a command or try help for more info:");
				String cmd = in.nextLine();
				if(cmd == null || cmd.trim().equals("")){
					continue;
				}
				
				if (cmd.trim().equals("set")) {
					System.out.print("Input delay time: ");
					try {
						delayTime = in.nextLong();
					} catch (Exception e) {
						System.out.println("Error input delay time, it must be a number");
						continue;
					}
					
					startTime = System.currentTimeMillis() + delayTime;
					System.out.print("Input time interval: ");
					
					try {
						timeInterval = in.nextLong();
					} catch (Exception e) {
						System.out.println("Error input time interval, it must be a number");
						continue;
					}
					
					timer.cancel();
					timer.purge();
					timer = new Timer();
					timer.schedule(new TransferThread(), delayTime, timeInterval);
				} else if (cmd.trim().equals("transfer now")) {
					Thread thread = new Thread(new TransferThread());
					thread.start();
				} else if (cmd.trim().equals("switch")) {
					System.out.print("set timing task on(false) or off(true):");
					boolean switchTiming;
					try {
						switchTiming = in.nextBoolean();
					} catch (Exception e) {
						System.out.println("Error input format, it must be false or true");
						continue;
					}
					
					if (switchTiming) {
						/** start schedule */
						timer.cancel();
						timer.purge();
						long nowtime = System.currentTimeMillis();
						while (startTime < nowtime) {
							startTime += timeInterval;
						}
						delayTime = startTime - System.currentTimeMillis();
						timer = new Timer();
						timer.schedule(new TransferThread(), delayTime, timeInterval);
					} else {
						timer.cancel();
						timer.purge();
					}
				} else if (cmd.trim().equals("quit") || cmd.trim().equals("exit")) {
					System.out.println("Exit normally");
					break;
				} else {
					System.out.println("Unknown input command, please try help for more info");
				}
			}
		} catch (Exception e) {
			System.out.println(String.format("Error occurs because: ", e.getMessage()));
		} finally {
			in.close();
		}
	}

	public static void main(String[] args) {
		Client transferClient = new Client();
		transferClient.clientService();
	}
}