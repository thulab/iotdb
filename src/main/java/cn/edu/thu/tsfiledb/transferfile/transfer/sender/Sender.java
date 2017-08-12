package cn.edu.thu.tsfiledb.transferfile.transfer.sender;

import cn.edu.thu.tsfiledb.transferfile.transfer.conf.SenderConfig;

import java.util.Scanner;
import java.util.Timer;

/**
 * Created by lylw on 2017/7/17.
 */
public class Sender {
	private static SenderConfig config = SenderConfig.getInstance();
	private long timeInterval;
	private long delayTime = 0L;
	private long startTime;
	private Timer timer;
	private static boolean timerTaskRunning = false;

	public static boolean isTimerTaskRunning() {
		return timerTaskRunning;
	}

	public static void setTimerTaskRunning(boolean timerTaskRunning) {
		Sender.timerTaskRunning = timerTaskRunning;
	}

	public long getStartTime() {
		return startTime;
	}

	public void senderService() {
		/** transfer files */
		Scanner in = new Scanner(System.in);
		timer = new Timer();
		timeInterval = config.transferTimeInterval;
		startTime = System.currentTimeMillis() + delayTime;
		timer.schedule(new TransferThread(), delayTime, timeInterval);

		/** waiting for input command */
		try {
			while (true) {
				System.out.println("-------------------------------------------------------------");
				System.out.println("Input a command or try help for more info:");
				String cmd = in.nextLine();
				if(cmd == null || cmd.trim().equals("")){
					continue;
				}
				
				if (cmd.trim().equals("set")) {
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
					delayTime = startTime - System.currentTimeMillis();
					while(delayTime<0)delayTime+=timeInterval;
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
						while(delayTime<0)delayTime+=timeInterval;
						timer.schedule(new TransferThread(), delayTime, timeInterval);
					} else {
						timer.cancel();
						timer.purge();
					}
				} else if (cmd.trim().equals("quit") || cmd.trim().equals("exit")) {
					System.out.println("Exit normally");
					break;
				} else if (cmd.trim().equals("help")){
					System.out.println("All legal commands:");
					showLegalCommands();
				} else {
					System.out.println("Unknown input command, please try commands below:");
					showLegalCommands();
				}
			}
		} catch (Exception e) {
			System.out.println(String.format("Error occurs because: ", e.getMessage()));
		} finally {
			in.close();
		}
	}

	private void showLegalCommands() {
		System.out.println("set		-Set parameters for timing task");
		System.out.println("transfer now	-Start TransferThread now");
		System.out.println("switch		-Start or stop timing task");
		System.out.println("help	-Show all commands");
		System.out.println("quit	-quit program");
		System.out.println("exit	-exit program");
	}

	public static void main(String[] args) {
		Sender transferClient = new Sender();
		transferClient.senderService();
	}
}