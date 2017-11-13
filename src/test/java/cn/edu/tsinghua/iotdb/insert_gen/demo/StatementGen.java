package main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Date;
import java.util.Random;

import devSimulator.DevopsSimulator;
import devSimulator.DevopsSimulatorConfig;
import measurement.Point;

public class StatementGen {
	private static FileWriter out; 

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//开始结束时间以及host数目
		Date start = new Date();
		Date end = new Date(start.getTime() + 3546*1000);
		long HostCount = 4;
		
		//文件
		out = new FileWriter(HostCount+"Host.txt");
		
		//初始化种子
		Random rand;
		if(args.length != 0){
			long seed = Long.parseLong(args[0]);
			rand = new Random(seed);
		}
		else{
			rand = new Random();
		}
		
		//生成插入语句过程
		DevopsSimulatorConfig cfg = new DevopsSimulatorConfig(start, end, HostCount, rand);
		
		while(!cfg.Finished()){
			out.write(cfg.createInsertSQLStatment()+"\n");	
		}

		out.close();
		System.out.println("End！");
	}

}
