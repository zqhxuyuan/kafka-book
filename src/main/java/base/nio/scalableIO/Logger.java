package base.nio.scalableIO;

import static base.nio.scalableIO.ServerContext.isLog;

public class Logger {

	public static void log(String info){
		if(isLog){
			System.out.println(info);
		}
	}
	
	public static void err(String info){
		if(isLog){
			System.err.println(info);
		}
	}
	
}
