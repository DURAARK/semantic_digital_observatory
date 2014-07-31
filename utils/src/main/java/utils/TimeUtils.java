package utils;

import java.text.NumberFormat;
import java.util.Locale;

public class TimeUtils {
	/**
	 * For a given start time (as long taken from the system.nanotime(), compute the time difference
	 * which results in the time span taken to perform a process.
	 * @param time
	 * @return
	 */
	public static String measureComputingTime(long time){
		time = System.nanoTime() - time;
		
		String timeString = NumberFormat.getInstance(Locale.US).format((double) time / 1000D / 1000D / 1000D);
		return ((new StringBuilder(String.valueOf(timeString))).append("s").toString());
	}
}
