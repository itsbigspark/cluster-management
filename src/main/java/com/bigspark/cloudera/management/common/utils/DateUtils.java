package com.bigspark.cloudera.management.common.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;

public class DateUtils {
	public static Boolean isMonthEnd(LocalDate date) {
		YearMonth ym = YearMonth.from(date);
		if(date.isEqual(ym.atEndOfMonth())) {
			return true;
		} else {
			return false;
		}
	}

	public static LocalDate getPriorMonthEnd(LocalDate date) {
		if(DateUtils.isMonthEnd(date)) {
			return date;
		} else {
			YearMonth ym = YearMonth.from(date);
			Period p = Period.ofMonths(1);
			return ym.minus(p).atEndOfMonth();
		}
	}

	public static LocalDate getMonthStart(LocalDate date) {
		if(DateUtils.isMonthEnd(date)) {
			return date;
		} else {
			YearMonth ym = YearMonth.from(date);
		//	Period p = Period.ofDays(1);
			return ym.atDay(1);
		}
	}

	public static Date getDate(Date currDate, int daysToSubtract) {
		Calendar c = Calendar.getInstance();
		c.add(Calendar.DAY_OF_MONTH, 0 - daysToSubtract);
		return c.getTime();
	}

	public static Date getDate(String date) throws ParseException {
		return getDate(date, "yyyy-MM-dd");
	}

	public static boolean validDate(String date) throws ParseException {
		try{
			getDate(date, "yyyy-MM-dd");
			return true;
		} catch (Exception e){
			return false;
		}
	}

	public static Date getDate(String date, String dateFormat) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
		return formatter.parse(date);
	}

	public static int getDay(Date currDate) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(currDate);
		return cal.get(Calendar.DAY_OF_MONTH);
	}

	public static int getDayOfWeek(Date currDate) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(currDate);
		return cal.get(Calendar.DAY_OF_WEEK);
	}

	public static Date getEDIFileDate(FileStatus currFile) {
		Date currFileDate = new Date();
		try {
			currFileDate = DateUtils.getEDIFileDate(currFile.getPath().getName());
		} catch (Exception e) {
		}
		return currFileDate;
	}


	public static Date getEDIFileDate(String fileName) throws ParseException {
		if (fileName.endsWith(".gz")) {
			fileName = fileName.replaceAll(".gz", "");
		}
		String parsedFileName = fileName.substring(0, fileName.lastIndexOf("."));
		parsedFileName = parsedFileName.substring(parsedFileName.lastIndexOf(".") + 1, parsedFileName.length());
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		return format.parse(parsedFileName);
	}

	public static String getFormattedDateTime(Date currDate) throws ParseException {
		return getFormattedDate(currDate, "yyyy-MM-dd HH:mm:ss.SSS");
	}
	
	public static String getFormattedDate(Date currDate) throws ParseException {
		return getFormattedDate(currDate, "yyyy-MM-dd");
	}

	public static String getFormattedDate(Date currDate, String dateFormat) throws ParseException {
		DateFormat format = new SimpleDateFormat(dateFormat);
		return format.format(currDate);
	}

	public static int getMonth(Date currDate) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(currDate);
		return cal.get(Calendar.MONTH) + 1;
	}

	public static int getYear(Date currDate) {
		// return DateUtils.getU
		Calendar cal = Calendar.getInstance();
		cal.setTime(currDate);
		return cal.get(Calendar.YEAR);
	}

	public static boolean shouldRunForDate(String runDays, Date runDate) {
		int dow = DateUtils.getDayOfWeek(runDate);
		String[] runDaysAct = runDays.split("|");

		for (String day : runDaysAct) {
			if (Utils.tryParseInt(day, -1) == dow)
				return true;
		}

		return false;

	}
}
