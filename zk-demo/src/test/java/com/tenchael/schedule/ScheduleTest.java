package com.tenchael.schedule;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tengzhizhang on 2018/8/29.
 */
public class ScheduleTest {

	private static class SystemMock extends MockUp<System> {
		@Mock
		public static long currentTimeMillis() {
//			return 10000000L;
			return System.currentTimeMillis();
		}

		private static final AtomicLong ATOMIC_LONG = new AtomicLong(10000000L);

		@Mock
		public static long nanoTime() {
			return TimeUnit.NANOSECONDS.convert(ATOMIC_LONG.getAndIncrement(), TimeUnit.MILLISECONDS);
		}
	}

	@Test
	public void testSchedule() throws InterruptedException {
		new SystemMock();
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				printCount();
			}
		}, 0, 1, TimeUnit.SECONDS);
		TimeUnit.SECONDS.sleep(60);
	}

	private static final AtomicInteger COUNTER = new AtomicInteger(0);
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss.SSS");

	private void printCount() {
		System.out.println(String.format("%s:\t%s", COUNTER.getAndIncrement(), sdf.format(new Date())));
	}


}
