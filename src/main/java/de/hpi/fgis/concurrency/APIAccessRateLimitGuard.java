package de.hpi.fgis.concurrency;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

/**
 * this class enables the execution of several API calls with rate limits (i.e.,
 * calls per second/minute/hour)
 * 
 * @author tongr
 * 
 */
public abstract class APIAccessRateLimitGuard implements Closeable {
	private final long startDelay;
	private final long period;
	private final Timer timer = new Timer();
	private final Queue<RateLimitedTask> jobs = new LinkedList<>();

	protected APIAccessRateLimitGuard(long startDelay, long period) {
		this.startDelay = startDelay;
		this.period = period;

		init();
	}

	private void init() {
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				RateLimitedTask task;
				synchronized (jobs) {
					task = jobs.poll();
				}

				if (task != null) {
					task.run();

					// add the task to the queue if it has to be repeated
					if (task.repeat()) {
						synchronized (jobs) {
							jobs.add(task);
						}
					}
				}
			}
		}, startDelay, period);
	}

	/**
	 * adds another rate limited task to the scheduler
	 * 
	 * @param task
	 */
	public void add(RateLimitedTask task) {
		synchronized (jobs) {
			jobs.add(task);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() {
		timer.cancel();
	}

	/**
	 * instances of this interface are allowed to be executed in a scheduled
	 * manner for API calls with rate limits
	 * 
	 * @author tongr
	 * 
	 */
	public static interface RateLimitedTask extends Runnable {
		/**
		 * if this method returns <code>true</code>, the task will be repeated
		 * at a given point of time (see also {@link APIAccessRateLimitGuard})
		 * 
		 * @return if <code>true</code>, the task will be repeated
		 */
		public abstract boolean repeat();
	}
}
