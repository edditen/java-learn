package com.tenchael.zkdemo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tengzhizhang on 2018/7/13.
 */
public class LeaderElection {

	private static String connString = "127.0.0.1:2181";
	private static final String MASTER_PATH = "/recipes/master";
	private static int clientNums = 10;

	private static CuratorFramework getDefaultClient() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		return CuratorFrameworkFactory.builder().connectString(connString)
				.sessionTimeoutMs(5000).connectionTimeoutMs(3000)
				.retryPolicy(retryPolicy)
				.namespace("zk-demo")
				.build();
	}

	public static void main(String[] args) throws IOException {
		List<CuratorFramework> clients = new ArrayList<>();
		List<ExampleClient> examples = new ArrayList<>();
		for (int i = 0; i < clientNums; ++i) {
			CuratorFramework client = getDefaultClient();
			clients.add(client);

			ExampleClient example = new ExampleClient(client, MASTER_PATH, "Client #" + i);
			examples.add(example);

			client.start();
			new Thread(example).start();
		}


		System.in.read();

	}

	static class ExampleClient extends LeaderSelectorListenerAdapter implements Runnable {

		private LeaderSelector leaderSelector;
		private String name;
		private final AtomicInteger leaderCount = new AtomicInteger(0);

		public ExampleClient(CuratorFramework client, String path, String name) {
			this.name = name;
			leaderSelector = new LeaderSelector(client, path, this);
			leaderSelector.autoRequeue();
		}

		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			// we are now the leader. This method should not return until we want to relinquish leadership

			final int waitSeconds = (int) (5 * Math.random()) + 1;

			System.out.println(name + " is now the leader. Waiting " + waitSeconds + " seconds...");
			System.out.println(name + " has been leader " + leaderCount.getAndIncrement() + " time(s) before.");
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
			} catch (InterruptedException e) {
				System.err.println(name + " was interrupted.");
				Thread.currentThread().interrupt();
			} finally {
				System.out.println(name + " relinquishing leadership.\n");
			}
		}

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			super.stateChanged(client, newState);
			System.out.println(name + " state changed:" + newState);
			switch (newState) {
				case SUSPENDED:
				case LOST:
					throw new CancelLeadershipException();
				default:
					//do nothing
			}

			//random cancel leadership taking to mock exception
			if (((int) (Math.random() * 13)) % 2 == 0) {
				System.out.println(name + " do *******************");
				throw new CancelLeadershipException(name + " cancel leader take");
			}
		}

		@Override
		public void run() {
			leaderSelector.start();
		}
	}
}
