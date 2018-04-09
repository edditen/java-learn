package com.tenchael.zkdemo.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tengzhizhang on 2018/3/14.
 */
public class CuratorTest extends Assert {
	private String connString = "127.0.0.1:2181";

	private CuratorFramework getDefaultClient() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		return CuratorFrameworkFactory.builder().connectString(connString)
				.sessionTimeoutMs(5000).connectionTimeoutMs(3000)
				.retryPolicy(retryPolicy)
				.namespace("zk-demo")
				.build();
	}

	/**
	 * 获取授权客户端
	 *
	 * @param auth 用户凭证，格式："username:password"
	 * @return
	 */
	public CuratorFramework getDefaultAclClient(final String auth) {
		ACLProvider aclProvider = new ACLProvider() {
			private List<ACL> acl;

			@Override
			public List<ACL> getDefaultAcl() {
				if (this.acl == null) {
					ArrayList<ACL> local_acl = ZooDefs.Ids.CREATOR_ALL_ACL;
					local_acl.clear();
					local_acl.add(new ACL(ZooDefs.Perms.ALL, new Id("auth", auth)));
					this.acl = local_acl;
				}
				return this.acl;
			}

			@Override
			public List<ACL> getAclForPath(String s) {
				return getDefaultAcl();
			}
		};

		String scheme = "digest";
		return CuratorFrameworkFactory.builder().aclProvider(aclProvider).
				authorization(scheme, auth.getBytes()).
				connectionTimeoutMs(3000).
				sessionTimeoutMs(5000).
				connectString(connString).
				namespace("zk-acl-demo").
				retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
	}

	@Test
	public void testNewClient() throws Exception {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(connString, 5000, 3000, retryPolicy);
		client.start();
	}

	@Test
	public void testNewFluentClient() {
		getDefaultClient().start();
	}

	@Test
	public void testMakeZknode() throws Exception {
		CuratorFramework client = getDefaultClient();
		client.start();
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL)
				.forPath("/zk-test", "init".getBytes());
	}

	@Test
	public void testRemoveZknode() throws Exception {
		CuratorFramework client = getDefaultClient();
		client.start();
		String path = "/zk-test";
		for (int i = 1; i < 5; i++) {
			client.create().creatingParentsIfNeeded()
					.withMode(CreateMode.PERSISTENT_SEQUENTIAL)
					.forPath(String.format("%s/0", path),
							String.format("init-%s", i).getBytes());
		}
		client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
	}


	@Test
	public void testAcl() throws Exception {
		final String auth = "teng:123";
		String path = "/testAcl";
		CuratorFramework client = getDefaultAclClient(auth);
		client.start();
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL)
				.forPath(path, "aaaaa".getBytes());
		client.setData().forPath(path, "bbbb".getBytes());

		CuratorFramework client2 = getDefaultAclClient("edgar:123456");
		client2.start();
		try {
			client2.setData().forPath(path, "ccccc".getBytes());
			assertTrue(false);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(true);
		}
	}

	@Test
	public void testStat() throws Exception {
		CuratorFramework client = getDefaultClient();
		client.start();
		Stat stat = new Stat();
		String path = "/zk-test";
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL)
				.forPath(String.format("%s/0", path),
						"init".getBytes());
		client.getData().storingStatIn(stat).forPath(path);
		System.out.println(stat.getVersion());
		client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
	}

	@Test
	public void testStatVersion() throws Exception {
		CuratorFramework client = getDefaultClient();
		client.start();
		Stat stat = new Stat();
		String path = "/zk-test/0";
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL)
				.forPath(path, "init".getBytes());
		client.getData().storingStatIn(stat).forPath(path);
		System.out.println("old version: " + stat.getVersion());
		client.setData().withVersion(stat.getVersion()).forPath(path, "hello".getBytes());
		Stat stat2 = new Stat();
		client.getData().storingStatIn(stat2).forPath(path);
		System.out.println("new version: " + stat2.getVersion());
		assertEquals(stat.getVersion() + 1, stat2.getVersion());

		try {
			client.setData().withVersion(stat.getVersion()).forPath(path, "world".getBytes());
			assertTrue(false);
		} catch (Exception e) {
			e.printStackTrace();
			assertTrue(true);
		}

		client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
	}


	@Test
	public void testCallback() throws Exception {
		CuratorFramework client = getDefaultClient();
		client.start();
		String path = "/zk-test";
		final CountDownLatch semaphore = new CountDownLatch(2);
		final ExecutorService executor = Executors.newFixedThreadPool(2);
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL)
				.inBackground(new BackgroundCallback() {
					@Override
					public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent)
							throws Exception {
						System.out.println(String.format("event[code: %s, type: %s]",
								curatorEvent.getResultCode(), curatorEvent.getType()));
						System.out.println(String.format("Thread of processResult: %s",
								Thread.currentThread().getName()));
						semaphore.countDown();
					}
				}, executor).forPath(path, "init".getBytes());


		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL)
				.inBackground(new BackgroundCallback() {
					@Override
					public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent)
							throws Exception {
						System.out.println(String.format("event[code: %s, type: %s]",
								curatorEvent.getResultCode(), curatorEvent.getType()));
						System.out.println(String.format("Thread of processResult: %s",
								Thread.currentThread().getName()));
						semaphore.countDown();
					}
				}).forPath(path, "init".getBytes());
		semaphore.await();
		executor.shutdown();

	}

	@Test
	public void testNodeChangedListener() throws Exception {
		String path = "/zk-test";
		CuratorFramework client = getDefaultClient();
		client.start();
		client.create().creatingParentsIfNeeded()
				.withMode(CreateMode.EPHEMERAL)
				.forPath(path, "init".getBytes());
		final NodeCache cache = new NodeCache(client, path, false);
		cache.start();
		cache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
				System.out.println(String.format("Node data update, new data: %s",
						new String(cache.getCurrentData().getData())));
			}
		});
		client.setData().forPath(path, "hello".getBytes());
		TimeUnit.SECONDS.sleep(1);
		client.delete().deletingChildrenIfNeeded().forPath(path);

	}

	@Test
	public void testSelectMaster() throws Exception {
		final String masterPath = "/recipes/master";
		CuratorFramework client = getDefaultClient();
		client.start();
		LeaderSelector selector = new LeaderSelector(client, masterPath,
				new LeaderSelectorListenerAdapter() {
					@Override
					public void takeLeadership(CuratorFramework curatorFramework)
							throws Exception {
						System.out.println("become master");
						TimeUnit.MILLISECONDS.sleep(300);
						System.out.println("after becoming master, release master");
					}
				});
		selector.autoRequeue();
		selector.start();
		TimeUnit.SECONDS.sleep(3);
	}

	@Test
	public void testDistributedLock() throws Exception {
		final String lockPath = "/recipes/lock";
		CuratorFramework client = getDefaultClient();
		client.start();
		final InterProcessMutex lock = new InterProcessMutex(client, lockPath);
		for (int i = 0; i < 30; i++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						lock.acquire();
					} catch (Exception e) {
						e.printStackTrace();
					}
					SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss|SSS");
					String orderNo = sdf.format(new Date());
					System.out.println(orderNo);
					try {
						lock.release();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();
		}

		TimeUnit.SECONDS.sleep(3);

	}


	@Test
	public void testDistributedCount() throws Exception {
		final String countPath = "/recipes/count";
		CuratorFramework client = getDefaultClient();
		client.start();
		DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, countPath,
				new RetryNTimes(3, 1000));
		AtomicValue<Integer> rc = atomicInteger.add(10);
		System.out.println(String.format("result: %s", rc.succeeded()));
	}

	@Test
	public void testCyclicBarrier() throws Exception {
		int count = 5;
		final CyclicBarrier barrier = new CyclicBarrier(count);
		final ExecutorService excutor = Executors.newFixedThreadPool(count);
		final AtomicInteger index = new AtomicInteger(0);
		for (; index.get() < count; index.incrementAndGet()) {
			excutor.submit(new Runnable() {
				private String name = "runner-" + index.get();

				@Override
				public void run() {
					try {
						System.out.println(String.format("%s is ready", this.name));
						barrier.await();
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println(String.format("%s start running", this.name));
				}
			});
		}
		excutor.shutdown();
		TimeUnit.SECONDS.sleep(1);
	}

	@Test
	public void testDistributedBarrier() throws Exception {
		final String barrierPath = "/recipes/barrier";
		final int count = 5;
		final AtomicInteger index = new AtomicInteger(0);
		final ExecutorService executor = Executors.newFixedThreadPool(count);
		for (; index.get() < count; index.incrementAndGet()) {
			executor.submit(new Runnable() {

				private String name = "Runner-" + index.get();

				@Override
				public void run() {
					CuratorFramework client = getDefaultClient();
					client.start();
					DistributedBarrier barrier = new DistributedBarrier(client, barrierPath);
					System.out.println(String.format("%s ready", this.name));
					try {
						barrier.setBarrier();
						barrier.waitOnBarrier();
						System.out.println(String.format("%s start running", this.name));
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			});
		}

		executor.shutdown();
		TimeUnit.SECONDS.sleep(2);

	}


}
