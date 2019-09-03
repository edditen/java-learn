package com.tenchael.waitnotify;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WaitNotifyTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(WaitNotifyTests.class);


    @Test
    public void testWaitNotify() throws InterruptedException {
        final List<Integer> taskQueue = new ArrayList<>();
        int maxSize = 1;
        final AtomicInteger count = new AtomicInteger(0);
        Producer producer1 = new Producer(taskQueue, maxSize, count);
        Producer producer2 = new Producer(taskQueue, maxSize, count);
        Consumer consumer1 = new Consumer(taskQueue);
        Consumer consumer2 = new Consumer(taskQueue);

        new Thread(consumer1, "consumer-1").start();
        new Thread(consumer2, "consumer-2").start();
        new Thread(producer1, "producer-1").start();
        new Thread(producer2, "producer-2").start();
        TimeUnit.SECONDS.sleep(1);
    }

    static class Producer implements Runnable {

        private final List<Integer> taskQueue;
        private final int maxSize;
        private final AtomicInteger count;
        private final int maxCount = 100;

        public Producer(List<Integer> taskQueue, int maxSize, AtomicInteger count) {
            this.taskQueue = taskQueue;
            this.maxSize = maxSize;
            this.count = count;
        }

        @Override
        public void run() {
            while (count.get() < maxCount) {
                try {
                    produce(count.incrementAndGet());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("produce occurs error: {}", e.getMessage(), e);
                }
            }

        }

        private void produce(int i) throws InterruptedException {
            synchronized (taskQueue) {
                while (taskQueue.size() == maxSize) {
                    taskQueue.wait();
                }
                taskQueue.add(i);
                LOGGER.info("produce: {}", i);
                taskQueue.notifyAll();
            }
        }
    }

    static class Consumer implements Runnable {

        private final List<Integer> taskQueue;

        public Consumer(List<Integer> taskQueue) {
            this.taskQueue = taskQueue;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    consume();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("consume occurs error: {}", e.getMessage(), e);
                }
            }
        }

        private void consume() throws InterruptedException {
            synchronized (taskQueue) {
                while (taskQueue.isEmpty()) {
                    taskQueue.wait();
                }
                Integer i = taskQueue.remove(0);
                LOGGER.info("consume: {}", i);
                taskQueue.notifyAll();
            }
        }
    }

}
