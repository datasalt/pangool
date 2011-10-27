package com.datasalt.pangolin.commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

import com.datasalt.pangolin.commons.ExpiringSemaphore;

/**
 * Unit tests for the {@link ExpiringSemaphore}
 * 
 * @author pere
 *
 */
public class TestExpiringSemaphore {

	/*
	 * Here we will test that after "expiring" time the acquires() done with no release() afterwards become available again
	 */
	@Test
	public void testPermitExpiring() throws InterruptedException {
		long expiring = 1000;
		long start = System.currentTimeMillis();
		ExpiringSemaphore semaphore = new ExpiringSemaphore(2, expiring);
		assertEquals(semaphore.availablePermits(), 2);
		semaphore.acquire();
		assertEquals(semaphore.availablePermits(), 1);
		semaphore.acquire();
		// next call will block until one permit expires
		assertEquals(semaphore.availablePermits(), 0); 
		semaphore.acquire();
		// expired
		assertTrue(semaphore.expiredCount() >= 1);
		long end = System.currentTimeMillis();
		assertTrue((end - start) >= expiring);
		// let's grab one more one
		semaphore.acquire();
		// expire count = 2
		assertEquals(semaphore.expiredCount(), 2);
	}

	/*
	 * Here we will test the usedPermits() method. We will block on it until it reaches = 0 because of automatic expiring.
	 */
	@Test
	public void testUsedPermits() throws InterruptedException {
		long expiring = 1000;
		ExpiringSemaphore semaphore = new ExpiringSemaphore(4, expiring);
		semaphore.acquire();
		semaphore.acquire();
		semaphore.acquire();
		semaphore.acquire();
		assertEquals(semaphore.availablePermits(), 0);
		assertEquals(semaphore.usedPermits(), 4);
		while(semaphore.usedPermits() > 0) {
			Thread.sleep(100);
		}
		assertEquals(semaphore.availablePermits(), 4);
		/*
		 * Here we are also testing the currentlyExpiredPermits() which shows us a snapshot of the
		 * permits that are expired in the semaphore. It tells us how many permits someone could
		 * grab with acquire() such that these permits have expired (there wasn't release() called for them).
		 */
		assertEquals(semaphore.currentlyExpiredPermits(), 4);
		semaphore.acquire();
		semaphore.acquire();
		semaphore.acquire();
		semaphore.acquire();
		assertEquals(semaphore.expiredCount(), 4);
	}

	/*
	 * Here we will test that release() behaves like a normal semaphore. We won't expire any permit.
	 */
	@Test
	public void testSequentialRelease() throws InterruptedException {
		long expiring = 10000;
		ExpiringSemaphore semaphore = new ExpiringSemaphore(3, expiring);
		/*
		 * Acquire 3
		 */
		semaphore.acquire();
		semaphore.acquire();
		semaphore.acquire();
		long start = System.currentTimeMillis();
		/*
		 * release + acquire
		 */
		semaphore.release();
		semaphore.acquire();
		long end = System.currentTimeMillis();
		assertTrue((end - start) < 50);
		start = System.currentTimeMillis();
		/*
		 * release + acquire
		 */
		semaphore.release();
		semaphore.acquire();
		end = System.currentTimeMillis();
		assertTrue((end - start) < 50);
		start = System.currentTimeMillis();
		/*
		 * release + acquire
		 */
		semaphore.release();
		semaphore.acquire();
		end = System.currentTimeMillis();
		assertTrue((end - start) < 50);
		/*
		 * Assert we have had no expired permits
		 */
		assertEquals(semaphore.expiredCount() + semaphore.currentlyExpiredPermits(), 0);
	}

	/*
	 * Here we will test that release() is an operation that awakes threads blocking on acquire()
	 * One thread will block on an acquire() and the other (main) thread will release() a resource.
	 */
	@Test
	public void testConcurrentRelease() throws InterruptedException {
		final long expiring = 10000;
		final ExpiringSemaphore semaphore = new ExpiringSemaphore(3, expiring);

		Thread t = new Thread() {

			public void run() {
				try {
					semaphore.acquire();
					semaphore.acquire();
					long start = System.currentTimeMillis();
					semaphore.acquire();
					long end = System.currentTimeMillis();
					assertTrue((end - start) < 50);
				} catch(InterruptedException e) {
					e.printStackTrace();
				}
			}
		};

		semaphore.acquire();
		t.start();
		semaphore.release();
		t.join();
		assertEquals(semaphore.expiredCount(), 0);
	}

	/*
	 * This parallelism thread just asserts that the semaphore can be used like a normal semaphore.
	 * We will use 10 threads that will compete for a 1-sized semaphore and execute a block of code like this:
	 * 
	 * flag = true
	 * flag = false
	 * 
	 * The threads will assert that flag is always false when they have acquired() the semaphore, otherwise
	 * the semaphore is not acting like a mutex.
	 */
	@Test
	public void testParallelism() throws InterruptedException {
		final ExpiringSemaphore semaphore = new ExpiringSemaphore(1, 2000);
		final BooleanWritable flag = new BooleanWritable();

		Thread[] threads = new Thread[10];

		for(int i = 0; i < 10; i++) {
			threads[i] = new Thread() {
				@Override
				public void run() {
					try {
						for(int i = 0; i < 200; i++) {
							semaphore.acquire();
							if(flag.get()) {
								throw new RuntimeException("Semaphore is not behaving as expected");
							}
							flag.set(true);
							flag.set(false);
							semaphore.release();
						}
					} catch(InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			};
		}
		
		for(int i = 0; i < 10; i++) {
			threads[i].start();
		}
		
		for(int i = 0; i < 10; i++) {
			threads[i].join();
		}
	}
}
