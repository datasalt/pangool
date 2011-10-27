package com.datasalt.pangolin.commons;

import java.util.Iterator;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;



/**
 * This semaphore's permits expire after a certain fixed time.
 * This can be usefull in scenarios where controlling release() is not trivial.
 * <p>
 * Calling release() has the same effect than on a normal semaphore: immediately releasing a resource.
 * 
 * @author pere
 *
 */
public class ExpiringSemaphore {

	// The number of permits
	int capacity;
	// The expiration time for permits
	long expire;
	// We use a timestamp oracle as a unique-id generator. It generates always-increasing unique ids.
	TimestampOracle oracle; 
	
	/**
	 * This flag is updated each time someone blocks on acquire(). If it receives an expired permit, it increments it.
	 */
	int expiredCount = 0;
	
	/**
	 * This semaphore is backed by a delayed queue of fixed size.
	 * There are as many elements in the queue as permits we can grant.
	 * The idea is that each permit has a fixed expiration time, and that it can be acquired again
	 * after that time even if nobody called release() for it.
	 */
	DelayQueue<DelayedElement> queue;

	public ExpiringSemaphore(int capacity, long expire) {
		this.capacity = capacity;
		this.expire = expire;
		oracle = new TimestampOracle();
		queue = new DelayQueue<DelayedElement>();
		for(int i = 0; i < capacity; i++) {
			queue.put(new DelayedElement(oracle.getUniqueTimestamp()));
		}
	}
	
	/**
	 * This is the elements that are contained inside the delay queue.
	 * <p>
	 * Their equals() compare a unique ID.
	 * <p>
	 * Their compare() method is consistent with the getDelay() method and only compares timestamps.
	 */
	public static class DelayedElement implements Delayed {

		Long tstamp;
		Long uniqueId;
		boolean acquired = false;
		
		public DelayedElement(Long id) {
			this.uniqueId = id;
			tstamp = System.currentTimeMillis();
		}
		
		@Override
    public int compareTo(Delayed delayed) {
			long thisDelay = getDelay(TimeUnit.MILLISECONDS);
			long itsDelay  = delayed.getDelay(TimeUnit.MILLISECONDS);
			if(thisDelay > itsDelay) {
				return 1;
			} else if(thisDelay < itsDelay) {
				return -1;
			}
			return 0;
		}
		
		@Override
		public boolean equals(Object obj) {
			return uniqueId.equals(((DelayedElement)obj).uniqueId);
		}

		@Override
    public long getDelay(TimeUnit unit) {
			return unit.convert(tstamp - System.currentTimeMillis(), TimeUnit.MILLISECONDS); 
		}
	}
		
	Object putMonitor = new Object();

	/**
	 * Acquiring means waiting for a permit to be available. Two things may happen:
	 * <ul>
	 *   <li>A permit may be released by calling release(). This way the permit's timestamp will be reset to 'now' and it will be grabbed.</li>
	 *   <li>A permit may expire. This way the permit will also be grabbed.</li>
	 * </ul>
	 * There is a flag "acquired" that is used internally in order to know which permits have been acquired by expiration or by release().
	 *  
	 * @throws InterruptedException
	 */
	public void acquire() throws InterruptedException {
		DelayedElement elem = queue.take();
		synchronized(putMonitor) {
			if(elem.acquired) {
				expiredCount++;
			} else {
				elem.acquired = true;
			}
			elem.tstamp = System.currentTimeMillis() + expire;
			queue.put(elem);
		}
	}
	
	/**
	 * Release() means granting one more permit. To do that, we introspect the DelayQueue and find one element such that its timestamp
	 * tells us that it is no yet available. Then, we remove this element from the queue and we reset its timestamp to 'now' so that it
	 * becomes automatically available, and we insert it in the queue again. All this is atomically performed by blocking on an internal lock.
	 */
	public void release() {
		synchronized(putMonitor) {
			DelayedElement elem = null;
			Iterator<DelayedElement> it = queue.iterator();
			do {
				elem = it.next();
			} while(it.hasNext() && elem.tstamp <= System.currentTimeMillis());
			if(elem.tstamp > System.currentTimeMillis()) { // otherwise we can't release more 
				queue.remove(elem);
				elem.tstamp = System.currentTimeMillis();
				elem.acquired = false;
				queue.put(elem);
			}
		}
	}
	
	/**
	 * This method iterates the queue and returns how many permits have its timestamp > 'now'.
	 * 
	 * @return
	 */
	public int usedPermits() {
		synchronized(putMonitor) {
			int usedPermits = 0;
			Iterator<DelayedElement> elem = queue.iterator();
			while(elem.hasNext()) {
				if(elem.next().tstamp > System.currentTimeMillis()) {
					usedPermits++;
				}
			}
			return usedPermits;
		}
	}
	
	/**
	 * This method returns the number of permits that where acquired by acquire() that were already expired.
	 * 
	 * @return
	 */
	public int expiredCount() {
		return expiredCount;
	}
	
	/**
	 * This method iterates the queue and returns how many permits have its timestamp <= 'now'.
	 * 
	 * @return
	 */
	public int availablePermits() {
		synchronized(putMonitor) {
			int availablePermits = 0;
			Iterator<DelayedElement> elem = queue.iterator();
			while(elem.hasNext()) {
				if(elem.next().tstamp <= System.currentTimeMillis()) {
					availablePermits++;
				}
			}
			return availablePermits;
		}		
	}
	
	/**
	 * This method iterates the queue and returns how many permits have its timestamp <= 'now' and have been acquired but not released.
	 * <p>
	 * In other words, it tells us how many permits we can grab right now so that these permits are also expired.
	 * <p>
	 * In order to know the total number of expired permits by a semaphore when we stop using it, the correct formula to use is:
	 * currentlyExpiredPermits() + expiredCount()
	 * 
	 * @return
	 */
	public int currentlyExpiredPermits() {
		synchronized(putMonitor) {
			int expiredPermits = 0;
			Iterator<DelayedElement> elem = queue.iterator();
			while(elem.hasNext()) {
				DelayedElement el = elem.next();
				if(el.tstamp <= System.currentTimeMillis() && el.acquired) {
					expiredPermits++;
				}
			}
			return expiredPermits;
		}		
	}
}
