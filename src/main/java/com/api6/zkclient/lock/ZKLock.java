package com.api6.zkclient.lock;

public interface ZKLock {
	/**
	 * 获得锁
	 * @param timeout 超时时间
	 * 		如果超时间大于0，则会在超时后直接返回false。
	 * 		如果超时时间小于等于0，则会等待直到获取锁为止。
	 * @return 
	 * @return boolean 成功获得锁返回true，否则返回false
	 */
	boolean lock(int timeout);
	/**
	 * 释放锁
	 * @return void
	 */
	void unlock();
}
