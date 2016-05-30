/**
 *Copyright [2016] [zhaojie]
 *
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License.
 *You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
 */
package com.api6.zkclient.connection;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.api6.zkclient.exception.ZKException;
import com.api6.zkclient.exception.ZKInterruptedException;
import com.api6.zkclient.exception.ZKTimeoutException;
import com.api6.zkclient.lock.ZKEventLock;

/**
 * ZK客户端连接接口，定义了ZKConnection所需实现的方法
 * @author: zhaojie/zh_jie@163.com.com 
 */
public interface ZKConnection {
	/**
	 * 连接zookeeper服务器
	 * @param watcher 
	 * @return void
	 */
	void connect(Watcher watcher);
	
	/**
	 * 重新连接ZooKeeper服务器
	 * @param watcher 
	 * @return void
	 */
	void reconnect(Watcher watcher);
	
	
	/**
	 * 尝试连接，只到连接成功为止
	 * 对于连接失败，或者会话超时，会发起重新连接的请求，只到连接成功。
	 * @param callable
	 * @return
	 * @throws ZKInterruptedException 如果操作被Interrupted抛出异常
	 * @throws ZKException 所有的ZooKeeper异常发生会抛出此异常
	 * @throws RuntimeException 执行时异常
	 * @throws ZKTimeoutException 如果设置了超时时间operationRetryTimeoutInMillis，
	 * 			则会在尝试时间超过operationRetryTimeoutInMillis时
	 *			抛出异常 
	 * @return T
	 */
	<T> T retryUntilConnected(Callable<T> callable) 
			throws ZKInterruptedException, ZKTimeoutException, ZKException, RuntimeException;
	
	/**
	 * 等待直到连接成功
	 * @param timeout 尝试的超时时间
	 * @param timeUnit 时间单位
	 * @return
	 * @throws ZKInterruptedException  
	 * 			如果操作被interrupted抛出中断异常
	 * @return boolean
	 */
	boolean waitUntilConnected(long timeout, TimeUnit timeUnit) throws ZKInterruptedException;
	
	/**
	 * 关闭与zookeeper服务端的连接
	 * @throws InterruptedException 
	 * @return void
	 */
    void close() throws InterruptedException;
   
    /**
     * 获取到当前ZooKeeper客户端的状态
     * @return 
     * @return KeeperState
     */
    KeeperState getCurrentState();
    /**
     * 设置ZooKeeper客户端的状态
     * @param currentState 
     * @return void
     */
	void setCurrentState(KeeperState currentState);
    
	/**
	 * 获得EventLock
	 * @return 
	 * @return ZKEventLock
	 */
    ZKEventLock getEventLock();
    
    /**
     * 尝试获得EventLock
     * @return void
     */
    void acquireEventLock();
    /**
     * 释放EventLock
     * @return void
     */
	void releaseEventLock();
	/**
     * 获得可中断的EventLock，在获取锁的时候被阻塞后，如果当前线程收到interrupt信号，
     * 此线程会被唤醒并处理InterruptedException，不会一直阻塞下去
     * @return void
     */
	void acquireEventLockInterruptibly();
	
	
	/**
	 * 添加指定的scheme:auth认证信息连接服务器
	 * @param scheme
	 * @param auth 
	 * @return void
	 */
	void addAuthInfo(String scheme, byte[] auth);
	
	 /**
     * 返回服务连接
     * @return 
     * @return String
     */
    String getServers();
    
    /**
     * 获得原生ZooKeeper客户端
     * @return 
     * @return ZooKeeper
     */
    ZooKeeper getZooKeeper();
    
}
