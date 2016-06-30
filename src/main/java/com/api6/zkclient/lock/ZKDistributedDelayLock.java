/**
 *Copyright 2016 zhaojie
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
package com.api6.zkclient.lock;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.api6.zkclient.ZKClient;
import com.api6.zkclient.exception.ZKException;
import com.api6.zkclient.exception.ZKNoNodeException;
import com.api6.zkclient.exception.ZKNodeExistsException;
import com.api6.zkclient.listener.ZKNodeListener;
import com.api6.zkclient.listener.ZKStateListener;

/**
 * 带延迟获取的分布式锁
 * 此分布式锁主要针对网络闪断的情况。
 * 不带延迟功能的分布式锁：某个线程获取了分布式锁，但是网络出现2秒的断开，只要网络断开就会释放锁。
 * 带延迟功能的分布式锁：例如设置了delayTimeMillis的值为5000，那么只要网络断开没有超过这个延迟时间，则当前线程还有依旧可以重新获得锁。
 * 
 * 非线程安全，每个线程请单独创建实例
 * @author: zhaojie/zh_jie@163.com.com 
 * @version: 2016年5月31日 下午3:48:36
 */
public class ZKDistributedDelayLock implements ZKLock {
    private final static Logger logger = LoggerFactory.getLogger(ZKDistributedDelayLock.class);
    private final ExecutorService executorService;
    private final ZKNodeListener nodeListener;
    private final ZKStateListener stateListener;
    private final ZKClient client;
    private final String lockPath;
    private Semaphore semaphore;
    private final AtomicBoolean hasLock = new AtomicBoolean(false);
    private String lockNodeData;
    private final AtomicInteger delayTimeMillis = new AtomicInteger(0);
    
    private  ZKDistributedDelayLock(final ZKClient client,String lockPach) {
        this.client = client;
        this.lockPath = lockPach;
        this.executorService = Executors.newSingleThreadExecutor();
        this.nodeListener = new ZKNodeListener() {
            
            @Override
            public void handleSessionExpired(String path) throws Exception {}
            
            @Override
            public void handleDataDeleted(String path) throws Exception {
                /*//同步方式
                 if(!hasLock()){//如果当前没有持有锁
                    //为了解决网络闪断问题，先等待一段时间，再重新竞争锁
                    Thread.currentThread().sleep(delayTimeMillis.longValue());
                    semaphore.release();
                }*/
                
                //异步方式
                executorService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        if(!hasLock()){//如果当前没有持有锁
                            //为了解决网络闪断问题，先等待一段时间，再重新竞争锁
                            Thread.currentThread().sleep(delayTimeMillis.longValue());
                            //如果之前获得锁的线程解除了锁定，则所有等待的线程都重新尝试，这里使得信号量加1
                            semaphore.release();
                        }
                        return null;
                    }
               });
            }
            
            @Override
            public void handleDataCreated(String path, Object data) throws Exception {
            }
            
            @Override
            public void handleDataChanged(String path, Object data) throws Exception {}
        };
        
        this.stateListener = new ZKStateListener() {
            @Override
            public void handleStateChanged(KeeperState state) throws Exception {
               if(state == KeeperState.SyncConnected){//如果重新连接
                   if(hasLock.get()){//现在持有锁
                      //重新创建节点
                       try {
                           client.create(lockPath+"/lock", lockNodeData, CreateMode.EPHEMERAL);
                       } catch (ZKNodeExistsException e) {
                           hasLock.set(false);
                       }
                   }
               }
            }
            
            @Override
            public void handleSessionError(Throwable error) throws Exception {}
            
            @Override
            public void handleNewSession() throws Exception {}
        };
    }
    
    /**
     * 创建分布式锁实例的工厂方法
     * @param client
     * @param lockPach
     * @return 
     * @return ZKDistributedLock
     */
    public static ZKDistributedDelayLock newInstance(ZKClient client,String lockPach) {
       if(!client.exists(lockPach)){
            throw new ZKNoNodeException("The lockPath is not exists!,please create the node.[path:"+lockPach+"]");
       }
       ZKDistributedDelayLock zkDistributedDelayLock = new ZKDistributedDelayLock(client, lockPach);
       client.listenNodeChanges(lockPach+"/lock", zkDistributedDelayLock.nodeListener);
       client.listenStateChanges(zkDistributedDelayLock.stateListener);
       try {
           client.create(lockPach+"/nodes", null, CreateMode.PERSISTENT);
        } catch (ZKNodeExistsException e) {
            //已被其他线程创建，这里忽略就可以
        }
       
       return zkDistributedDelayLock;
    }
    
    @Override
    public boolean lock(){
        return lock(0);
    }
    
    /**
     * 获得锁,默认的延迟时间5000毫秒
     * @param timeout 超时时间
     *         如果超时间大于0，则会在超时后直接返回false。
     *         如果超时时间小于等于0，则会等待直到获取锁为止。
     * @return 
     * @return boolean 成功获得锁返回true，否则返回false
     */
    public boolean lock(int timeout) {
        return lock(timeout,5000);
    }
    
    /**
     * 获得锁路径
     * @return 
     * @return String
     */
    public String getLockPath(){
        return lockPath+"/lock";
    }
    
    /**
     * 
     * @param timeout
     * @param delayTimeMillis
     * @return 
     * @return boolean
     */
    public boolean lock(int timeout,int delayTimeMillis){
        this.delayTimeMillis.set(delayTimeMillis);
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
              //信号量为0，线程就会一直等待直到数据变成正数
                semaphore = new Semaphore(0);
                client.create(lockPath+"/lock", lockNodeData, CreateMode.EPHEMERAL);
                hasLock.set(true);
                return true;
            } catch (ZKNodeExistsException e) {
                try {
                     semaphore.acquire();
                } catch (InterruptedException interruptedException) {
                    return false;
                }
            }
            //超时处理
            if (timeout > 0 && (System.currentTimeMillis() - startTime) >= timeout) {
                return false;
            }
        }
    }

    
    public void setLockNodeData(String lockNodeData){
       this.lockNodeData = lockNodeData;
    }
    
    public boolean hasLock(){
        return hasLock.get();
    }
    
    @Override
    public boolean unlock() {
        if(hasLock()){
            client.unlistenNodeChanges(lockPath+"/lock", nodeListener);
            client.unlistenStateChanges(stateListener);
            executorService.shutdownNow();
            boolean flag = client.delete(lockPath+"/lock");
            hasLock.set(false);
            return flag;
        }
        throw new ZKException("not locked can not unlock!");
    }
 
}
