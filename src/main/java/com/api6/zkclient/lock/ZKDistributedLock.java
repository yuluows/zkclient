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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.api6.zkclient.ZKClient;
import com.api6.zkclient.exception.ZKInterruptedException;
import com.api6.zkclient.exception.ZKNoNodeException;
import com.api6.zkclient.listener.ZKChildCountListener;

/**
 * 分布式锁
 * 非线程安全，每个线程请单独创建实例
 * @author: zhaojie/zh_jie@163.com.com 
 * @version: 2016年5月31日 下午3:48:36
 */
public class ZKDistributedLock implements ZKLock {
    private final static Logger logger = LoggerFactory.getLogger(ZKDistributedLock.class);
    private final Map<String, Semaphore> waitLocks = new ConcurrentHashMap<String, Semaphore>();
    
    private ZKClient client;
    private String lockPath;
    private String currentSeq;
    private Semaphore semaphore;
    
    public  ZKDistributedLock(ZKClient client,String lockPach) {
        this.client = client;
        this.lockPath = lockPach;
        
        //对lockPath进行子节点数量的监听
        client.listenChildCountChanges(lockPach, new ZKChildCountListener() {
            @Override
            public void handleSessionExpired(String path, List<String> children) throws Exception {
            /*    if(check(currentSeq, children)){
                    semaphore.release();
                }*/
            }
            
            @Override
            public void handleChildCountChanged(String path, List<String> children) throws Exception {
                if(check(currentSeq, children)){
                    semaphore.release();
                }
            }
        });
        
        if(!client.exists(lockPach)){
            throw new ZKNoNodeException("The lockPath is not exists!,please create the node.[path:"+lockPach+"]");
        }
    }
    
    @Override
    public boolean lock(int timeout) {
        //信号量为0，线程就会一直等待直到数据变成正数
        semaphore = new Semaphore(0);
        String newPath = client.create(lockPath+"/1", null, CreateMode.EPHEMERAL_SEQUENTIAL);
        String[] paths = newPath.split("/");
        currentSeq = paths[paths.length - 1];
        waitLocks.put(currentSeq, semaphore);
        boolean getLock = false;
        try {
            if(timeout>0){
                getLock = semaphore.tryAcquire(timeout, TimeUnit.MICROSECONDS);
            }else{
                semaphore.acquire();
                getLock = true;
            }
        } catch (InterruptedException e) {
            throw new ZKInterruptedException(e);
        }
        if (getLock) {
            logger.debug("get lock successful.");
        } else {
            logger.debug("failed to get lock.");
        }
        return getLock;
    }

    @Override
    public void unlock() {
        client.delete(lockPath+"/"+currentSeq);
    }
    
    
    /**
     * 判断路径是否可以获得锁，如果checkPath 对应的序列是所有子节点中最小的，则可以获得锁。
     * @param checkPath 需要判断的路径
     * @param allChildPath 所有的子路径
     * @return boolean 如果可以获得锁返回true，否则，返回false;
     */
    private boolean check(String checkPath, List<String> allChildPath) {
        if(allChildPath==null || !allChildPath.contains(checkPath)){
            return false;
        }
        Long chePathSeq = Long.parseLong(checkPath);
        boolean isLock = true;
        for (String path : allChildPath) {
            Long pathSeq = Long.parseLong(path);
            if (chePathSeq > pathSeq) {
                isLock = false;
                continue;
            }
        }
        return isLock;
    }

}
