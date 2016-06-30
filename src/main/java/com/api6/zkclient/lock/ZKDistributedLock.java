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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
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
    private final ZKChildCountListener countListener;
    private final ZKClient client;
    private final String lockPath;
    private String currentSeq;
    private Semaphore semaphore;
    private String lockNodeData;
    
    private  ZKDistributedLock(ZKClient client,String lockPach) {
        this.client = client;
        this.lockPath = lockPach;
        this.countListener =  new ZKChildCountListener() {
            @Override
            public void handleSessionExpired(String path, List<String> children) throws Exception {
                //ignore
            }
            
            @Override
            public void handleChildCountChanged(String path, List<String> children) throws Exception {
                if(check(currentSeq, children)){
                    semaphore.release();
                }
            }
        };
    }
    
    /**
     * 创建分布式锁实例的工厂方法
     * @param client
     * @param lockPach
     * @return 
     * @return ZKDistributedLock
     */
    public static ZKDistributedLock newInstance(ZKClient client,String lockPach) {
       if(!client.exists(lockPach)){
            throw new ZKNoNodeException("The lockPath is not exists!,please create the node.[path:"+lockPach+"]");
       }
       ZKDistributedLock zkDistributedLock = new ZKDistributedLock(client, lockPach);
       //对lockPath进行子节点数量的监听
       client.listenChildCountChanges(lockPach,zkDistributedLock.countListener);
       return zkDistributedLock;
    }
    
    @Override
    public boolean lock(){
        return lock(0);
    }
    
    /**
     * 获得锁
     * @param timeout 超时时间
     *         如果超时间大于0，则会在超时后直接返回false。
     *         如果超时时间小于等于0，则会等待直到获取锁为止。
     * @return 
     * @return boolean 成功获得锁返回true，否则返回false
     */
    public boolean lock(int timeout) {
        //信号量为0，线程就会一直等待直到数据变成正数
        semaphore = new Semaphore(0);
        String newPath = client.create(lockPath+"/1", lockNodeData, CreateMode.EPHEMERAL_SEQUENTIAL);
        String[] paths = newPath.split("/");
        currentSeq = paths[paths.length - 1];
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
    

    public void setLockNodeData(String lockNodeData){
       this.lockNodeData = lockNodeData;
    }
    
    /**
     * 释放锁
     * @return 
     * @return boolean 
     *      如果释放锁成功返回true，否则返回false
     *      释放锁失败只有一种情况，就是线程正好获得锁，在释放之前，
     *      与服务器断开连接，这时候服务器会自动删除EPHEMERAL_SEQUENTIAL节点。
     *      在会话过期之后再删除节点就会删除失败，因为路径已经不存在了。
     */
    @Override
    public boolean unlock() {
       client.unlistenChildChanges(lockPath, countListener);
       return client.delete(lockPath+"/"+currentSeq);
    }
    
    /**
     * 获得所有参与者的节点名称
     * @return 
     * @return List<String>
     */
    public List<String> getParticipantNodes(){
       List<String> children = client.getChildren(lockPath);
        Collections.sort
        (
              children,
            new Comparator<String>()
            {
                @Override
                public int compare(String lhs, String rhs)
                {
                    return lhs.compareTo(rhs);
                }
            }
        );
        return children;
    }
    
    /**
     * 判断路径是否可以获得锁，如果checkPath 对应的序列是所有子节点中最小的，则可以获得锁。
     * @param checkPath 需要判断的路径
     * @param children 所有的子路径
     * @return boolean 如果可以获得锁返回true，否则，返回false;
     */
    private boolean check(String checkPath, List<String> children) {
        if(children==null || !children.contains(checkPath)){
            return false;
        }
        //判断checkPath 是否是children中的最小值，如果是返回true，不是返回false
        Long chePathSeq = Long.parseLong(checkPath);
        boolean isLock = true;
        for (String path : children) {
            Long pathSeq = Long.parseLong(path);
            if (chePathSeq > pathSeq) {
                isLock = false;
                break;
            }
        }
        return isLock;
    }

}
