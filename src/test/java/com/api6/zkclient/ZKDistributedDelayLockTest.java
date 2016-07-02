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
package com.api6.zkclient;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.api6.zkclient.lock.ZKDistributedDelayLock;
import com.api6.zkclient.util.TestSystem;
import com.api6.zkclient.util.TestUtil;
import com.api6.zkclient.util.ZKServer;

public class ZKDistributedDelayLockTest {
    
    private TestSystem testSystem = TestSystem.getInstance();
    private ZKServer zkServer = null;
    private ZKClient zkClient = null;
    @Before
    public void before() {
        zkServer = testSystem.getZKserver();
        zkClient = ZKClientBuilder.newZKClient()
                                .servers("localhost:"+zkServer.getPort())
                                .sessionTimeout(1000)
                                .build();
    }
    
    @After
    public void after(){
        testSystem.cleanup(zkClient);
    }
   
    /**
     * 测试分布式锁
     * @throws Exception 
     * @return void
     */
    @Test
    public void testDistributedDelayLock() throws Exception{
        final String lockPach = "/zk/delylock1";
        final List<String> msgList = new ArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(5);
        zkClient.createRecursive(lockPach, null, CreateMode.PERSISTENT);
        final AtomicInteger index = new AtomicInteger(0);
        for(int i=0;i<5;i++){
            
            Thread thread1 = new Thread(new Runnable() {
                public void run() {
                    final ZKClient zkClient1 = ZKClientBuilder.newZKClient()
                            .servers("localhost:"+zkServer.getPort())
                            .sessionTimeout(1000)
                            .build();
                    ZKDistributedDelayLock lock = ZKDistributedDelayLock.newInstance(zkClient1, lockPach);
                    
                    try {
                        latch.await();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    lock.lock();
                    System.out.println(Thread.currentThread().getName()+":lock....");
                    msgList.add(Thread.currentThread().getName()+":unlock");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName()+":unlock....");
                    lock.unlock();
                }
            });
            thread1.start();
            latch.countDown();
            index.getAndIncrement();
        }
        
        
      //等待事件到达
       int size = TestUtil.waitUntil(5, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return msgList.size();
            }
            
       }, TimeUnit.SECONDS, 100);
       
      assertThat(size).isEqualTo(5);
    }
    
 
    
  /*  @Test
    public void testDistributedDelayLock1() throws Exception{
        final String lockPach = "/zk/delylock1";
        ZKClient client = ZKClientBuilder.newZKClient()
                .servers("192.168.1.102:2181")
                .sessionTimeout(10000)
                .build();
        client.createRecursive(lockPach, null, CreateMode.PERSISTENT);
        ZKDistributedDelayLock lock = ZKDistributedDelayLock.newInstance(client, lockPach);
        lock.lock();
        System.out.println("sleep");
        Thread.sleep(20000);
        System.out.println(client.getData(lockPach+"/lock"));
        
    }*/
    
    
}
