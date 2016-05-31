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
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.api6.zkclient.lock.ZKHALock;
import com.api6.zkclient.util.TestSystem;
import com.api6.zkclient.util.TestUtil;
import com.api6.zkclient.util.ZKServer;

public class ZKHALockTest {
    
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
     * 测试主从服务锁
     * @throws Exception 
     * @return void
     */
    @Test
    public void testDistributedQueue() throws Exception{
        final String lockPach = "/zk/halock";
        final List<String> msgList = new ArrayList<String>();
        zkClient.createRecursive(lockPach, null, CreateMode.PERSISTENT);
        
        
        Thread thread1 = new Thread(new Runnable() {
            public void run() {
                final ZKClient zkClient1 = ZKClientBuilder.newZKClient()
                        .servers("localhost:"+zkServer.getPort())
                        .sessionTimeout(1000)
                        .build();
                ZKHALock lock = new ZKHALock(zkClient1, lockPach);
                //尝试获取锁，如果获取成功则变为主服务
                lock.lock();
                msgList.add("thread1 is master");
                System.out.println("thread1 now is master!");
                //变为主服务后3秒断开连接并重连，此时变为从服务继续等待获取锁变成主服务
                try {
                    Thread.sleep(1000*3);
                    zkClient1.reconnect();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                
            }
        });
        
        Thread thread2 = new Thread(new Runnable() {
            public void run() {
                final ZKClient zkClient2 = ZKClientBuilder.newZKClient()
                        .servers("localhost:"+zkServer.getPort())
                        .sessionTimeout(1000)
                        .build();
                ZKHALock lock = new ZKHALock(zkClient2, lockPach);
              //尝试获取锁，如果获取成功则变为主服务
                lock.lock();
                msgList.add("thread2 is master");
                System.out.println("thread2 now is master!");
                //变为主服务后3秒断开连接并重连，此时变为从服务继续等待获取锁变成主服务
                try {
                    Thread.sleep(1000*3);
                    zkClient2.reconnect();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        
        Thread thread3 = new Thread(new Runnable() {
            public void run() {
                final ZKClient zkClient3 = ZKClientBuilder.newZKClient()
                        .servers("localhost:"+zkServer.getPort())
                        .sessionTimeout(1000)
                        .build();
                ZKHALock lock = new ZKHALock(zkClient3, lockPach);
              //尝试获取锁，如果获取成功则变为主服务
                lock.lock();
                msgList.add("thread3 is master");
                System.out.println("thread3 now is master!");
                //变为主服务后3秒断开连接并重连，此时变为从服务继续等待获取锁变成主服务
                try {
                    Thread.sleep(1000*3);
                    zkClient3.reconnect();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread1.start();
        thread2.start();
        thread3.start();
        
      //等待事件到达
        int size = TestUtil.waitUntil(3, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return msgList.size();
            }
            
        }, TimeUnit.SECONDS, 100);
      assertThat(size).isEqualTo(3);
        
    }
}
