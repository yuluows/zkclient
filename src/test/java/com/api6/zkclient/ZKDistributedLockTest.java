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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.api6.zkclient.lock.ZKDistributedLock;
import com.api6.zkclient.util.TestSystem;
import com.api6.zkclient.util.TestUtil;
import com.api6.zkclient.util.ZKServer;

public class ZKDistributedLockTest {
    
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
    public void testDistributedLock() throws Exception{
        final String lockPath = "/zk/lock";
        zkClient.createRecursive(lockPath, null, CreateMode.PERSISTENT);
        final AtomicInteger integer = new AtomicInteger(0);
        final List<String> msgList = new ArrayList<String>();
        for(int i=0;i<20;i++){
            Thread thread1 = new Thread(new Runnable() {
                public void run() {
                    try {
                        ZKDistributedLock lock =new ZKDistributedLock(zkClient,lockPath);
                        lock.lock(0);
                        integer.getAndIncrement();
                        msgList.add("thread "+integer);
                        System.out.println("Thread "+integer+" got lock........");
                        
                        if(integer.get()==3){
                            Thread.sleep(1000);
                        }
                        if(integer.get()==5){
                            Thread.sleep(700);
                        }
                        
                        if(integer.get()==6 || integer.get()==11){
                            Thread.sleep(500);
                        }
                        
                        if(integer.get()==10){
                            Thread.sleep(500);
                        }
                        if(integer.get()==15){
                            Thread.sleep(400);
                        }
                        lock.unlock();
                        System.out.println("thread "+integer+" unlock........");
                        
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        
                    }
                }
            });
            thread1.start();
        }

        //等待事件到达
        int size = TestUtil.waitUntil(20, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return msgList.size();
            }
            
        }, TimeUnit.SECONDS, 100);
        assertThat(size).isEqualTo(20);
        boolean flag = true;
        for(int i =0;i<20;i++){
            if(!msgList.get(i).equals("thread "+(i+1))){
                flag = false;
            }
        }
        assertThat(flag).isTrue();
    }
}
