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

import com.api6.zkclient.leader.ZKLeaderSelector;
import com.api6.zkclient.leader.ZKLeaderSelectorListener;
import com.api6.zkclient.util.TestSystem;
import com.api6.zkclient.util.TestUtil;
import com.api6.zkclient.util.ZKServer;

public class ZKLeaderSelectorTest {
    
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
    public void testZKLeaderSeletor() throws Exception{
        final String leaderPath = "/zk/leader";
        final List<String> msgList = new ArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(20);
        final CountDownLatch latch1 = new CountDownLatch(20);
        zkClient.createRecursive(leaderPath, null, CreateMode.PERSISTENT);
        final AtomicInteger index = new AtomicInteger(0);
        for(int i=0;i<20;i++){
            final String name = "server:"+index.get();
            
            Thread thread1 = new Thread(new Runnable() {
                public void run() {
                    final ZKClient zkClient1 = ZKClientBuilder.newZKClient()
                            .servers("localhost:"+zkServer.getPort())
                            .sessionTimeout(1000)
                            .build();
                   final ZKLeaderSelector selector = new ZKLeaderSelector(name, true, zkClient1, leaderPath, new ZKLeaderSelectorListener() {
                        
                        @Override
                        public void takeLeadership(ZKClient client, ZKLeaderSelector selector) {
                            msgList.add(name+" I am the leader");
                            System.out.println(name+": I am the leader-"+selector.getLeader());
                            selector.close();
                            latch1.countDown();
                        }
                    });
                    
                    try {
                        System.out.println(name+":waiting");
                        latch.await();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    selector.start();
                    
                    try {
                        latch1.await();
                        
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            thread1.start();
            latch.countDown();
            index.getAndIncrement();
        }
        
        
      //等待事件到达
       int size = TestUtil.waitUntil(20, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return msgList.size();
            }
            
       }, TimeUnit.SECONDS, 100);
       
      assertThat(size).isEqualTo(20);
    }
}
