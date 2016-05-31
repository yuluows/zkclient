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

import com.api6.zkclient.queue.ZKDistributedQueue;
import com.api6.zkclient.util.TestSystem;
import com.api6.zkclient.util.TestUtil;
import com.api6.zkclient.util.ZKServer;

public class ZKDistributedQueueTest {
    
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
     * 测试分布式队列
     * @throws Exception 
     * @return void
     */
    @Test
    public void testDistributedQueue() throws Exception{
        final String rootPath = "/zk/queue";
        zkClient.createRecursive(rootPath, null, CreateMode.PERSISTENT);
        
        final List<String> list1 = new ArrayList<String>();
        final List<String> list2 = new ArrayList<String>();
        for(int i=0;i<20;i++){
            Thread thread1 = new Thread(new Runnable() {
                public void run() {
                    ZKDistributedQueue<String> queue = new ZKDistributedQueue(zkClient, rootPath);
                    queue.offer(Thread.currentThread().getName());
                    list1.add(Thread.currentThread().getName());
                }
            });
            thread1.start();
        }
        
        //等待事件到达
        int size1 = TestUtil.waitUntil(20, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return list1.size();
            }
            
        }, TimeUnit.SECONDS, 100);
        System.out.println(zkClient.getChildren(rootPath));

        for(int i=0;i<20;i++){
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    ZKDistributedQueue<String> queue = new ZKDistributedQueue(zkClient, rootPath);
                    list2.add(queue.poll());
                }
            });
            thread.start();
        }
        //等待事件到达
        int size2 = TestUtil.waitUntil(20, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return list2.size();
            }
            
        }, TimeUnit.SECONDS, 100);
        assertThat(size2).isEqualTo(20);
        boolean flag = true;
        for(int i =0;i<20;i++){
           if(!list1.get(i).equals(list2.get(i))){
               flag = false;
               break;
           }
        }
        assertThat(flag).isTrue();
    }
}
