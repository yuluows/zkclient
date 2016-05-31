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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.api6.zkclient.exception.ZKException;
import com.api6.zkclient.exception.ZKNoNodeException;
import com.api6.zkclient.listener.ZKChildCountListener;
import com.api6.zkclient.listener.ZKChildDataListener;
import com.api6.zkclient.listener.ZKNodeListener;
import com.api6.zkclient.listener.ZKStateListener;
import com.api6.zkclient.serializer.BytesSerializer;
import com.api6.zkclient.serializer.SerializableSerializer;
import com.api6.zkclient.util.TestSystem;
import com.api6.zkclient.util.TestUtil;
import com.api6.zkclient.util.ZKServer;

public class ZKClientTest {
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
        //zkClient = new ZKClient("192.168.1.104:2181");
    }
    
    @After
    public void after(){
        testSystem.cleanup(zkClient);
    }
    
    @Test
    public void testZKClient(){
        String address = "localhost:"+zkServer.getPort();
        ZKClient zkClient1 = new ZKClient(address);
        ZKClient zkClient2 = new ZKClient(address,500);
        ZKClient zkClient3 = new ZKClient(address,500,1000*60);
        ZKClient zkClient4 = new ZKClient(address,500,1000*60,new BytesSerializer());
        ZKClient zkClient5 = new ZKClient(address,500,1000*60,new BytesSerializer(),Integer.MAX_VALUE);
        ZKClient zkClient6 = new ZKClient(address,500,1000*60,new BytesSerializer(),Integer.MAX_VALUE,2);
        ZKClient zkClient7 = ZKClientBuilder.newZKClient(address)
                            .sessionTimeout(1000)
                            .serializer(new SerializableSerializer())
                            .eventThreadPoolSize(1)
                            .retryTimeout(1000*60)
                            .connectionTimeout(Integer.MAX_VALUE)
                            .build();
                            
        zkClient1.close();
        zkClient1.close();
        zkClient2.close();
        zkClient3.close();
        zkClient4.close();
        zkClient5.close();
        zkClient6.close();
        zkClient7.close();
    }
    
    @Test
    public void testZKClientStartTimeout(){
        try {
            ZKClient zkClient1 = new ZKClient("localhost:"+zkServer.getPort(),500,1000*60,new BytesSerializer(),1);
            fail("expected throw ZKException");
        } catch (ZKException e) {
            //expected
        }
        
    }
    /**
     * 新增节点测试
     * @return void
     */
    @Test
    public void testCreate()  {
        zkClient.create("/test1", "123", CreateMode.EPHEMERAL);
        zkClient.create("/test1-1",123,CreateMode.EPHEMERAL_SEQUENTIAL);
        zkClient.create("/test1-2",123,CreateMode.PERSISTENT);
        zkClient.create("/test1-3",123,CreateMode.PERSISTENT_SEQUENTIAL);
    }
    
    /**
     * 获取数据
     * @return void
     */
    @Test
    public void testGtData()  {
        String path = "/test2";
        String value = "123";
        zkClient.create(path, value, CreateMode.EPHEMERAL);
        assertThat(zkClient.getData(path)).isEqualTo(value);
        Stat stat = new Stat();
        zkClient.getData(path, stat);
        assertTrue(stat.getDataLength()>0);
        
        assertThat(zkClient.getData("/nonode", true)).isNull();
        
    }
    
    /**
     * 更新数据
     * @return void
     */
    @Test
    public void testSetData()  {
        String path = "/test3";
        zkClient.create(path, "123", CreateMode.EPHEMERAL);
        zkClient.setData(path, "456");
        assertThat(zkClient.getData(path)).isEqualTo("456");
    }


    
    /**
     * 等待直到节点创建
     * @return void
     */
    @Test
    public void testWaitUntilExists()  {
        final String path = "/test4";
        Thread thread = new Thread(new Runnable() {
            
            @Override
            public void run() {
                try {
                    Thread.sleep(1000*3);
                    zkClient.create(path, "111", CreateMode.PERSISTENT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        boolean flag = zkClient.waitUntilExists(path, TimeUnit.MILLISECONDS, 1000*5);
        assertTrue(flag);
        
    }
    
    /**
     * 等待节点创建超时
     * @return void
     */
    @Test
    public void testWaitUntilExistsTimeout()  {
        final String path = "/test5";
        Thread thread = new Thread(new Runnable() {
            
            @Override
            public void run() {
                try {
                    Thread.sleep(1000*5);
                    zkClient.create(path, "111", CreateMode.PERSISTENT);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        boolean flag = zkClient.waitUntilExists(path, TimeUnit.MILLISECONDS, 1000*3);
        assertFalse(flag);
        
    }
    
    /**
     * 删除节点
     * @return void
     */
    @Test
    public void testDeleteData()  {
        String path = "/test6";
        zkClient.create(path, "123", CreateMode.EPHEMERAL);
        zkClient.delete(path);
        try {
            zkClient.getData(path);
            fail("expected exception!");
        } catch (ZKNoNodeException e) {
            // expected
        }
    }
    
    /**
     * 级联删除子节点
     * @return void
     */
    @Test
    public void testDeleteRecursive(){
        String path = "/test7";
        zkClient.create(path, "123", CreateMode.PERSISTENT);
        zkClient.create(path+"/a", "123", CreateMode.PERSISTENT);
        zkClient.create(path+"/b", "123", CreateMode.PERSISTENT);
        assertThat(zkClient.getChildren(path).size()).isEqualTo(2);
        
        zkClient.deleteRecursive(path);
        try {
            zkClient.getData(path);
            fail("expected exception!");
        } catch (ZKNoNodeException e) {
            // expected
        }
    }
    
    @Test
    public void testPersistentNodeCreateRecursivet()  {
        String path = "/test8/1/2/3";
        zkClient.deleteRecursive(path);
        zkClient.createRecursive(path, "abc", CreateMode.PERSISTENT);
        assertThat(zkClient.getData(path)).isEqualTo("abc");
    }
    
    @Test
    public void testEphemeralNodeCreateRecursive()  {
        String path = "/test9/1/2/3";
        zkClient.deleteRecursive(path);
        zkClient.createRecursive(path, "efg", CreateMode.EPHEMERAL);
        assertThat(zkClient.getData(path)).isEqualTo("efg");
    }
    
    
    /**
     * 创建普通的EPHEMERAL类型节点，此节点在连接断开后会消失
     * @return void
     */
    @Test
    public void testCreateEphemeral1()  {
        String path = "/test12";
        zkClient.create(path, "123", CreateMode.EPHEMERAL);
        zkClient.reconnect();
        assertFalse(zkClient.exists(path, false));
    }
    
    /** 创建特殊的EPHEMERAL类型节点，此节点在连接重连后会自动创建
     * @return void
     */
    @Test
    public void testCreateEphemeral2()  {
        String path = "/test10";
        zkClient.createEphemerale(path, "123", false);
        String retPath = zkClient.createEphemerale(path+"1", "456", true);
        zkClient.reconnect();
        zkClient.waitUntilExists(path, TimeUnit.MICROSECONDS, 1000*2);
        zkClient.waitUntilExists(retPath, TimeUnit.MICROSECONDS, 1000*2);
        
    }
    
    /**
     *  创建特殊的PERSISTENT_SEQUENTIAL类型节点，此节点在连接重连后会自动创建
     */
    @Test
    public void testCreateEphemeral3()  {
        String path = "/test11";
        zkClient.create(path, "123", CreateMode.PERSISTENT);
        zkClient.createEphemerale(path+"/a", "123", true);
        zkClient.reconnect();
        List<String> children = zkClient.getChildren(path);
        assertThat(1==children.size());
        System.out.println("================children:"+children);
    }
    
    
    /**
     * 监听节点的创建，删除，和数据改变
     * @return void
     * @throws Exception 
     */
    @Test
    public void testListenNodeChange() throws Exception  {
        String path = "/test12";
        final List<String> msgList = new ArrayList<String>();
        Integer size = 0;
        zkClient.listenNodeChanges(path, new ZKNodeListener() {
            @Override
            public void handleSessionExpired(String path) throws Exception {
                msgList.add("session  expired ["+path+"]");
                System.out.println("session  expired ["+path+"]");
            }
            
            @Override
            public void handleDataDeleted(String path) throws Exception {
                msgList.add("node is deleted ["+path+"]");
                System.out.println("node is deleted ["+path+"]");
            }
            
            @Override
            public void handleDataCreated(String path, Object data) throws Exception {
                msgList.add("node is created ["+path+"]");
                System.out.println("node is created ["+path+"]");
            }
            
            @Override
            public void handleDataChanged(String path, Object data) throws Exception {
                msgList.add("node is changed ["+path+"]");
                System.out.println("node is changed ["+path+"]");
            }
        });
        
        //创建节点
        zkClient.create(path, "123", CreateMode.PERSISTENT);
        //等待事件到达
        size = TestUtil.waitUntil(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return msgList.size();
            }
            
        }, TimeUnit.SECONDS, 5);
        
        assertThat(1).isEqualTo(size);
        
        
        //更新数据
        zkClient.setData(path, "456");
        //等待事件到达
        size = TestUtil.waitUntil(2, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return msgList.size();
            }
            
        }, TimeUnit.SECONDS, 5);
        assertThat(2).isEqualTo(size);
        
        //删除节点
        zkClient.delete(path);
        
        //等待事件到达
        size = TestUtil.waitUntil(3, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return msgList.size();
            }
            
        }, TimeUnit.SECONDS, 5);
        
        assertThat(3).isEqualTo(size);
        
        
    }
    
    /**
     * 监听子节点的数量变化
     * @return void
     * @throws Exception 
     */
    @Test
    public void testListenChildCountChange() throws Exception  {
        String path = "/test13";
        final List<String> expiredMsg = new ArrayList<String>();
        final AtomicInteger childrenSize = new AtomicInteger(-1);
        zkClient.listenChildCountChanges(path, new ZKChildCountListener() {
            
            @Override
            public void handleSessionExpired(String path, List<String> children) throws Exception {
                expiredMsg.add("session expired.");
            }
            
            @Override
            public void handleChildCountChanged(String path, List<String> children) throws Exception {
                if(children==null){
                    children = new ArrayList<String>();
                }
                childrenSize.set(children.size());
            }
        });
        
        Integer size =childrenSize.get();
        //创建根节点
        zkClient.create(path, null, CreateMode.PERSISTENT);
        size= TestUtil.waitUntil(0, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(0).isEqualTo(size);
        
        //创建第一个子节点
        childrenSize.set(-1);
        zkClient.create(path+"/a", 123, CreateMode.EPHEMERAL);
        size = TestUtil.waitUntil(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(1).isEqualTo(size);
        
        //创建第二个子节点
        childrenSize.set(-1);
        zkClient.create(path+"/b", 123, CreateMode.EPHEMERAL);
        size = TestUtil.waitUntil(2, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(2).isEqualTo(size);
        
        //删除第一个子节点
        childrenSize.set(-1);
        zkClient.delete(path+"/a");
        size = TestUtil.waitUntil(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(1).isEqualTo(size);
        
        //删除根节点及子节点
        childrenSize.set(-1);
        zkClient.deleteRecursive(path);
        size = TestUtil.waitUntil(0, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(0).isEqualTo(size);
        
    }
    
    /**
     * 监听子节点的数量变化，以及子节点数据的变化
     * @return void
     * @throws Exception 
     */
    @Test
    public void testListenChildCountAndDataChange() throws Exception  {
        String path = "/test14";
        final AtomicInteger childrenSize = new AtomicInteger(-1);
        final String[] changedDatas = {""};
        zkClient.listenChildDataChanges(path, new ZKChildDataListener() {
            @Override
            public void handleSessionExpired(String path, Object data) throws Exception {
                //ignore 
            }
            
            @Override
            public void handleChildDataChanged(String path, Object data) throws Exception {
                changedDatas[0] = (String)data;
            }
            
            @Override
            public void handleChildCountChanged(String path, List<String> children) throws Exception {
                if(children==null){
                    children = new ArrayList<String>();
                }
                childrenSize.set(children.size());
            }
        });
        
        Integer size =childrenSize.get();
        //创建根节点
        zkClient.create(path, null, CreateMode.PERSISTENT);
        size= TestUtil.waitUntil(0, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(0).isEqualTo(size);
        
        //创建第一个子节点
        childrenSize.set(-1);
        zkClient.create(path+"/a", 123, CreateMode.EPHEMERAL);
        size = TestUtil.waitUntil(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(1).isEqualTo(size);
        
        //修改第一个子节点
        zkClient.setData(path+"/a", "456");
        String theData = TestUtil.waitUntil("456", new Callable<String>() {

            @Override
            public String call() throws Exception {
                return changedDatas[0];
            }
        }, TimeUnit.SECONDS, 5);
        assertThat(theData).isEqualTo(changedDatas[0]);
        
        //创建第二个子节点
        childrenSize.set(-1);
        zkClient.create(path+"/b", 123, CreateMode.EPHEMERAL);
        size = TestUtil.waitUntil(2, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(2).isEqualTo(size);
        
        //删除第一个子节点
        childrenSize.set(-1);
        zkClient.delete(path+"/a");
        size = TestUtil.waitUntil(1, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(1).isEqualTo(size);
        
        //删除根节点及子节点
        childrenSize.set(-1);
        zkClient.deleteRecursive(path);
        size = TestUtil.waitUntil(0, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return childrenSize.get();
            }
        }, TimeUnit.SECONDS, 5);
        
        assertThat(0).isEqualTo(size);
    }
    
    @Test
    public void testStateListener() throws Exception{
        final List<String> msgList = new ArrayList<String>();
        int msgSize = 0;
        zkClient.listenStateChanges(new ZKStateListener() {
            
            @Override
            public void handleStateChanged(KeeperState state) throws Exception {
                String msg = "state changed:"+state;
                msgList.add(msg);
                System.out.println(msg);
            }
            
            @Override
            public void handleSessionError(Throwable error) throws Exception {
                String msg = "session error:"+error.getMessage();
                msgList.add(msg);
                System.out.println(msg);
            }
            
            @Override
            public void handleNewSession() throws Exception {
                String msg = "create new session";
                msgList.add(msg);
                System.out.println(msg);
            }
        });
        
        //重启server
        zkServer.shutdown();
        //5秒后重启server;
        Thread thread1 = new Thread(new Runnable() {
            
            @Override
            public void run() {
                try {
                    Thread.sleep(1000*5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                zkServer.start();
            }
        });
        
        thread1.start();
        thread1.join();
        
        
        //等待事件到达 Disconnected SyncConnected
        msgSize = TestUtil.waitUntil(2, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return msgList.size();
            }
            
        }, TimeUnit.SECONDS, 10);
        assertThat(msgSize).isEqualTo(2);
        
        
    }
    
 
    @Test
    public void testUnlistenNode(){
        String path = "/test14";
        
        ZKNodeListener  nodeListener= new ZKNodeListener() {
            
            @Override
            public void handleSessionExpired(String path) throws Exception {
            }
            
            @Override
            public void handleDataDeleted(String path) throws Exception {
            }
            
            @Override
            public void handleDataCreated(String path, Object data) throws Exception {
            }
            
            @Override
            public void handleDataChanged(String path, Object data) throws Exception {
            }
        };
        zkClient.listenNodeChanges(path,nodeListener);
        assertThat(zkClient.getNodeListenerMap().get(path).size()).isEqualTo(1);
        zkClient.unlistenNodeChanges(path, nodeListener);
        assertThat(zkClient.getNodeListenerMap().get(path)).isNull();
        
        
    }
    
    @Test
    public void testUnlistenChildCount(){
        String path = "/test15";
        ZKChildCountListener countListener = new ZKChildCountListener() {
            
            @Override
            public void handleSessionExpired(String path, List<String> children) throws Exception {
            }
            
            @Override
            public void handleChildCountChanged(String path, List<String> children) throws Exception {
            }
        };
        
        zkClient.listenChildCountChanges(path, countListener);
        assertThat(zkClient.getChildListenerMap().get(path).size()).isEqualTo(1);
        zkClient.unlistenChildChanges(path, countListener);
        assertThat(zkClient.getChildListenerMap().get(path)).isNull();
    }
    
    @Test
    public void testUnlistenChildData(){
        String path = "/test16";
        
        ZKChildDataListener listener = new ZKChildDataListener() {
            
            @Override
            public void handleSessionExpired(String path, Object data) throws Exception {
            }
            
            @Override
            public void handleChildDataChanged(String path, Object data) throws Exception {
            }
            
            @Override
            public void handleChildCountChanged(String path, List<String> children) throws Exception {
            }
        };
        zkClient.create(path, "123", CreateMode.PERSISTENT);
        zkClient.create(path+"/a", "456", CreateMode.EPHEMERAL);
        
        zkClient.listenChildDataChanges(path, listener);
        assertThat(zkClient.getChildListenerMap().get(path).size()).isEqualTo(1);
        assertThat(zkClient.getNodeListenerMap().get(path+"/a").size()).isEqualTo(1);
        zkClient.unlistenChildChanges(path, listener);
        assertThat(zkClient.getChildListenerMap().get(path)).isNull();
        assertThat(zkClient.getNodeListenerMap().get(path+"/a")).isNull();
    }
    
    @Test
    public void testUnlistenState(){
        String path = "/test17";
        ZKStateListener listener = new ZKStateListener() {
            
            @Override
            public void handleStateChanged(KeeperState state) throws Exception {
            }
            
            @Override
            public void handleSessionError(Throwable error) throws Exception {
            }
            
            @Override
            public void handleNewSession() throws Exception {
            }
        };
        
        zkClient.listenStateChanges(listener);
        assertThat(zkClient.getStateListenerSet().size()).isEqualTo(1);
        zkClient.unlistenStateChanges(listener);
        assertThat(zkClient.getStateListenerSet().size()).isEqualTo(0);
    }
    
    @Test
    public void testSerializer(){
        String path = "/test17";
        ZKClient zkClient1 = ZKClientBuilder.newZKClient()
                            .servers("localhost:"+zkServer.getPort())
                            .serializer(new BytesSerializer())
                            .build();
        byte[] data = "123".getBytes();
        zkClient1.create(path+"-1", data, CreateMode.PERSISTENT);
        assertThat(zkClient1.getData(path+"-1")).isEqualTo(data);
        
        ZKClient zkClient2 = ZKClientBuilder.newZKClient()
                .servers("localhost:"+zkServer.getPort())
                .serializer(new SerializableSerializer())
                .build();
        zkClient2.create(path+"-2", 123, CreateMode.PERSISTENT);
        assertThat(zkClient2.getData(path+"-2")).isEqualTo(123);
    }
    
    
    @Test
    public void testAuthorized() {
        zkClient.addAuthInfo("digest", "user:passwd".getBytes());
        zkClient.create("/path1", null, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        zkClient.getData("/path1");
    }

    @Test
    public void testSetAndGetAcls() {
        zkClient.addAuthInfo("digest", "user:passwd".getBytes());

        zkClient.create("/path1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertThat(zkClient.getAcl("/path1").getKey()).isEqualTo(ZooDefs.Ids.OPEN_ACL_UNSAFE);

        for (int i = 0; i < 100; i++) {
            zkClient.setAcl("/path1", ZooDefs.Ids.OPEN_ACL_UNSAFE);
            assertThat(zkClient.getAcl("/path1").getKey()).isEqualTo(ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
    }
}
