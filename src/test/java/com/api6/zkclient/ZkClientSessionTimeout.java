package com.api6.zkclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.api6.zkclient.listener.ZKChildCountListener;
import com.api6.zkclient.listener.ZKChildDataListener;
import com.api6.zkclient.listener.ZKNodeListener;
import com.api6.zkclient.listener.ZKStateListener;
import com.api6.zkclient.util.TestSystem;
import com.api6.zkclient.util.TestUtil;
import com.api6.zkclient.util.ZKServer;

public class ZkClientSessionTimeout {
	
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
    public void testZKClentExpried() throws Exception  {
    	String path = "/test-expried";
    	final List<String> msgList = new ArrayList<String>();
    	
    	
    	
    	zkClient.listenNodeChanges(path, new ZKNodeListener() {
			
			@Override
			public void handleSessionExpired(String path) throws Exception {
				msgList.add("session expried");
			}
			
			@Override
			public void handleDataDeleted(String path) throws Exception {
				//ignore
			}
			
			@Override
			public void handleDataCreated(String path, Object data) throws Exception {
				//ignore
			}
			
			@Override
			public void handleDataChanged(String path, Object data) throws Exception {
				//ignore
			}
		});
    	
    	zkClient.listenChildCountChanges(path, new ZKChildCountListener() {
			
			@Override
			public void handleSessionExpired(String path, List<String> children) throws Exception {
				msgList.add("session expried");
			}
			
			@Override
			public void handleChildCountChanged(String path, List<String> children) throws Exception {
				//ignore			
			}
		});
    	
    	zkClient.listenChildDataChanges(path, new ZKChildDataListener() {
			
			@Override
			public void handleSessionExpired(String path, Object data) throws Exception {
				msgList.add("session expried");
			}
			
			@Override
			public void handleChildDataChanged(String path, Object data) throws Exception {
				//ignore
			}
			
			@Override
			public void handleChildCountChanged(String path, List<String> children) throws Exception {
				//ignore
			}
		});
    	
    	zkClient.listenStateChanges(new ZKStateListener() {
			
			@Override
			public void handleStateChanged(KeeperState state) throws Exception {
				if(state==KeeperState.Expired){
					msgList.add("session expried");
				}
			}
			
			@Override
			public void handleSessionError(Throwable error) throws Exception {
				//ignore
			}
			
			@Override
			public void handleNewSession() throws Exception {
				msgList.add("new session");
			}
		});
    	
    	//创建节点
    	//zkClient.create(path, "123", CreateMode.PERSISTENT);
    	
    	zkServer.shutdown();
    	
    	
    	//20秒后重启server;
    	Thread thread = new Thread(new Runnable() {
    		
			@Override
			public void run() {
				try {
					Thread.sleep(1000*40);
					zkServer.start();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
    	
    	thread.start();
    	thread.join();
    	
    	//等待事件到达
    	int size = TestUtil.waitUntil(5, new Callable<Integer>() {
			@Override
			public Integer call() throws Exception {
				return msgList.size();
			}
    		
		}, TimeUnit.SECONDS, 60);
    	assertThat(size).isEqualTo(5);
    }
    
}
