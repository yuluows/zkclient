package com.api6.zkclient.util;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.api6.zkclient.ZKClient;

public class TestSystem {
	private static Logger LOG = LoggerFactory.getLogger(TestSystem.class);
	private static TestSystem testSystem;
	private final ZKServer zKserver;
	private int port = 1009;
	private String serverAddress = "localhost";
	
	private TestSystem() {
		zKserver = TestUtil.startServer(serverAddress, port);
	}
	
	
	public static TestSystem getInstance(){
		synchronized (TestSystem.class) {
			if(testSystem == null) {
				testSystem = new TestSystem();
				Runtime.getRuntime().addShutdownHook(new Thread() {
					 @Override
					 public void run() {
						 LOG.info("shutting zkserver down");
						 getInstance().getZKserver().shutdown();
					 }
				});
			}
		}
		
		return testSystem;
	}

	public void cleanup(ZKClient zKClient) {
		LOG.info("unlisten all listeners");
		zKClient.unlistenAll();
		
		LOG.info("cleanup zkserver namespace");
		List<String> children = zKClient.getChildren("/");
		for (String child : children) {
			if (!child.equals("zookeeper")) {
				zKClient.deleteRecursive("/" + child);
			}
		}
		zKClient.close();
	}
	 
	public ZKServer getZKserver() {
		return zKserver;
	}
	
	
}
