package com.api6.zkclient.listener;

import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * 监听基类
 * @author: zhaojie/zh_jie@163.com.com 
 */
public interface ZKListener {
	void handle(String path,EventType eventType, Object data) throws Exception;
}
