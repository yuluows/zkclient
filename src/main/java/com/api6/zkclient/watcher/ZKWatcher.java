package com.api6.zkclient.watcher;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.api6.zkclient.ZKClient;

/**
 * 事件监听类，用于事件分发
 * @author: zhaojie/zh_jie@163.com.com 
 */
public class ZKWatcher implements Watcher {
	private static final Logger LOG = LoggerFactory.getLogger(ZKWatcher.class);
	private final ZKClient client;
	private final ZKWatcherProcess process;
	
	public ZKWatcher(ZKClient client) {
		this.client = client;
		this.process = new ZKWatcherProcess(client);
	}

	/**
	 * 事件处理
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	@Override
	public void process(WatchedEvent event) {
		LOG.debug("ZooKeeper event is arrived [" + event+" ]...");
		EventType eventType = event.getType();
		//状态更新
		boolean stateChanged = event.getPath() == null;
		//节点相关的所有事件
		boolean znodeChanged = event.getPath() != null;
		
		//节点创建、删除和数据改变的事件
		boolean nodeChanged = eventType == EventType.NodeDataChanged 
				|| eventType == EventType.NodeDeleted 
				|| eventType == EventType.NodeCreated;
		
		//子节点数量改变相关的事件，包括节点创建和删除（都会影响子节点数量的变化），以及子节点数量的改变
		boolean childChanged = eventType == EventType.NodeDeleted 
				|| eventType == EventType.NodeCreated
				|| eventType == EventType.NodeChildrenChanged;
		
		client.acquireEventLock();
		try {
			if (client.getShutdownTrigger()) {
				LOG.debug("client will shutdown,ignore the event [" + eventType + " | " + event.getPath() + "]");
				return;
			}
			if (stateChanged) {//ZooKeeper状态改变的处理
				process.processStateChanged(event);
			}
			if (nodeChanged) {//节点改变事件处理，包括节点的创建、删除、数据改变
				process.processNodeChanged(event);
			}
			if (childChanged) {//造成子节点数量改变的事件的处理，包括节点的创建、删除、子节点数量改变
				process.processChildChanged(event);
			}
		} finally {
			if (stateChanged) {
				client.getEventLock().getStateChangedCondition().signalAll();
				// 在会话失效后，服务端会取消watch.
				// 如果在会话失效后与重连这段时间内有数据发生变化，监听器是无法监听到的，
				// 所以要唤醒等待的监听，并触发所有的监听事件
				if (event.getState() == KeeperState.Expired) {
					client.getEventLock().getNodeEventCondition().signalAll();
					client.getEventLock().getNodeOrChildChangedCondition().signalAll();
					
					// 通知所有的监听器，可能存在数据变化
					process.processAllNodeAndChildListeners(event);
				}
			}
			if (znodeChanged) {
				client.getEventLock().getNodeEventCondition().signalAll();
			}
			if (nodeChanged || childChanged) {
				client.getEventLock().getNodeOrChildChangedCondition().signalAll();
			}
			client.releaseEventLock();
		}
	}

}
