package com.api6.zkclient.listener;

import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * ZooKeeper状态监听类
 * @author: zhaojie/zh_jie@163.com.com 
 */
public interface ZKStateListener {
	
	/**
	 * 状态改变的回调函数
	 * @param state
	 * @throws Exception 
	 * @return void
	 * @author: zhaojie/zh_jie@163.com 
	 */
    public void handleStateChanged(KeeperState state) throws Exception;

    /**
     * 会话创建的回调函数
     * @throws Exception 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public void handleNewSession() throws Exception;

    /**
     * 会话出错的回调函数
     * @param error
     * @throws Exception 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public void handleSessionError(final Throwable error) throws Exception;

}
