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
