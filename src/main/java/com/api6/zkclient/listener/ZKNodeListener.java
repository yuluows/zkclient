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

import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * 节点变化监听，监听节点的创建，数据的修改，以及节点的删除。
 * @author: zhaojie/zh_jie@163.com.com 
 */
public abstract class ZKNodeListener implements ZKListener {

    @Override
    public void handle(String path, EventType eventType, Object data) throws Exception {
        
        if (eventType == EventType.NodeCreated) {
            handleDataCreated(path,data);
        }
        
        if (eventType == EventType.NodeDataChanged) {
            handleDataChanged(path,data);
        }
        
        if(eventType == EventType.NodeDeleted ){
            handleDataDeleted(path);
        }
        
        if(eventType == eventType.None){
            handleSessionExpired(path);
        }
    }
    
    /**
     * 节点创建后回调此方法
     * @param path
     * @param data
     * @throws Exception 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public abstract void handleDataCreated(String path, Object data) throws Exception;
    /**
     * 节点内容变化的回调函数
     * @param path
     * @param data
     * @throws Exception 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public abstract void handleDataChanged(String path, Object data) throws Exception;
    /**
     * 节点删除的回调函数
     * @param path
     * @throws Exception 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月26日 下午4:00:37
     */
    public abstract void handleDataDeleted(String path) throws Exception;
    /**
     * 会话失效并重新连接后会回调此方法。
     * 因为在会话失效时，服务端会注销Wather监听，
     * 所以在会话失效后到连接成功这段时间内，数据可能发生变化，会触发此方法
     * @param path
     * @throws Exception 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public abstract void handleSessionExpired(String path) throws Exception;

}
