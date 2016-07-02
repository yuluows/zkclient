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
package com.api6.zkclient.watcher;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.api6.zkclient.ZKClient;
import com.api6.zkclient.event.ZKEvent;
import com.api6.zkclient.event.ZKEventThreadPool;
import com.api6.zkclient.exception.ZKNoNodeException;
import com.api6.zkclient.listener.ZKChildDataListener;
import com.api6.zkclient.listener.ZKListener;
import com.api6.zkclient.listener.ZKNodeListener;
import com.api6.zkclient.listener.ZKStateListener;

/**
 * 事件处理类，接受监听器，并回调
 * @author: zhaojie/zh_jie@163.com.com 
 */
public class ZKWatcherProcess {
    private static Logger LOG = LoggerFactory.getLogger(ZKWatcherProcess.class);
    private final ZKEventThreadPool eventThreadPool;
    private final ZKClient client;
    
    public ZKWatcherProcess(ZKClient zkClient) {
        this.client = zkClient;
        //创建事件处理线程池
        eventThreadPool = new ZKEventThreadPool(zkClient.getEventThreadPoolSize());
    }
    
    /**
     * 停止处理
     * @return void
     */
    public void stop(){
        eventThreadPool.destory();
    }
    
    public void processStateChanged(WatchedEvent event) {
        final KeeperState keeperState = event.getState();
        LOG.info("ZooKeeper state is changed [" + keeperState + "] .");
        
        //这里需要更新一下，ZooKeeper客户端的状态
        client.setCurrentState(keeperState);
        if (client.getShutdownTrigger()) {
            return;
        }
        //获取所有的事件监听器
        Set<ZKStateListener> listeners = client.getStateListenerSet();
        
        //状态改变事件处理
        for (final ZKStateListener stateListener : listeners) {
            eventThreadPool.submit(new ZKEvent("State changed to " + keeperState + " sent to " + stateListener) {
                @Override
                public void run() throws Exception {
                    stateListener.handleStateChanged(keeperState);
                }
            });
        }
        
        //如果会话过期，要重新连接服务器
        if (event.getState() == KeeperState.Expired) {
            try {
                //会话过期，重新连接
                client.reconnect();
                //会话过期事件处理
                for (final ZKStateListener stateListener : listeners) {
                    ZKEvent zkEvent = new ZKEvent("New session event sent to " + stateListener) {
                        @Override
                        public void run() throws Exception {
                            stateListener.handleNewSession();
                        }
                    };
                    eventThreadPool.submit(zkEvent);
                }
            } catch (final Exception e) {
                LOG.info("Unable to re-establish connection. Notifying consumer of the following exception: ", e);
                //会话过期后重连出错，事件处理
                for (final ZKStateListener stateListener : listeners) {
                    eventThreadPool.submit(new ZKEvent("Session establishment error[" + e + "] sent to " + stateListener) {
                        @Override
                        public void run() throws Exception {
                            stateListener.handleSessionError(e);
                        }
                    });
                }
            } 
        }
    }
    
    
    /**
     * 处理子节点变化事件
     * @param event 
     * @return void
     */
    public void processChildChanged(final WatchedEvent event){
        final String path = event.getPath();
        final Set<ZKListener> listeners = client.getChildListenerMap().get(path);
        //提交事件监听进行处理
        submitChildEvent(listeners,path,event.getType());
    }
    
    /**
     * 处理数据改变事件
     * @param event 
     * @return void
     */
    public void processNodeChanged(final WatchedEvent event){
        final String path = event.getPath();
        final EventType eventType = event.getType();
        final Set<ZKListener> listeners = client.getNodeListenerMap().get(path);
        if (listeners == null || listeners.isEmpty()) {
            return;
        }
        
        //如果listeners中如果有ZKChildDataListener类型的监听器，
        //证明是此节点是某个节点的子节点，并监听此节点的数据变化
        //这里要单独拿出来，用于触发ZKChildDataListener
        final Set<ZKListener> childDataChangeListners = new CopyOnWriteArraySet<>();
        final Set<ZKListener> nodeListners = new CopyOnWriteArraySet<>();
        
        classifyListeners(listeners,nodeListners,childDataChangeListners);
        
        //提交事件监听进行处理
        submitNodeEvent(nodeListners,childDataChangeListners,path,eventType);
        
        //当前节点作为子节点数据变化
        if(eventType == EventType.NodeDataChanged){
            //提交事件监听进行处理
            submitChildDataEvent(childDataChangeListners,path,eventType);
        }
    }
    
    /**
     * 触发所有的监听器，用于在会话失效后调用。
     * 会话失效后，服务端会取消watch，
     * 如果在会话失效后与重连这段时间内有数据发生变化，监听器是无法监听到的，
     * 所以要调用此方法，触发所有监听器，告诉监听器，会话失效，可能存在数据变化（不是一定有变化）。
     * @param eventType 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public void processAllNodeAndChildListeners(final WatchedEvent event){
        LOG.debug("processAllNodeAndChildListeners....");
        //对选取的监听器进行处理
        for (Entry<String, CopyOnWriteArraySet<ZKListener>> entry : client.getNodeListenerMap().entrySet()) {
            Set<ZKListener> nodeListners = new CopyOnWriteArraySet<ZKListener>();
            Set<ZKListener> childDataChangeListners = new CopyOnWriteArraySet<ZKListener>();
            Set<ZKListener> listeners = entry.getValue();
            
            classifyListeners(listeners,nodeListners,childDataChangeListners);
            //提交事件监听进行处理
            submitNodeEvent(nodeListners, childDataChangeListners, entry.getKey(), event.getType());
        }
        //获取所有的节点监听器，并进行处理
        for (Entry<String, CopyOnWriteArraySet<ZKListener>> entry : client.getChildListenerMap().entrySet()) {
            //提交事件监听进行处理
            submitChildEvent(entry.getValue(),entry.getKey(),event.getType());
        }
    }
    
    /**
     * 对listeners进行分类整理，把{@link ZKNodeListener}类型的放入nodeListeners，把{@link ZKChildDataListener} 类型的放入childDataChangeListeners
     * @param listeners
     * @param nodeListeners
     * @param childDataChangeListeners 
     * @return void
     */
    private void classifyListeners(Set<ZKListener> listeners,Set<ZKListener> nodeListeners,Set<ZKListener> childDataChangeListeners){
        for(ZKListener listener : listeners){
            if(listener instanceof ZKChildDataListener){
                if(!childDataChangeListeners.contains(listener)){
                    childDataChangeListeners.add(listener);
                }
            }else{
                if(!nodeListeners.contains(listener)){
                    nodeListeners.add(listener);
                }
            }
        }
    }
    
    
    /**
     * 提交节点改变相关的事件进行处理
     * @param listeners
     * @param childDataChangeListners
     * @param path
     * @param eventType 
     * @return void
     */
    private void submitNodeEvent(final Set<ZKListener> listeners,final Set<ZKListener> childDataChangeListners,final String path,final EventType eventType ){
        if (listeners != null && !listeners.isEmpty()) {
            for (final ZKListener listener : listeners) {
                ZKEvent zkEvent = new ZKEvent("Node of " + path + " changed sent to " + listener) {
                    @Override
                    public void run() throws Exception {
                        //原生的zookeeper 的监听只生效一次，重新注册监听
                        LOG.debug("Rewatch the path ["+path+"] by exists method");
                        boolean flag = client.exists(path, true);
                        LOG.debug("Rewatched the path ["+path+"] by exists method");
                        try {
                            LOG.debug("Rewatch and get changed data [path:"+path+" | EventType:"+eventType+"] by getData method");
                            Object data = client.getData(path, null);
                            LOG.debug("Rewatched and return data   ["+path+" | "+data+" | EventType:"+eventType+"] by getData method");
                            listener.handle(path, eventType, data);
                        } catch (ZKNoNodeException e) {
                            //如果是节点不存在了，则只移除，ZKChildDataListener监听器
                            client.unlistenNodeChanges(path, childDataChangeListners);
                            //如果路径不存在，在调用client.getData(path,null)会抛出异常
                            listener.handle(path, eventType, null);
                            
                            //如果是创建节点事件，并且在创建事件收到后，监听还没来得及重新注册，刚创建的节点已经被删除了。
                            //对于这种情况，客户端就无法重新监听到节点的删除事件的，这里做特殊处理
                            //主动触发删除的监听事件
                            if(eventType == EventType.NodeCreated && !flag) {
                                listener.handle(path, EventType.NodeDeleted, null);
                            }
                        }
                    }
                };
                eventThreadPool.submit(zkEvent);
            }
        }
        
    }
    
    /**
     * 提交子节点改变相关的事件进行处理
     * @param listeners
     * @param path
     * @param eventType 
     * @return void
     */
    private void submitChildEvent(final Set<ZKListener> listeners,final String path,final EventType eventType){
        if (listeners != null && !listeners.isEmpty()) {
            try {
                for (final ZKListener listener : listeners) {
                    //创建事件，并在独立的事件处理线程池中执行
                    ZKEvent zkEvent = new ZKEvent("Children of " + path + " changed sent to " + listener) {
                        @Override
                        public void run() throws Exception {
                            //原生的zookeeper 的监听只生效一次，所以要重新注册父节点的监听
                            LOG.debug("Rewatch the path ["+path+"] by exists method");
                            client.exists(path);
                            LOG.debug("Rewatched the path ["+path+"] by exists method");
                            try {
                                
                                LOG.debug("Rewatch and get chilldren [path:"+path+" | EventType:"+eventType+"] by getChildren method");
                                //获取数据并重新监听子节点变化
                                List<String> children = client.getChildren(path);
                                LOG.debug("Rewatched and return children  [children:"+children+" | EventType:"+eventType+"] by getChildren method");
                                //子节点数量改变，如果当前路径path设置了ZKChildDataListener。
                                //则尝试重新注册子节点数据变化的监听
                                client.listenNewChildPathWithData(path,children);
                                listener.handle(path, eventType, children);
                            } catch (ZKNoNodeException e) {
                                //如果路径不存在，在调用client.getChildren(path)会抛出异常
                                listener.handle(path, eventType, null);
                            }
                        }
                    };
                    eventThreadPool.submit(zkEvent);
                }
            } catch (Exception e) {
                LOG.error("Failed to fire child changed event. Unable to getChildren.  ", e);
            }
        }
    }
    
    /**
     * 提交子节点数据改变的事件进行处理
     * @param listeners
     * @param path
     * @param eventType 
     * @return void
     */
    private void submitChildDataEvent(final Set<ZKListener> listeners,final String path,final EventType eventType){
        if (listeners != null && !listeners.isEmpty()) {
            for (final ZKListener listener : listeners) {
                ZKEvent zkEvent = new ZKEvent("Children of " + path + " changed sent to "+ listener) {
                    @Override
                    public void run() throws Exception {
                        //原生的zookeeper 的监听只生效一次，重新注册监听
                        LOG.debug("rewatch the path ["+path+"]");
                        client.exists(path, true);
                        LOG.debug("rewatched the path ["+path+"]");
                        try {
                            LOG.debug("Try to get child changed data  [path:"+path+" | EventType:"+eventType+"]");
                            //在事件触发后到获取数据之间的时间，节点的值是可能改变的，
                            //所以此处的获取也只是，获取到最新的值，而不一定是事件触发时的值
                            //重新监听节点变化
                            Object data = client.getData(path, null);
                            LOG.debug("Child changed data is  [path:"+path+" | data:"+data+" | EventType:"+eventType+"]");
                            listener.handle(path, eventType, data);
                        } catch (ZKNoNodeException e) {
                            //ignore
                        }
                    }
                };
                eventThreadPool.submit(zkEvent);
            }
        }
    }
}
