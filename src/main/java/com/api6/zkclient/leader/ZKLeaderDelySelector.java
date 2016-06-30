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
package com.api6.zkclient.leader;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.api6.zkclient.ZKClient;
import com.api6.zkclient.exception.ZKException;
import com.api6.zkclient.listener.ZKStateListener;
import com.api6.zkclient.lock.ZKDistributedDelayLock;

/**
 * 选举Leader
 * @author: zhaojie/zh_jie@163.com.com 
 * @version: 2016年6月29日 下午8:51:03
 */
public class ZKLeaderDelySelector implements LeaderSelector {
    private final String id;
    private final ZKClient client;
    private final AtomicInteger delayTimeMillisAtomic = new AtomicInteger(5000);
    private final ZKDistributedDelayLock lock;
    private final String leaderPath;
    private final ExecutorService executorService;
    private final ZKLeaderSelectorListener listener;
    private final ZKStateListener stateListener;
    private final AtomicBoolean isInterrupted = new AtomicBoolean(false);
    private final AtomicBoolean autoRequeue = new AtomicBoolean(false);
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final AtomicReference<Future<?>> ourTask = new AtomicReference<Future<?>>(null);
    private String curentNodePath;
    
    
    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }
    
    /**
     * 创建Leader选举对象
     * ZKLeaderSelector. 
     * 
     * @param id 每个Leader选举的参与者都有一个ID标识，用于区分各个参与者。
     * @param autoRequue 是否在由于网络问题造成与服务器断开连接后，自动参与到选举队列中。
     * @param delayTimeMillis 延迟选举的时间，主要是针对网络闪断的情况，给Leader以重连并继续成为Leader的机会，一般5秒合适。
     * @param client ZKClient
     * @param leaderPath 选举的路径
     * @param listener 成为Leader后执行的的监听器
     */
    public ZKLeaderDelySelector(String id,Boolean autoRequue,Integer delayTimeMillis, ZKClient client, String leaderPath, ZKLeaderSelectorListener listener) {
        this.delayTimeMillisAtomic.set(delayTimeMillis);
        this.id = id;
        this.client = client;
        this.autoRequeue.set(autoRequue);
        this.leaderPath = leaderPath;
        this.lock = ZKDistributedDelayLock.newInstance(client, leaderPath);
        this.lock.setLockNodeData(this.id);
        this.executorService = Executors.newSingleThreadExecutor();
        this.listener = listener;
        
        this.stateListener = new ZKStateListener() {
            @Override
            public void handleStateChanged(KeeperState state) throws Exception {
               if(state == KeeperState.SyncConnected){//如果重新连接
                   if(isInterrupted.get() == false) {
                       requeue();
                   }
               }
            }
            
            @Override
            public void handleSessionError(Throwable error) throws Exception {
                //ignore
            }
            
            @Override
            public void handleNewSession() throws Exception {
                //ignore
            }
        };
    }
    
    /**
     * 启动参与选举Leader
     * @return void
     */
    @Override
    public void start() {
        if (!state.compareAndSet(State.LATENT, State.STARTED)) {
            throw new ZKException("Cannot be started more than once");
        }
        client.listenStateChanges(stateListener);
        requeue();
    }
    
    /**
     * 重新添加当前线程到选举队列
     * @return void
     */
    @Override
    public synchronized void requeue() {
       if (state.get() != State.STARTED) {
           throw new ZKException("close() has already been called");
       }
       
       isInterrupted.set(false);
       
       if(ourTask.get() != null) {
           ourTask.get().cancel(true);
       }
       
       //添加到参与者节点中
       addParticipantNode();
       
       Future<Void> task = executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
           lock.lock(0,delayTimeMillisAtomic.get());
           listener.takeLeadership(client,ZKLeaderDelySelector.this);
           return null;
        }
       
       });
       ourTask.set(task);
    }

    /**
     * 获得Leader ID
     * @return 
     * @return String
     */
    @Override
    public String getLeader() {
        return client.getData(lock.getLockPath());
    }
    
    /**
     * 是否是Leader
     * @return 
     * @return boolean
     */
    @Override
    public boolean isLeader(){
        return lock.hasLock();
    }
    /**
     * 获得当前的所有参与者的路径名
     * @return 
     * @return List<String>
     */
    @Override
    public List<String> getParticipantNodes(){
        return client.getChildren(leaderPath+"/nodes");
    }
    
    private void addParticipantNode() {
       String path = client.create(leaderPath+"/nodes/1", id, CreateMode.EPHEMERAL_SEQUENTIAL);
       curentNodePath = path;
    }
    
    private void removeParticipantNode() {
        client.delete(curentNodePath);
    }
    
    /**
     * 终止等待成为Leader
     * @return void
     */
    @Override
    public synchronized void interruptLeadership(){
        Future<?> task = ourTask.get();
        if ( task != null ) {
            task.cancel(true);
        }
        isInterrupted.set(true);
    }
    
    /**
     * 关闭Leader选举
     * @return void
     */
    @Override
    public synchronized void close() {
        if(!state.compareAndSet(State.STARTED, State.CLOSED)){
            throw new ZKException("Already closed or has not been started");
        }
        client.unlistenStateChanges(stateListener);
        lock.unlock();
        //从参与者节点列表中移除
        removeParticipantNode();
        executorService.shutdown();
        ourTask.set(null);
    }
    
    
}
