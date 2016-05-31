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
package com.api6.zkclient.connection;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.security.auth.login.Configuration;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.api6.zkclient.event.ZKEventLock;
import com.api6.zkclient.exception.ZKException;
import com.api6.zkclient.exception.ZKInterruptedException;
import com.api6.zkclient.exception.ZKTimeoutException;
import com.api6.zkclient.util.ExceptionUtil;

/**
 * 负责连接、关闭与服务器的连接，实现了断线重连，会话过期重连等机制
 * @author: zhaojie/zh_jie@163.com.com 
 */
public class ZKConnectionImpl implements ZKConnection {
    private static final Logger  LOG = LoggerFactory.getLogger(ZKConnectionImpl.class);
    
    protected static final String JAVA_LOGIN_CONFIG_PARAM = "java.security.auth.login.config";
    protected static final String ZK_SASL_CLIENT = "zookeeper.sasl.client";
    protected static final String ZK_LOGIN_CONTEXT_NAME_KEY = "zookeeper.sasl.clientconfig";
    
    //session超时时间
    private static final int DEFAULT_SESSION_TIMEOUT = 30000;
    private final String servers;//服务器地址
    private final int sessionTimeout;//会话超时时间
    
    //原生zookeeper客户端
    private ZooKeeper zooKeeper = null;
    //ReentrantLock 比起synchronized有更多的操作空间，
    //类似定时锁等候和可中断锁等候,都可以实现,同时性能更优
    private Lock connectionLock = new ReentrantLock();
    
    private final ZKEventLock eventLock = new ZKEventLock();
    private KeeperState currentState;//ZooKeeper连接状态
    protected final long retryTimeout;//重试超时时间
    private boolean isZkSaslEnabled;//
    
    /**
     * 根据给出的服务地址，创建连接
     * @param zkServers zookeeper服务地址，
     * 格式采用，逗号分隔主机:端口号。
     *    例如 "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     *    如果改变了根目录例如： "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     */
    public ZKConnectionImpl(String servers) {
        this(servers, DEFAULT_SESSION_TIMEOUT,-1);
    }
    
    
    public ZKConnectionImpl(String servers,int sessionTimeOut) {
        this(servers, sessionTimeOut,-1);
    }

    /**
     * 根据给出的服务地址，和会话超时时间创建连接
     * @param servers 服务器地址
     * @param sessionTimeOut 会话超时时间
     */
    public ZKConnectionImpl(String servers, int sessionTimeout,int retryTimeout) {
        this.servers = servers;
        this.sessionTimeout = sessionTimeout;
        this.isZkSaslEnabled = isZkSaslEnabled();
        this.retryTimeout = retryTimeout;
    }

    @Override
    public void connect(Watcher watcher) {
        connectionLock.lock();
        try {
            if (zooKeeper != null) {
                throw new ZKException("connection is already connected to server");
            }
            try {
                LOG.info("connecting to server [" + servers + "]");
                zooKeeper = new ZooKeeper(servers, sessionTimeout, watcher);
                LOG.info("connected to server [" + servers + "]");
            } catch (IOException e) {
                throw new ZKException("connecting to server failed [" + servers+ "]", e);
            }
        } finally {
            connectionLock.unlock();
        }
    }
    
    public void reconnect(Watcher watcher) {
        try {
            acquireEventLock();
            close();
            connect(watcher);
        } catch (InterruptedException e) {
            throw new ZKInterruptedException(e);
        } finally {
            releaseEventLock();
        }
    }

    
    @Override
     public <T> T retryUntilConnected(Callable<T> callable) 
             throws ZKInterruptedException,ZKTimeoutException, ZKException, RuntimeException {
        final long operationStartTime = System.currentTimeMillis();
        while (true) {
            try {
                T retVal = callable.call();
                return retVal;
            } catch (ConnectionLossException e) {
                // 当前线程交出CPU权限，让CPU去执行其他的线程
                // 主要是处理事件监听，将currentState置为'Disconnected'状态
                Thread.yield();
               LOG.debug("Connection Disconnected waitForRetry...");
                //等待重试
                waitForRetry();
            } catch (SessionExpiredException e) {
                 // 当前线程交出CPU权限，让CPU去执行其他的线程
                // 主要是处理事件监听，将currentState置为'Expired'状态 
                Thread.yield();
                LOG.debug("Session Expired waitForRetry...");
                //等待重试
                waitForRetry();
            } catch (KeeperException e) {
                throw ZKException.create(e);
            } catch (InterruptedException e) {
                throw new ZKInterruptedException(e);
            } catch (Exception e) {
                throw ExceptionUtil.convertToRuntimeException(e);
            }
            // 检查是否超时，如果超时抛出异常终止尝试连接
            if (retryTimeout > -1 && (System.currentTimeMillis() - operationStartTime) >= retryTimeout) {
                throw new ZKTimeoutException("retry connect timeout  (" + retryTimeout + " milli seconds)");
            }
        }
    }

    /**
     * 等待直到超时或者成功连接
     * @return void
     */
    private void waitForRetry() {
        if (retryTimeout < 0) {
             waitUntilConnected(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
            return;
        }
        waitUntilConnected(retryTimeout, TimeUnit.MILLISECONDS);
    }
    
    
    
    @Override
    public boolean waitUntilConnected(long timeout, TimeUnit timeUnit) throws ZKInterruptedException {
        if (isZkSaslEnabled) {
            return waitForKeeperState(KeeperState.SaslAuthenticated, timeout, timeUnit);
        } else {
            return waitForKeeperState(KeeperState.SyncConnected, timeout, timeUnit);
        }
    }
    
    
    /**
     * 等待直到连接处于某个状态才停止，如果超时返回false，当正确处于某个状态返回true
     * 这里使用到了EventLock的 stateChangedCondition 条件，
     * 如果当前状态不是期待的状态，则此时线程处于等待状态。
     * 1.如果事件监听器发现ZooKeeper状态改变，则会标记stateChangedCondition，当前线程被唤醒，
     *         当前线程继续判断是否是期待的状态，如果是则返回true，如果不是，则线程继续处于等待状态，直到下次ZooKeeper状态改变，重复上述操作。
     * 2.如果等待超时则直接返回false。
     * @param keeperState ZooKeeper状态
     * @param timeout 超时时间 
     * @param timeUnit 时间单位
     * @return
     * @throws ZKInterruptedException  
     * @return boolean
     */
    private boolean waitForKeeperState(KeeperState keeperState, long timeout, TimeUnit timeUnit) throws ZKInterruptedException {
        Date timeoutDate = new Date(System.currentTimeMillis() + timeUnit.toMillis(timeout));
        
        LOG.info("Waiting for ZooKeeper state " + keeperState);
        //使用可中断锁
        acquireEventLockInterruptibly();
        try {
            boolean stillWaiting = true;
            while (currentState != keeperState) {
                if (!stillWaiting) {
                    return false;
                }
                stillWaiting = getEventLock().getStateChangedCondition().awaitUntil(timeoutDate);
                // Throw an exception in the case authorization fails
                if (currentState == KeeperState.AuthFailed && isZkSaslEnabled) {
                    throw new ZKException("认证失败");
                }
            }
            LOG.info("ZooKeeper State is " + currentState);
            return true;
        } catch (InterruptedException e) {
            throw new ZKInterruptedException(e);
        } finally {
           releaseEventLock();
        }
    }
    
    @Override
    public void close() throws InterruptedException {
        connectionLock.lock();
        try {
            if (zooKeeper != null) {
                LOG.debug("closing the connetion [" + servers+ "]");
                zooKeeper.close();
                zooKeeper = null;
                LOG.debug("the connection is closed [" + servers+ "]");
            }
        } finally {
            connectionLock.unlock();
        }
    }
    
    
    private boolean isZkSaslEnabled() {
        boolean isSecurityEnabled = false;
        boolean zkSaslEnabled = Boolean.parseBoolean(System.getProperty(ZK_SASL_CLIENT, "true"));
        String zkLoginContextName = System.getProperty(ZK_LOGIN_CONTEXT_NAME_KEY, "Client");

        if (!zkSaslEnabled) {
            LOG.warn("Client SASL has been explicitly disabled with " + ZK_SASL_CLIENT);
            return false;
        }

        String loginConfigFile = System.getProperty(JAVA_LOGIN_CONFIG_PARAM);
        if (loginConfigFile != null && loginConfigFile.length() > 0) {
            LOG.info("JAAS File name: " + loginConfigFile);
            File configFile = new File(loginConfigFile);
            if (!configFile.canRead()) {
                throw new IllegalArgumentException("File " + loginConfigFile + "cannot be read.");
            }

            try {
                Configuration loginConf = Configuration.getConfiguration();
                isSecurityEnabled = loginConf.getAppConfigurationEntry(zkLoginContextName) != null;
            } catch (Exception e) {
                throw new ZKException(e);
            }
        }
        return isSecurityEnabled;
    }
    
    @Override
    public String getServers() {
        return servers;
    }
    
    @Override
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }
    
    @Override
    public ZKEventLock getEventLock(){
        return eventLock;
    }
    
    @Override
    public void acquireEventLock(){
        getEventLock().lock();
    }
    @Override
    public void releaseEventLock(){
        getEventLock().unlock();
    }
    
    /**
     * 获得可中断的EventLock，在获取锁的时候被阻塞后，如果当前线程抛出interrupt信号，
     * 此线程会被唤醒并处理InterruptedException异常，不会一直阻塞下去
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月24日 上午9:05:55
     */
    @Override
    public void acquireEventLockInterruptibly() {
        try {
            getEventLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw new ZKInterruptedException(e);
        }
    }
    
    @Override
    public KeeperState getCurrentState() {
        return currentState;
    }
    
    @Override
    public void setCurrentState(KeeperState currentState) {
        acquireEventLock();
        try {
            this.currentState = currentState;
        } finally {
           releaseEventLock();
        }
    }
    
     /**
     * 添加认证信息，用于访问被ACL保护的节点
     * @param scheme
     * @param auth 
     * @return void
     */
    @Override
    public void addAuthInfo(final String scheme, final byte[] auth) {
        retryUntilConnected(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                getZooKeeper().addAuthInfo(scheme, auth);
                return null;
            }
        });
    }
}
