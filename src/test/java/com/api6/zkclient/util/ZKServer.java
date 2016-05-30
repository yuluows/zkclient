/**
 *Copyright [2016] [zhaojie]
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
package com.api6.zkclient.util;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.api6.zkclient.exception.ZKException;
import com.api6.zkclient.exception.ZKInterruptedException;

public class ZKServer {

    private final static Logger LOG = Logger.getLogger(ZKServer.class);

    public static final int DEFAULT_PORT = 2181;
    public static final int DEFAULT_TICK_TIME = 1000;
    public static final int DEFAULT_MIN_SESSION_TIMEOUT = 2 * DEFAULT_TICK_TIME;

    private String dataPath;
    private String logPath;
    private ZooKeeperServer zooKeeperServer;
    private NIOServerCnxnFactory nioFactory;
    private int port;
    private int tickTime;
    private int minSessionTimeout;

    public ZKServer(String dataPath, String logPath) {
        this(dataPath, logPath, DEFAULT_PORT);
    }

    public ZKServer(String dataPath, String logPath, int port) {
        this(dataPath, logPath, port, DEFAULT_TICK_TIME);
    }

    public ZKServer(String dataPath, String logPath, int port, int tickTime) {
        this(dataPath, logPath, port, tickTime, DEFAULT_MIN_SESSION_TIMEOUT);
    }

    public ZKServer(String dataPath, String logPath, int port, int tickTime, int minSessionTimeout) {
        this.dataPath = dataPath;
        this.logPath = logPath;
        this.port = port;
        this.tickTime = tickTime;
        this.minSessionTimeout = minSessionTimeout;
    }

    public int getPort() {
        return port;
    }

    @PostConstruct
    public void start() {
        LOG.info("Start single zookeeper server on: localhost:" + port + "...");
        try {
            startZooKeeperServer();
        } catch (RuntimeException e) {
            shutdown();
            throw e;
        }
    }

    private void startZooKeeperServer() {
        //检查端口是否被占用
        if (isPortFree(port)) {
            final File dataDir = new File(dataPath);
            final File dataLogDir = new File(logPath);
            dataDir.mkdirs();
            dataLogDir.mkdirs();
            //单机版 zk server
            LOG.info("data dir: " + dataDir.getAbsolutePath());
            LOG.info("data log dir: " + dataLogDir.getAbsolutePath());
            LOG.info("JAAS login file: " + System.getProperty("java.security.auth.login.config", "none"));
            
            try {
                zooKeeperServer = new ZooKeeperServer(dataDir, dataLogDir, tickTime);
                zooKeeperServer.setMinSessionTimeout(minSessionTimeout);
                nioFactory = new NIOServerCnxnFactory();
                int maxClientConnections = 0; // 0 means unlimited
                nioFactory.configure(new InetSocketAddress(port), maxClientConnections);
                nioFactory.startup(zooKeeperServer);
            } catch (IOException e) {
                throw new ZKException("Unable to start single ZooKeeper server.", e);
            } catch (InterruptedException e) {
                throw new ZKInterruptedException(e);
            }
            
        } else {
            throw new IllegalStateException("Zookeeper port " + port + " was already in use.");
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down ZkServer...");
        if (nioFactory != null) {
            nioFactory.shutdown();
            try {
                nioFactory.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            nioFactory = null;
        }
        if (zooKeeperServer != null) {
        	zooKeeperServer.shutdown();
        	zooKeeperServer = null;
        }
        LOG.info("Shutting down ZkServer...done");
    }
    
    /**
     * 检查端口是否空闲
     * @param port
     * @return 
     * @return boolean 空闲返回true，否则返回false
     */
    private static boolean isPortFree(int port) {
        try {
            Socket socket = new Socket("localhost", port);
            socket.close();
            return false;
        } catch (ConnectException e) {
            return true;
        } catch (SocketException e) {
            if (e.getMessage().equals("Connection reset by peer")) {
                return true;
            }
            throw new RuntimeException(e);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
