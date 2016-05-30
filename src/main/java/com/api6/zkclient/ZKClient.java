package com.api6.zkclient;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.api6.zkclient.connection.ZKConnection;
import com.api6.zkclient.connection.ZKConnectionImpl;
import com.api6.zkclient.exception.ZKException;
import com.api6.zkclient.exception.ZKInterruptedException;
import com.api6.zkclient.exception.ZKNoNodeException;
import com.api6.zkclient.exception.ZKNodeExistsException;
import com.api6.zkclient.listener.ZKChildCountListener;
import com.api6.zkclient.listener.ZKChildDataListener;
import com.api6.zkclient.listener.ZKListener;
import com.api6.zkclient.listener.ZKStateListener;
import com.api6.zkclient.lock.ZKEventLock;
import com.api6.zkclient.serializer.SerializableSerializer;
import com.api6.zkclient.serializer.ZKSerializer;
import com.api6.zkclient.watcher.ZKWatcher;

/**
 * ZooKeeper客户端
 * @author: zhaojie/zh_jie@163.com.com 
 */
public class ZKClient  {
	private static Logger LOG = LoggerFactory.getLogger(ZKClient.class);
	
	//子节点监听器
	private final Map<String, CopyOnWriteArraySet<ZKListener>> childListenerMap = new ConcurrentHashMap<String, CopyOnWriteArraySet<ZKListener>>();
	//节点监听器
	private final Map<String, CopyOnWriteArraySet<ZKListener>> nodeListenerMap = new ConcurrentHashMap<String, CopyOnWriteArraySet<ZKListener>>();
	//状态监听器
	private final Set<ZKStateListener> stateListenerSet = new CopyOnWriteArraySet<ZKStateListener>();
	
	//保存EPHEMERAL类型的节点，用于在断开重连，以及会话失效后的自动创建节点
	private final Map<String, ZKNode> ephemeralNodeMap = new ConcurrentHashMap<String,ZKNode>();
	
	private final ZKConnection connection;//连接
	private final ZKWatcher watcher;//监听器
	private boolean shutdownTrigger;//触发关闭的标记，如果为true证明正在关闭客户端及连接
	private final ZKSerializer serializer;//序列化工具
	private volatile boolean closed;//是否已关闭客户端的标记
	private final int eventThreadPoolSize; //线程池的大小，同时也是并发线程数
	
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param servers 服务器地址
	 */
	public ZKClient(String servers) {
		this(new ZKConnectionImpl(servers));
	}
	
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param servers 
	 * 				服务器地址
	 * @param sessionTimeOut 
	 * 				会话超时时间，单位毫秒，这里并不是真实值，只是参考计算的值。
	 * 				会话超时时间和服务端的配置有关，一般是2 * tickTime ~ 20 * tickTime之间
	 */
	public ZKClient(String servers, int sessionTimeOut) {
		this(new ZKConnectionImpl(servers, sessionTimeOut));
	}
	
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param servers 服务器地址
	 * @param sessionTimeOut 会话超时时间，单位毫秒，这里并不是真实值，只是参考计算的值。
	 * 		会话超时时间和服务端的配置有关，一般是2 * tickTime ~ 20 * tickTime之间
	 * @param operationRetryTimeoutInMillis 重新连接超时时间,单位毫秒。
	 * 		在会话过期或连接断开后，重新尝试连接的失效时间
	 */
	public ZKClient(String servers, int sessionTimeOut,int operationRetryTimeoutInMillis) {
		this(new ZKConnectionImpl(servers, sessionTimeOut,operationRetryTimeoutInMillis),new SerializableSerializer());
	}
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param servers 服务器地址
	 * @param sessionTimeOut 会话超时时间，单位毫秒，这里并不是真实值，只是参考计算的值。
	 * 		会话超时时间和服务端的配置有关，一般是2 * tickTime ~ 20 * tickTime之间
	 * @param operationRetryTimeoutInMillis 重新连接超时时间,单位毫秒。
	 * 		在会话过期或连接断开后，重新尝试连接的失效时间
	 * @param zkSerializer 序列化类
	 */
	public ZKClient(String servers, int sessionTimeOut,int operationRetryTimeoutInMillis,final ZKSerializer zkSerializer) {
		this(new ZKConnectionImpl(servers, sessionTimeOut,operationRetryTimeoutInMillis),zkSerializer,Integer.MAX_VALUE);
	}
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param servers 服务器地址
	 * @param sessionTimeOut 会话超时时间，单位毫秒，这里并不是真实值，只是参考计算的值。
	 * 		会话超时时间和服务端的配置有关，一般是2 * tickTime ~ 20 * tickTime之间
	 * @param operationRetryTimeoutInMillis 重新连接超时时间,单位毫秒。
	 * 		在会话过期或连接断开后，重新尝试连接的失效时间
	 * @param zkSerializer 序列化类
	 * @param connectionTimeout 连接超时时间，单位毫秒
	 * 			启动时连接到服务器的超时时间
	 */
	public ZKClient(String servers, int sessionTimeOut,int operationRetryTimeoutInMillis,final ZKSerializer zkSerializer,final int connectionTimeout) {
		this(new ZKConnectionImpl(servers, sessionTimeOut,operationRetryTimeoutInMillis),zkSerializer,connectionTimeout,1);
	}
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param servers 服务器地址
	 * @param sessionTimeOut 会话超时时间，单位毫秒，这里并不是真实值，只是参考计算的值。
	 * 			会话超时时间和服务端的配置有关，一般是2 * tickTime ~ 20 * tickTime之间
	 * @param operationRetryTimeoutInMillis 重新连接超时时间,单位毫秒。
	 * 			在会话过期或连接断开后，重新尝试连接的失效时间
	 * @param zkSerializer 序列化类
	 * @param connectionTimeout 连接超时时间，单位毫秒
	 * 			启动时连接到服务器的超时时间
	 * @param eventThreadPoolSize 事件处理线程池的并发线程数，默认值：1。
	 * 				1.如果想要串行执行监听器并且保证严格的顺序，请将设置为 1。
	 * 				2.此值不易太大，推荐1~3即可。一般情况下1个线程就足够，因为一般ZooKeeper监听的响应并不会有很大的并发
	 */
	public ZKClient(String servers, int sessionTimeOut,int operationRetryTimeoutInMillis,
			final ZKSerializer zkSerializer,final int connectionTimeout,final int eventThreadPoolSize) {
		this(new ZKConnectionImpl(servers, sessionTimeOut,operationRetryTimeoutInMillis),zkSerializer,connectionTimeout,eventThreadPoolSize);
	}
	
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param connection ZKConnection的连接对象
	 */
	public ZKClient(final ZKConnection connection) {
		this(connection,new SerializableSerializer());
	}
	
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param connection ZKConnection的连接对象
	 * @param zkSerializer 序列化工具类
	 */
	public ZKClient(final ZKConnection connection,final ZKSerializer zkSerializer) {
		this(connection,zkSerializer,Integer.MAX_VALUE);
	}
	
	public ZKClient(final ZKConnection connection,final ZKSerializer zkSerializer,final int connectionTimeout) {
		this(connection,zkSerializer,connectionTimeout,1);
	}
	/**
	 * 创建ZKClient客户端
	 * ZKClient. 
	 * @param connection ZKConnection的连接对象
	 * @param zkSerializer 序列化工具类
	 * @param connectionTimeout 连接超时时间，单位毫秒
	 * 		启动时连接到服务器的超时时间
	 */
	public ZKClient(final ZKConnection connection,final ZKSerializer zkSerializer,final int connectionTimeout, final int eventThreadPoolSize){
		this.connection = connection;
		this.serializer = zkSerializer;
		this.eventThreadPoolSize = eventThreadPoolSize;
		this.watcher = new ZKWatcher(this);
		start(connectionTimeout,watcher);
	}
	
	/**
	 * 连接服务端并注册监听watcher
	 * @param connectionTimeout 连接超时时间
	 * @param watcher 事件监听器
	 * @throws ZKInterruptedException
	 * @throws ZKException
	 * @throws IllegalStateException 
	 * @return void
	 * @author: zhaojie/zh_jie@163.com 
	 */
	private void start(final long connectionTimeout, Watcher watcher) throws ZKInterruptedException, ZKException, IllegalStateException {
        boolean started = false;
        //获取可中断锁
        acquireEventLockInterruptibly();
        LOG.info("The ZKClient is starting....");
        try {
            shutdownTrigger = false;
            connection.connect(watcher);
            boolean waitSuccessful = connection.waitUntilConnected(connectionTimeout, TimeUnit.MILLISECONDS);
            if (!waitSuccessful) {
                throw new ZKException("Unable to start ZKClient within timeout [" + connectionTimeout+"].");
            }
            closed = false;
            started = true;
            LOG.info("The ZkClient is started.");
        } finally {
            releaseEventLock();
            //如果没有连接成功，则需要关闭连接，防止连接继续等待连接成功
            if (!started) {
                close();
            }
        }
    }
	
	/**
	 * 重新连接服务器并注册监听
	 * @return void
	 * @author: zhaojie/zh_jie@163.com 
	 * @version: 2016年5月24日 上午9:12:02
	 */
	public void reconnect() {
		connection.reconnect(watcher);
		closed = false;
		//重新设置监听器
		relisten();
		//重新创建，需要创建的临时类型的节点
		recreatEphemeraleNode();
	}
	
	/**
	 * 重新设置监听
	 * @return void
	 * @author: zhaojie/zh_jie@163.com 
	 * @version: 2016年5月26日 上午12:18:36
	 */
	public void relisten(){
		LOG.info(" Relisten.......");
		for (Entry<String, CopyOnWriteArraySet<ZKListener>> entry : nodeListenerMap.entrySet()) {
			watchForData(entry.getKey());
			LOG.debug("Relisten node:{}", entry.getKey());
		}
		for (Entry<String, CopyOnWriteArraySet<ZKListener>> entry : childListenerMap.entrySet()) {
			watchForChilds(entry.getKey());
			LOG.debug("Relisten child:{}", entry.getKey());
		}
	}

	/**
	 * 关闭客户端，同时会取消与ZooKeeper的连接，清空并取消所有的监听
	 * @throws ZKInterruptedException 
	 * @return void
	 */
	public void close() throws ZKInterruptedException {
		if (closed) {
		    return;
		}
		LOG.debug("Closing ZKClient ...");
		acquireEventLock();
		try {
			shutdownTrigger = true;
			connection.close();
			closed = true;
			//取消监听
			unlistenAll();
		} catch (InterruptedException e) {
		    throw new ZKInterruptedException(e);
		} finally {
		    releaseEventLock();
		}
		LOG.debug("Closed the ZKClient .");
	}
	
	/**
	 * 监听子节点的变化，只是监听数量的变化。
	 * @param path 父路径
	 * @param listener 监听器
	 * @return 
	 * @return List<String> 返回当前子节点路径集合
	 */
    public List<String> listenChildCountChanges(String path, ZKChildCountListener listener) {
    	LOG.debug("Listen child count changes ["+path+"--"+listener+"] ");
        synchronized (childListenerMap) {
        	CopyOnWriteArraySet<ZKListener> listeners = childListenerMap.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<ZKListener>();
                childListenerMap.put(path, listeners);
            }
            listeners.add(listener);
        }
        
        return watchForChilds(path);
    }
    
    /**
     * 监听子节点变化，包括数量的变化，和子节点内容的变化。
     * 这里的内容的变化指的版本的变化，例如修改的值与原来的值一样，也会被视为数据已改变。
     * @param path
     * @param listener
     * @return 
     * @return List<String>
     */
    public List<String> listenChildDataChanges(String path, ZKChildDataListener listener) {
    	LOG.debug("Listen child count and data  changes ["+path+"--"+listener+"] ");
        synchronized (childListenerMap) {
        	CopyOnWriteArraySet<ZKListener> listeners = childListenerMap.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<ZKListener>();
                childListenerMap.put(path, listeners);
            }
            listeners.add(listener);
        }
        
       List<String> children =  watchForChilds(path);
  	  	//监听子节点数据
       listenNewChildPathWithData(path,children);
        return children;
    }
    
    /**
     * 为路径下的新增的子节点添加数据变化的监听
     * @param path 路径
     * @param children  子节点集合
     * @return void
     */
    public void listenNewChildPathWithData(String path,List<String> children){
    	
    	if(children==null || children.size()==0){
    		return;
    	}
    	CopyOnWriteArraySet<ZKListener> chidldDataListeners = new CopyOnWriteArraySet<ZKListener>();
    	synchronized (childListenerMap) {
    		Set<ZKListener> listeners = childListenerMap.get(path);
    		if(listeners == null || listeners.size() == 0 ){
    			return;
    		}
	    	for(ZKListener listener : listeners){
	    		if(listener instanceof ZKChildDataListener){
	    			if(!chidldDataListeners.contains(listener)){
	    				chidldDataListeners.add(listener);
	    			}
	    		}
	    	}
    	}
    	
    	List<String> newWatchPaths = new ArrayList<String>();
    	synchronized (nodeListenerMap) {
	     	for(String childPath : children){
	     		childPath = path+"/"+childPath;
	     		CopyOnWriteArraySet<ZKListener> nodeListeners = nodeListenerMap.get(childPath);
	     		boolean isNewPath = true;
	 			if(nodeListeners!=null && nodeListeners.size()>0){
	 				isNewPath = false;
	 			}
	 			if(nodeListeners == null){
	 				nodeListeners = new CopyOnWriteArraySet<ZKListener>();
	 				nodeListenerMap.put(childPath, nodeListeners);
	 			}
	 			for(ZKListener listener : chidldDataListeners){
	 				if(!nodeListeners.contains(listener)){
	 					nodeListeners.add(listener);
	         		}
	 			}
	     		if(isNewPath){
	     			if(nodeListeners!=null && nodeListeners.size()>0){
	     				newWatchPaths.add(childPath);
	         		}
	     		}
	     	}
    	}
     	
     	for(String newPath : newWatchPaths){
     		LOG.debug("listen new child path ["+newPath+"]");
     		watchForData(newPath);
     	}
    }

    /**
     * 解除对子节点的监听
     * @param path
     * @param childListener
     * @param withData 
     * @return void
     */
    public void unlistenChildChanges(String path, ZKListener childListener) {
    	LOG.debug("Unlisten child ["+path+"--"+childListener+"] ");
    	//解除对子节点的监听
        synchronized (childListenerMap) {
            final Set<ZKListener> listeners = childListenerMap.get(path);
            if (listeners != null) {
                listeners.remove(childListener);
                LOG.debug("unlistenChildChanges:Unlistener path["+path+"--"+childListener+"] ");
            }
            if(listeners == null || listeners.isEmpty()){
            	childListenerMap.remove(path);
            }
        }
        //如果监听了子节点的数据变化，则要同时解除对子节点的数据变化的监听。
		if(childListener instanceof ZKChildDataListener){
			 synchronized (nodeListenerMap) {
		    	Set<String> dataPaths = nodeListenerMap.keySet();
		    	//遍历所有的节点监听器，查找是path节点的子节点，并移除监听器
		    	for(String dataPath : dataPaths){
		    		//查找path的下一级节点的监听器,并删除注册的ZKChildDataListener监听器
		    		int index = dataPath.lastIndexOf("/");
		    		if(index>0){
		    			String parentPath = dataPath.substring(0, index);
		        		if(path.equals(parentPath)){//如果dataPath是下一级节点
		        			CopyOnWriteArraySet<ZKListener> nodeListeners = nodeListenerMap.get(dataPath);
		        			if(nodeListeners != null) {
		        				for(ZKListener nodeListener : nodeListeners){
		        					if(nodeListener instanceof ZKChildDataListener){
		        						nodeListeners.remove(nodeListener);
		        						LOG.debug(":Unlistener child data changes ["+dataPath+"--"+nodeListener+"] ");
		        					}
		        				}
		        			}
		        			 if(nodeListeners == null || nodeListeners.isEmpty()){
		        				 nodeListenerMap.remove(dataPath);
		        			 }
		        		}
		    		}
		    	}
			}
		}
    }

    /**
     * 对节点进行监听，包括节点的创建、删除、数据改变
     * @param path 监听的路径
     * @param listener  监听器
     * @return void
     */
    public void listenNodeChanges(String path, ZKListener nodeListener) {
    	LOG.debug("listen Node ["+path+"--"+nodeListener+"] ");
    	CopyOnWriteArraySet<ZKListener> listeners;
        synchronized (nodeListenerMap) {
            listeners = nodeListenerMap.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<ZKListener>();
                nodeListenerMap.put(path, listeners);
            }
            listeners.add(nodeListener);
        }
        watchForData(path);
        LOG.debug("listened path: " + path);
    }

    /**
     * 解除对节点的监听
     * @param path 监听路径
     * @param nodeListener  监听器
     * @return void
     */
    public void unlistenNodeChanges(String path, ZKListener nodeListener) {
    	LOG.debug("Unlisten Node ["+path+"--"+nodeListener+"] ");
        synchronized (nodeListenerMap) {
            final Set<ZKListener> listeners = nodeListenerMap.get(path);
            if (listeners != null) {
                listeners.remove(nodeListener);
            }
            if (listeners == null || listeners.isEmpty()) {
            	nodeListenerMap.remove(path);
            	LOG.debug("unlistened path: " + path);
            }
        }
    }
    
    /**
     * 解除对节点的监听
     * @param path 路径
     * @param dataListeners 监听器集合 
     * @return void
     */
    public void unlistenNodeChanges(String path, Set<ZKListener> dataListeners) {
    	LOG.debug("Unlisten  Node ["+path+"--"+dataListeners+"] ");
        synchronized (nodeListenerMap) {
            final Set<ZKListener> listeners = nodeListenerMap.get(path);
            if (listeners != null) {
                listeners.removeAll(dataListeners);
            }
            if (listeners == null || listeners.isEmpty()) {
            	nodeListenerMap.remove(path);
            }
        }
    }

    /**
     * 监听ZooKeeper客户端状态
     * @param listener 
     * @return void
     */
    public void listenStateChanges(final ZKStateListener listener) {
    	LOG.debug("listen state changes. ["+listener+"]");
        synchronized (stateListenerSet) {
        	stateListenerSet.add(listener);
        }
    }

    /**
     * 解除监听ZooKeeper的状态
     * @param stateListener 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月26日 下午3:23:11
     */
    public void unlistenStateChanges(ZKStateListener stateListener) {
        synchronized (stateListenerSet) {
        	stateListenerSet.remove(stateListener);
        }
    }

    /**
     * 清除所有的监听器
     * @return void
     */
    public void unlistenAll() {
        synchronized (childListenerMap) {
        	childListenerMap.clear();
        }
        synchronized (nodeListenerMap) {
        	nodeListenerMap.clear();
        }
        synchronized (stateListenerSet) {
        	stateListenerSet.clear();
        }
    }
    
    /**
     * 判断路径是否设置了监听
     * @param path
     * @return 
     * @return boolean
     */
    private boolean hasListeners(String path) {
        Set<ZKListener> dataListeners = nodeListenerMap.get(path);
        if (dataListeners != null && dataListeners.size() > 0) {
            return true;
        }
        Set<ZKListener> childListeners = childListenerMap.get(path);
        if (childListeners != null && childListeners.size() > 0) {
            return true;
        }
        return false;
    }
    
    /**
     * 等待直到数据被创建
     * @param path
     * @param timeUnit
     * @param time
     * @return
     * @throws ZKInterruptedException 
     * @return boolean
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月24日 上午12:08:36
     */
    public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) throws ZKInterruptedException {
        Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));
        LOG.debug("Waiting until node '" + path + "' becomes available.");
        if (exists(path)) {
            return true;
        }
        //可中断锁
        acquireEventLockInterruptibly();
        try {
        	//如果节点暂时不存在，则监听该节点。
        	//同时当前线程处于等待状态，如果节点被创建，则会触发监听事件，同时线程被唤醒
            while (!exists(path, true)) {
                boolean gotSignal = getEventLock().getNodeEventCondition().awaitUntil(timeout);
                if (!gotSignal) {
                    return false;
                }
            }
            LOG.debug("Node '" + path + "' is available.");
            return true;
        } catch (InterruptedException e) {
            throw new ZKInterruptedException(e);
        } finally {
           releaseEventLock();
        }
    }
    
    /**
     * 监听数据变化
     * @param path 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月23日 下午11:44:54
     */
    private void watchForData(final String path) {
    	LOG.debug("watch data change for path["+path+"]");
        retryUntilConnected(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                getZooKeeper().exists(path, true);
                return null;
            }
        });
    }

    /**
     * 监听子节点变化
     * 
     * @param path 父节点路径
     * @return 如果路径存在则返回路径下的子节点路径列表，否则返回null.
     */
    private List<String> watchForChilds(final String path) {
        return retryUntilConnected(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
            	//监听父节点
                exists(path, true);
                try {
                	//返回并监听子节点
                    return getChildren(path, true);
                } catch (ZKNoNodeException e) {
                    // 忽略，父节点没有被创建报错，因为设置了监听父节点，此处忽略
                }
                return null;
            }
        });
    }
    
	
    /**
     * 判断节点是否存在
     * @param path
     * @param watch 
     * 		true:将监听该节点的状态，包括创建、删除、修改节点数据，状态改变则会触发监听事件。
     * 		false:不启用监听
     * @return boolean 存在返回true，不存在返回false
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月23日 下午11:50:27
     */
	public boolean exists(final String path, final boolean watch) {
        return retryUntilConnected(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return getZooKeeper().exists(path, watch) != null;
            }
        });
    }
	
	/**
	 * 获取子节点
	 * @param path 父节点路径
	 * @param watch 
	 * 		true:监听该节点子节点状态，包括删除此节点、创建子节点，删除子节点，状态改变则会触发监听事件。
	 * 		false:不启用监听
	 * @return List<String> 子节点路径列表
	 * @author: zhaojie/zh_jie@163.com 
	 * @version: 2016年5月23日 下午11:52:55
	 */
	public List<String> getChildren(final String path, final boolean watch) {
        return retryUntilConnected(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
                return getZooKeeper().getChildren(path, watch);
            }
        });
    }
	

	/**
	 * 判断节点是否存在，如果当前client中设置了该节点的监听器，则启用对该节点的监听
	 * @param path
	 * @return 
	 * @return boolean
	 * @author: zhaojie/zh_jie@163.com 
	 * @version: 2016年5月24日 上午12:11:14
	 */
    public boolean exists(final String path) {
        return exists(path, hasListeners(path));
    }
    
    /**
     * 获得子节点，如果当前client中设置了该节点的子节点的监听器，则启用对该节点的子节点进行监听
     * @param path
     * @return 
     * @return List<String>
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月24日 上午12:12:03
     */
    public List<String> getChildren(String path) {
        return getChildren(path, hasListeners(path));
    }
    
    

    /**
     * 递归创建节点,如果节点的上层节点不存在，则自动创建
     * 上层节点都被创建为PERSISTENT类型的节点
     * @param path
     * @param data
     * @param createMode
     * @throws ZKInterruptedException
     * @throws ZKException
     * @throws RuntimeException 
     * @return void
     */
    public void createRecursive(String path, Object data, CreateMode createMode) 
    		throws ZKInterruptedException, ZKException, RuntimeException {
    	createRecursive(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,createMode);
    }

    /**
     * 递归创建节点,如果节点的上层节点不存在，则自动创建
     * 上层节点都被创建为PERSISTENT类型的节点
     * @param path
     * @param data
     * @param acl
     * @param createMode
     * @throws ZKInterruptedException
     * @throws ZKException
     * @throws RuntimeException 
     * @return void
     */
    public void createRecursive(String path , Object data, List<ACL> acl,CreateMode createMode) 
    		throws ZKInterruptedException, ZKException, RuntimeException {
        try {
            create(path, data, acl, createMode);
            System.out.println(path+" is created with data " + data +" mode "+ createMode);
        } catch (ZKNodeExistsException e) {
        	System.out.println(path+" not exists");
           //ignore
        } catch (ZKNoNodeException e) {
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createRecursive(parentDir, null, acl,createMode.PERSISTENT);
            create(path, data, acl,createMode);
            System.out.println(path+" is created...with data " + data +" mode "+ createMode);
        }
    }
    
    /**
     * 创建节点
     * @param path 节点路径
     * @param data 节点数据
     * @param createMode 创建的类型
     * @return
     * @throws ZKInterruptedException
     * @throws ZKException
     * @throws RuntimeException 
     * @return String
     * @author: zhaojie/zh_jie@163.com 
     */
    public String create(final String path, Object data, final CreateMode createMode) 
    		throws ZKInterruptedException, ZKException, RuntimeException {
        return create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    }
    
    
    /**
     * 创建节点并设置ACL（访问权限控制）
     * 
     * @param path 路径
     * @param data 数据
     * @param acl 访问控制配置
     * @param createMode 节点类型
     * @return 节点路径
     * @throws ZKInterruptedException
     * @throws ZKException
     * @throws RuntimeException
     */
    public String create(final String path, Object data, final List<ACL> acl, final CreateMode createMode) {
        if (path == null) {
            throw new NullPointerException("Missing value for path");
        }
        if (acl == null || acl.size() == 0) {
            throw new NullPointerException("Missing value for ACL");
        }
        final byte[] bytes = data == null ? null : serializer.serialize(data);

        LOG.debug("create path ["+path+"]");
        String retPath = retryUntilConnected(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return getZooKeeper().create(path, bytes, acl, createMode);
            }
        });
        LOG.debug("created path ["+path+"]");
        return retPath;
    }
    
    /**
     * 创建EPHEMERAL类型节点，该节点在连接断开后可重新自动创建。
     * 而使用{@link ZKClient#create(String, Object, CreateMode)}创建的EPHEMERAL类型节点，
     * 会在连接断开后消失，并且在重新连接后并不会重新创建。
     * @param path 路径
     * @param data 数据
     * @param sequential 
     * 		true:会创建{@link CreateMode#EPHEMERAL_SEQUENTIAL}类型的节点.
     * 		false:会创建{@link CreateMode#EPHEMERAL}类型的节点.
     * @return String
     * 		创建的节点路径
     */
    public String createEphemerale(final String path, Object data,Boolean sequential){
    	CreateMode createMode = CreateMode.EPHEMERAL;
    	if(sequential){
    		createMode = createMode.EPHEMERAL_SEQUENTIAL;
    	}
    	String retPath = create(path, data, createMode);
    	//将节点放入ephemeralNodeMap，用于重连后的自动创建
    	ephemeralNodeMap.put(path, new ZKNode(path,data,createMode));
    	return retPath;
    }
    
    /**
     * 创建EPHEMERAL类型节点，该节点在连接断开后可重新自动创建。
     * 而使用{@link ZKClient#create(String, Object, CreateMode)}创建的EPHEMERAL类型节点，
     * 会在连接断开后消失，并且在重新连接后并不会重新创建。
     * @param path 路径
     * @param data 数据
     * @param acl 访问控制配置
     * @param sequential 
     * 		true:会创建{@link CreateMode#EPHEMERAL_SEQUENTIAL}类型的节点.
     * 		false:会创建{@link CreateMode#EPHEMERAL}类型的节点.
     * @return String
     * 		创建的节点路径
     */
    public String createEphemerale(final String path, Object data, final List<ACL> acl,Boolean sequential){
    	CreateMode createMode = CreateMode.EPHEMERAL;
    	if(sequential){
    		createMode = createMode.EPHEMERAL_SEQUENTIAL;
    	}
    	String retPath = create(path, data, acl, createMode);
    	//将节点放入ephemeralNodeMap，用于重连后的自动创建
    	ephemeralNodeMap.put(path, new ZKNode(path,data,createMode));
    	return retPath;
    }
    
    /**
     * 重连后重新创建临时节点
     * @return void
     */
    private void recreatEphemeraleNode(){
    	for (Entry<String, ZKNode> entry : ephemeralNodeMap.entrySet()) {
    		ZKNode zkNode = entry.getValue();
    		LOG.debug("recreate ephemerale node " + entry.getKey());
    		try {
    			create(entry.getKey(), zkNode.getData(), zkNode.getCreateMode());
			} catch (ZKNodeExistsException e) {
				//ignore
			}
    	}
    }
    
    /**
     * 根据路径获取数据
     * @param path 节点路径
     * @return 
     * @return T
     */
    @SuppressWarnings("unchecked")
    public <T extends Object> T getData(String path) {
        return (T) getData(path, false);
    }

    /**
     * 根据路径获取数据，如果returnNullIfPathNotExists为true，
     * 则在路径不存在的情况下返回null，而不是抛出异常
     * 如果此路径设置了监听器，则会监听此节点
     * @param path 路径
     * @param returnNullIfPathNotExists 
     * 		值为true，在路径不存在的时候返回null，值为false，路径不存在的时候抛出异常
     * @return 
     * @return T
     */
    @SuppressWarnings("unchecked")
    public <T extends Object> T getData(String path, boolean returnNullIfPathNotExists) {
        T data = null;
        try {
            data = (T) getData(path, null);
        } catch (ZKNoNodeException e) {
            if (!returnNullIfPathNotExists) {
                throw e;
            }
        }
        return data;
    }

    /**
     * 根据路径返回数据及节点的stat信息。
     * 如果此路径设置了监听器，则会监听此节点
     * @param path 节点路径
     * @param stat 返回stat信息
     * @return 
     * @return T
     */
    @SuppressWarnings("unchecked")
    public <T extends Object> T getData(String path, Stat stat) {
        return (T) getData(path,hasListeners(path), stat);
    }

    
    /**
     * 根据路径返回节点信息
     * @param path 节点路径
     * @param watch 是否监听
     * @param stat stat信息
     * @return 
     * @return T
     */
    @SuppressWarnings("unchecked")
	private <T extends Object> T getData(final String path,final boolean watch,final Stat stat) {
        byte[] data = retryUntilConnected(new Callable<byte[]>() {
            @Override
            public byte[] call() throws Exception {
                return getZooKeeper().getData(path, watch, stat);
            }
        });
        if (data == null) {
            return null;
        }
        return (T) serializer.deserialize(data);
    }
    
    
    /**
     * 设置节点的值
     * @param path 节点路径
     * @param object 需要设置的值
     * @return Stat 返回stat信息
     */
    public Stat setData(String path, Object object) {
        return setData(path, object, -1);
    }

    /**
     * 设置节点的值
     * @param path 节点路径
     * @param object 需要设置的值
     * @param version 版本号
     * @return Stat Stat 返回stat信息
     */
    public Stat setData(final String path, Object datat, final int version) {
        final byte[] data = serializer.serialize(datat);
        LOG.debug("set data path["+path+"]");
        return (Stat) retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                Stat stat = getZooKeeper().setData(path, data, version);
                LOG.debug("set data path["+path+"] done");
                return stat;
            }
        });
    }
    
    /**
     * 删除节点所有版本的信息，只能删除子节点
     * @param path 要删除的节点的路径
     * @return 
     * @return boolean
     */
    public boolean delete(final String path) {
        return delete(path, -1);
    }

    /**
     * 删除节点的指定版本的信息，只能删除子节点
     * @param path 节点路径
     * @param version 版本号
     * @return boolean 成功返回true,否则返回false
     * @author: zhaojie/zh_jie@163.com 
     */
    public boolean delete(final String path, final int version) {
        try {
        	LOG.debug("delete path ["+path+"]..");
        	retryUntilConnected(new Callable<Object>() {

                @Override
                public Object call() throws Exception {
                    getZooKeeper().delete(path, version);
                    LOG.debug("deleted path ["+path+"]");
                    return null;
                }
            });

            return true;
        } catch (ZKNoNodeException e) {
            return false;
        }
    }
    
    /**
     * 删除节点，并递归删除所有的子节点
     * @param path 节点路径
     * @return 
     * @return boolean
     * @author: zhaojie/zh_jie@163.com 
     */
    public boolean deleteRecursive(String path) {
        List<String> children;
        try {
            children = getChildren(path, false);
        } catch (ZKNoNodeException e) {
            return true;
        }
        for (String subPath : children) {
            if (!deleteRecursive(path + "/" + subPath)) {
                return false;
            }
        }
        return delete(path);
    }

    
    /**
     * 设置访问权限
     * @param path
     * @param acl
     * @throws ZKException 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public void setAcl(final String path, final List<ACL> acl) throws ZKException {
        if (path == null) {
            throw new NullPointerException("Missing value for path");
        }

        if (acl == null || acl.size() == 0) {
            throw new NullPointerException("Missing value for ACL");
        }

        if (!exists(path)) {
            throw new RuntimeException("trying to set acls on non existing node " + path);
        }

        retryUntilConnected(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Stat stat = new Stat();
                getZooKeeper().getData(path,false,stat);
                getZooKeeper().setACL(path,acl,stat.getAversion());
                return null;
            }
        });
    }

    /**
     * 获取访问权限
     * @param path
     * @return
     * @throws ZKException 
     * @return Map.Entry<List<ACL>,Stat>
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月27日 上午8:17:52
     */
    public Map.Entry<List<ACL>, Stat> getAcl(final String path) throws ZKException {
        if (path == null) {
            throw new NullPointerException("Missing value for path");
        }

        if (!exists(path)) {
            throw new RuntimeException("trying to get acls on non existing node " + path);
        }

        return retryUntilConnected(new Callable<Map.Entry<List<ACL>, Stat>>() {
            @Override
            public Map.Entry<List<ACL>, Stat> call() throws Exception {
            	 Stat stat = new Stat();
                 List<ACL> acl = getZooKeeper().getACL(path, stat);
                 return new SimpleEntry(acl, stat);
            }
        });
    }
    
    /**
     * 重试直到重新连接位置，参考{@link ZKConnectionImpl#retryUntilConnected(Callable)}}
     * @param callable 连接成功后回调函数
     * @return
     * @throws ZKInterruptedException
     * @throws ZKException
     * @throws RuntimeException 
     * @return T
     */
    private <T> T retryUntilConnected(Callable<T> callable) throws ZKInterruptedException, ZKException, RuntimeException {
 	   if(closed){
 		   throw new ZKException("ZKClient is closed!");
 	   }
 	   return connection.retryUntilConnected(callable);
    }
   
    
    /**
     * 添加认证信息，用于访问被ACL保护的节点
     * @param scheme
     * @param auth 
     * @return void
     */
    public void addAuthInfo(final String scheme, final byte[] auth) {
    	connection.addAuthInfo(scheme, auth);
    }
    
    /**
     * 获得锁
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public void acquireEventLock(){
		connection.acquireEventLock();
	}
    
    /**
     * 释放锁
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
	public void releaseEventLock(){
		connection.releaseEventLock();
	}
	
	/**
	 * 尝试获得可中断锁
	 * @return void
	 * @author: zhaojie/zh_jie@163.com 
	 */
	public void acquireEventLockInterruptibly() {
       connection.acquireEventLockInterruptibly();
    }

	public boolean getShutdownTrigger() {
		return shutdownTrigger;
	}

	public KeeperState getCurrentState() {
		return connection.getCurrentState();
	}

	public void setCurrentState(KeeperState currentState) {
        connection.setCurrentState(currentState);
	}
	
	public Map<String, CopyOnWriteArraySet<ZKListener>> getChildListenerMap() {
		return childListenerMap;
	}

	public Map<String, CopyOnWriteArraySet<ZKListener>> getNodeListenerMap() {
		return nodeListenerMap;
	}

	public Set<ZKStateListener> getStateListenerSet() {
		return stateListenerSet;
	}

	public ZKConnection getConnection() {
		return connection;
	}

	public ZKEventLock getEventLock() {
		return connection.getEventLock();
	}
	
	public ZooKeeper getZooKeeper(){
		return this.connection.getZooKeeper();
	}

	public int getEventThreadPoolSize() {
		return eventThreadPoolSize;
	}
}
