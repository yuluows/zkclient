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
package com.api6.zkclient;

import com.api6.zkclient.exception.ZKException;
import com.api6.zkclient.serializer.SerializableSerializer;
import com.api6.zkclient.serializer.ZKSerializer;

/**
 * ZKClient辅助创建类
 * @author: zhaojie/zh_jie@163.com.com 
 */
public class ZKClientBuilder {
    private int connectionTimeout = Integer.MAX_VALUE;
    private ZKSerializer zkSerializer = new SerializableSerializer();
    private int eventThreadPoolSize = 1;
    private String servers;
    private int sessionTimeout = 30000;
    private int retryTimeout = -1;
    
    /**
     * 创建ZClient
     * @return 
     * @return ZKClientBuilder
     * @author: zhaojie/zh_jie@163.com 
     */
    public static ZKClientBuilder newZKClient(){
        ZKClientBuilder builder = new ZKClientBuilder();
        return builder;
    }
    
    /**
     * 创建ZClient
     * @param servers
     * @return 
     * @return ZKClientBuilder
     * @author: zhaojie/zh_jie@163.com 
     */
    public static ZKClientBuilder newZKClient(String servers){
        ZKClientBuilder builder = new ZKClientBuilder();
        builder.servers(servers);
        return builder;
    }
    
    /**
     * 组件并初始化ZKClient
     * @return 
     * @return ZKClient
     */
    public ZKClient build(){
        if(servers==null || servers.trim().equals("")){
            throw new ZKException("Servers can not be empty !");
        }
        ZKClient zkClient = new ZKClient(servers,sessionTimeout,retryTimeout,zkSerializer,connectionTimeout,eventThreadPoolSize);
        return zkClient;
    }
    
    /**
     * 设置服务器地址
     * @param servers
     * @return 
     * @return ZKClientBuilder
     */
    public ZKClientBuilder servers(String servers){
        this.servers = servers;
        return this;
    }
    
    /**
     * 设置序列化类，可选.
     *     （默认实现:{@link SerializableSerializer}）
     * @param zkSerializer
     * @return 
     * @return ZKClientBuilder
     */
    public ZKClientBuilder serializer(ZKSerializer zkSerializer){
        this.zkSerializer = zkSerializer;
        return this;
    }
    
    /**
     * 设置会话失效时间，可选
     *     （默认值：30000，实际大小ZooKeeper会重新计算大概在2 * tickTime ~ 20 * tickTime）
     * @param sessionTimeout
     * @return 
     * @return ZKClientBuilder
     * @author: zhaojie/zh_jie@163.com 
     */
    public ZKClientBuilder sessionTimeout(int sessionTimeout){
        this.sessionTimeout = sessionTimeout;
        return this;
    }
    
    /**
     * 连接超时时间，可选。
     * 默认值Integer.MAX_VALUE
     * @param connectionTimeout
     * @return 
     * @return ZKClientBuilder
     * @author: zhaojie/zh_jie@163.com 
     */
    public ZKClientBuilder connectionTimeout(int connectionTimeout){
        this.connectionTimeout = connectionTimeout;
        return this;
    }
    
    /**
     * 重试超时时间，可选，主要用于ZooKeeper与服务器断开后的重连。
     * 默认值-1，也就是没有超时限制
     * @param retryTimeout
     * @return 
     * @return ZKClientBuilder
     * @author: zhaojie/zh_jie@163.com 
     */
    public ZKClientBuilder retryTimeout(int retryTimeout){
        this.retryTimeout = retryTimeout;
        return this;
    }
    
    /**
     * 处理事件的线程数，可选，默认值为1
     * @param eventThreadPoolSize
     * @return 
     * @return ZKClientBuilder
     * @author: zhaojie/zh_jie@163.com 
     */
    public ZKClientBuilder eventThreadPoolSize(int eventThreadPoolSize){
        this.eventThreadPoolSize = eventThreadPoolSize;
        return this;
    }
    
}
