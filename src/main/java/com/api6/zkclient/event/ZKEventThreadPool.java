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
package com.api6.zkclient.event;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;  

/**
 * 事件处理线程池
 * 阻塞队列无限制
 * @author: zhaojie/zh_jie@163.com.com 
 */
public class ZKEventThreadPool {
    private static Logger LOG = LoggerFactory.getLogger(ZKEventThreadPool.class);
    
    private ThreadPoolExecutor pool = null;    
    private static AtomicInteger index = new AtomicInteger(0);
    
    /**
     * 初始化线程池，这里采用corePoolSize=maximumPoolSize，
     * 并且使用LinkedBlockingQueue无限大小的阻塞队列来处理事件
     * ZKEventThreadPoolExecutor. 
     * 
     * @param poolSize
     */
    public ZKEventThreadPool(Integer poolSize){
        pool = new ThreadPoolExecutor(
                poolSize,         //corePoolSize 核心线程池大小
                poolSize,         //aximumPoolSize 最大线程池大小
                30,               //keepAliveTime 线程池中超过corePoolSize数目的空闲线程最大存活时间
                TimeUnit.MINUTES, //TimeUnit keepAliveTime时间单位 这里是秒
                new LinkedBlockingQueue<Runnable>(),   //workQueue 阻塞队列
                new ZKEventThreadFactory());           //线程工厂
    }

    /**
     * 销毁线程池
     * @return void
     */
    public void destory() {
        if(pool != null) {
            pool.shutdownNow();
        }
    }
    
    /**
     * 处理事件
     * @param zkEvent 
     * @return void
     * @author: zhaojie/zh_jie@163.com 
     */
    public void submit(final ZKEvent zkEvent){
        pool.submit(new Runnable() {
            @Override
            public void run() {
                 int eventId = index.incrementAndGet();
                 try {
                    LOG.debug("Handling event-" + eventId + " " + zkEvent);
                    zkEvent.run();
                } catch (Exception e) {
                     LOG.error("Error handling event [" + zkEvent+"]", e);
                }
                LOG.debug("Handled the event-" + eventId);
            }
        });
    }
    
    
    /**
     * 私有内部类，线程创建工厂
     * @author: zhaojie/zh_jie@163.com.com 
     * @version: 2016年5月26日 下午9:06:53
     */
    private class ZKEventThreadFactory implements ThreadFactory {
        private AtomicInteger count = new AtomicInteger(0);    
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);    
            String threadName = "ZkClient-EventThread-" + count.addAndGet(1);    
            t.setName(threadName);    
            return t;    
        }
    }
}