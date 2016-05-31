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
package com.api6.zkclient.queue;

import java.io.Serializable;
import java.util.List;

import org.apache.zookeeper.CreateMode;

import com.api6.zkclient.ZKClient;
import com.api6.zkclient.exception.ZKNoNodeException;
import com.api6.zkclient.util.ExceptionUtil;

/**
 * 分布式队列
 * @author: zhaojie/zh_jie@163.com.com 
 * @version: 2016年5月31日 下午6:19:11
 */
public class ZKDistributedQueue<T extends Serializable> {
    private ZKClient zkClient;
    private String rootPath;
    private static final String ELEMENT_NAME = "element";
    
    private class Element<T> {
        private String name;
        private T data;

        public Element(String name, T data) {
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        public T getData() {
            return data;
        }
    }

    public ZKDistributedQueue(ZKClient zkClient, String root) {
        this.zkClient = zkClient;
        this.rootPath = root;
        if(!zkClient.exists(root)){
            throw new ZKNoNodeException("The root is not exists!,please create the node.[path:"+root+"]");
        }
    }

    
    /**
     * 添加一个元素
     * @param element
     * @return 
     * @return boolean
     */
    public boolean offer(T element) {
        try {
            zkClient.create(rootPath + "/" + ELEMENT_NAME + "-", element, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
        return true;
    }

    /**
     * 删除并返回顶部元素
     * @return 
     * @return T
     */
    public T poll() {
        while (true) {
            Element<T> element = getFirstElement();
            if (element == null) {
                return null;
            }

            try {
                boolean flag = zkClient.delete(element.getName());
                if(flag){
                    return element.getData();
                }else{
                  //如果删除失败，证明已被其他线程获取
                  //重新获取最新的元素，直到获取成功为止。
                }
            } catch (Exception e) {
                throw ExceptionUtil.convertToRuntimeException(e);
            }
        }
    }


    /**
     * 获取顶部元素
     * @return 
     * @return T
     */
    public T peek() {
        Element<T> element = getFirstElement();
        if (element == null) {
            return null;
        }
        return element.getData();
    }
    
    
    /**
     * 获取队列顶部元素
     * @return 
     * @return Element<T>
     */
    @SuppressWarnings("unchecked")
    private Element<T> getFirstElement() {
        try {
            while (true) {
                List<String> list = zkClient.getChildren(rootPath);
                if (list.size() == 0) {
                    return null;
                }
                String elementName = getSmallestElement(list);

                try {
                    return new Element<T>(rootPath + "/" + elementName, (T) zkClient.getData(rootPath + "/" + elementName));
                } catch (ZKNoNodeException e) {
                    //如果抛出此异常，证明该节点已被其他线程获取。
                    //此时忽略异常，重新获取最新的元素，直到获取成功为止。
                }
            }
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
    }
    
    /**
     * 获得最小节点
     * @param list
     * @return 
     * @return String
     */
    private String getSmallestElement(List<String> list) {
        String smallestElement = list.get(0);
        for (String element : list) {
            if (element.compareTo(smallestElement) < 0) {
                smallestElement = element;
            }
        }
        return smallestElement;
    }

    /**
     * 判断队列是否为空
     * @return 
     * @return boolean
     */
    public boolean isEmpty() {
        return zkClient.getChildren(rootPath).size() == 0;
    }
}
