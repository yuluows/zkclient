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
package com.api6.zkclient.event;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ZooKeeper事件锁
 * @author: zhaojie/zh_jie@163.com.com 
 */
public class ZKEventLock extends ReentrantLock {

    private static final long serialVersionUID = 1L;

    private Condition nodeOrChildChangedCondition = newCondition();
    private Condition stateChangedCondition = newCondition();
    private Condition nodeEventCondition = newCondition();

  
    /**
     * 条件在节点改变（节点新增、修改、删除）或者子节点数量改变时被标记。
     * @return 
     * @return Condition
     * @author: zhaojie/zh_jie@163.com 
     */
    public Condition getNodeOrChildChangedCondition() {
        return nodeOrChildChangedCondition;
    }

    
    /**
     * 条件在ZooKeeper状态发生改变时被标记，包括，连接成功，断开连接，会话失效等。
     * @return 
     * @return Condition
     * @author: zhaojie/zh_jie@163.com 
     * @version: 2016年5月26日 上午11:04:20
     */
    public Condition getStateChangedCondition() {
        return stateChangedCondition;
    }

   /**
    * 该条件在节点发生变化时会被标记
    * @return 
    * @return Condition
    * @author: zhaojie/zh_jie@163.com 
    * @version: 2016年5月26日 上午11:03:19
    */
    public Condition getNodeEventCondition() {
        return nodeEventCondition;
    }
}