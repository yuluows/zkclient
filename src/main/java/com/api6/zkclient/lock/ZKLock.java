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
package com.api6.zkclient.lock;

public interface ZKLock {
    /**
     * 获得锁
     * @param timeout 超时时间
     *         如果超时间大于0，则会在超时后直接返回false。
     *         如果超时时间小于等于0，则会等待直到获取锁为止。
     * @return 
     * @return boolean 成功获得锁返回true，否则返回false
     */
    boolean lock(int timeout);
    /**
     * 释放锁
     * @return void
     */
    void unlock();
}
