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
package com.api6.zkclient.serializer;

/**
 * 对Byte数组序列化，只是简单的原样返回
 * @author: zhaojie/zh_jie@163.com.com 
 */
public class BytesSerializer implements ZKSerializer {

    @Override
    public Object deserialize(byte[] bytes){
        return bytes;
    }

    @Override
    public byte[] serialize(Object bytes){
        return (byte[]) bytes;
    }

}
