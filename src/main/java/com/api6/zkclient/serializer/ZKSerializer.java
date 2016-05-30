
package com.api6.zkclient.serializer;

public interface ZKSerializer {

    public byte[] serialize(Object data);

    public Object deserialize(byte[] bytes);
}
