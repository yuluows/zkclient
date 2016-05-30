package com.api6.zkclient;

import org.apache.zookeeper.CreateMode;

class ZKNode {
	private String path;
	private Object data;
	private CreateMode createMode;
	
	public ZKNode() {
	}
	
	public ZKNode(String path,Object data,CreateMode createMode) {
		this.path = path;
		this.data = data;
		this.createMode = createMode;
	}
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public Object getData() {
		return data;
	}
	public void setData(Object data) {
		this.data = data;
	}
	public CreateMode getCreateMode() {
		return createMode;
	}
	public void setCreateMode(CreateMode createMode) {
		this.createMode = createMode;
	}
	
	
}
