package com.api6.zkclient.event;

public abstract class ZKEvent {
	 private String description;

     public ZKEvent(String description) {
         this.description = description;
     }

     public abstract void run() throws Exception;

     @Override
     public String toString() {
         return "ZKEvent[" + description + "]";
     }
}
