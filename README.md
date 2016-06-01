#ZKClient

这是一个ZooKeeper客户端，实现了断线重连，会话过期重连，永久监听，子节点数据变化的监听。并且加入了常用功能，例如分布式锁，主从服务锁，分布式队列等。
* * *
#使用说明

##一、创建ZKClient对象
有两种方式可以方便的创建ZKClient对象。

1. 使用构造函数创建

        String address = "localhost:"+zkServer.getPort();
        ZKClient zkClient1 = new ZKClient(address);
        ZKClient zkClient2 = new ZKClient(address,500);
        ZKClient zkClient3 = new ZKClient(address,500,1000*60);
        ZKClient zkClient4 = new ZKClient(address,500,1000*60,new BytesSerializer());
        ZKClient zkClient5 = new ZKClient(address,500,1000*60,new BytesSerializer(),Integer.MAX_VALUE);
        ZKClient zkClient6 = new ZKClient(address,500,1000*60,new BytesSerializer(),Integer.MAX_VALUE,2);
2. 使用辅助建造类

        ZKClient zkClient = ZKClientBuilder.newZKClient(address)
                            .sessionTimeout(1000)//可选
                            .serializer(new SerializableSerializer())//可选
                            .eventThreadPoolSize(1)//可选
                            .retryTimeout(1000*60)//可选
                            .connectionTimeout(Integer.MAX_VALUE)//可选
                            .build();创建实例

* * *

##二、节点的新增、更新、删除和获取
###新增节点
1. 常规新增节点

        zkClient.create("/test1", "123", CreateMode.EPHEMERAL);
        zkClient.create("/test1-1",123,CreateMode.EPHEMERAL_SEQUENTIAL);
        zkClient.create("/test1-2",123,CreateMode.PERSISTENT);
        zkClient.create("/test1-3",123,CreateMode.PERSISTENT_SEQUENTIAL);
2. 递归新增节点及父节点

        String path = "/test8/1/2/3";
        //递归创建节点及父节点
        zkClient.createRecursive(path, "abc", CreateMode.PERSISTENT);
        zkClient.createRecursive(path, "123", ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
    
3. 特殊的EPHEMERAL类型节点

    特殊类型的EPHEMERAL节点，该节点在连接断开后可重新自动创建。
    
        String path = "/test8/1/2/3";
        //EPHEMERAL类型节点
        zkClient.createEphemerale(path, "123", false);
        zkClient.createEphemerale(path, "123",ZooDefs.Ids.CREATOR_ALL_ACL, false);
        //EPHEMERAL_SEQUENTIAL类型
        String retPath = zkClient.createEphemerale(path, "456", true);
    

###更新节点数据
    
    String path = "/test";
    zkClient.setData(path, "456");
    zkClient.setData(path, "123", 2);
    

###删除节点
1. 常规删除

        boolean flag =  zkClient.delete("/test");//删除任意版本
        boolean flag =  zkClient.delete("/test",1);//删除指定版本
        
2. 递归删除（删除节点及子节点）

        String path = "/test8/1/2/3";
        //递归删除子节点
        zkClient.deleteRecursive(path);


###获取节点数据
    
    String path = "/test";
    //如果节点不存在抛出异常
    zkClient.getData(path);
    //如果节点不存在返回null
    zkClient.getData(path, true);
    //获得数据以及stat信息
    Stat stat = new Stat();
    zkClient.getData(path, stat);


###等待节点创建

    String path = "/test";
    //等待直到超时或者节点创建成果。
    zkClient.waitUntilExists(path, TimeUnit.MILLISECONDS, 1000*5);
    
* * *

##三、监听相关
###节点监听

         zkClient.listenNodeChanges(path, new ZKNodeListener() {
            @Override
            public void handleSessionExpired(String path) throws Exception {
                System.out.println("session  expired ["+path+"]");
            }
            
            @Override
            public void handleDataDeleted(String path) throws Exception {
                System.out.println("node is deleted ["+path+"]");
            }
            
            @Override
            public void handleDataCreated(String path, Object data) throws Exception {
                System.out.println("node is created ["+path+"]");
            }
            
            @Override
            public void handleDataChanged(String path, Object data) throws Exception {
                System.out.println("node is changed ["+path+"]");
            }
        });
###子节点数量监听
        
         zkClient.listenChildCountChanges(path, new ZKChildCountListener() {
            
            @Override
            public void handleSessionExpired(String path, List<String> children) throws Exception {
                System.out.println("children:"+children);
            }
            
            @Override
            public void handleChildCountChanged(String path, List<String> children) throws Exception {
                 System.out.println("children:"+children);
            }
        });
###子节点数量和子节点数据变化监听
        
        
        zkClient.listenChildDataChanges(path, new ZKChildDataListener() {
            @Override
            public void handleSessionExpired(String path, Object data) throws Exception {
               System.out.println("children:"+children);
            }
            
            @Override
            public void handleChildDataChanged(String path, Object data) throws Exception {
                System.out.println("the child data is changed:[path:"+path+",data:"+data+"]");
            }
            
            @Override
            public void handleChildCountChanged(String path, List<String> children) throws Exception {
               System.out.println("children:"+children);
            }
        });
        
* * *

##四、扩展功能
    
###分布式锁

    ZKClient zkClient = ZKClientBuilder.newZKClient()
                                .servers("localhost:2181")
                                .sessionTimeout(1000)
                                .build();
    final String lockPath = "/zk/lock";
    zkClient.createRecursive(lockPath, null, CreateMode.PERSISTENT);
    //创建分布式锁
    ZKDistributedLock lock = ZKDistributedLock.newInstance(zkClient,lockPath);
   
    lock.lock(); //获得锁
    //do someting
    lock.unlock();//释放锁
###分布式队列
    
    ZKClient zkClient = ZKClientBuilder.newZKClient()
                                .servers("localhost:2181")
                                .sessionTimeout(1000)
                                .build();
    final String lockPath = "/zk/queue";
    zkClient.createRecursive(rootPath, null, CreateMode.PERSISTENT);
    
    //创建分布式队列对象
    ZKDistributedQueue<String> queue = new ZKDistributedQueue(zkClient, rootPath);
    //放入元素
    queue.offer("123");
    //删除并获取顶部元素
    String value = queue.poll();
    //获取顶部元素，不会删除
    String value =  queue.peek();
    
###主从服务锁

    ZKClient zkClient = ZKClientBuilder.newZKClient()
                                .servers("localhost:2181")
                                .sessionTimeout(1000)
                                .build();
    final String lockPath = "/zk/halock";
    zkClient.createRecursive(rootPath, null, CreateMode.PERSISTENT);
    //尝试获取锁
    lock.lock();
    //获取锁成功，当前线程变为主服务
