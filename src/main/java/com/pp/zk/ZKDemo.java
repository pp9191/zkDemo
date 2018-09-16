package com.pp.zk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZKDemo {
	

	// 会话超时时间，设置为与系统默认时间一致
    private static final int SESSION_TIMEOUT = 30 * 1000;
    private static final String CONNECT_ADDR = "192.168.0.104:2181";

    // 创建 ZooKeeper 实例
    private ZooKeeper zk;       

    // 初始化 ZooKeeper 实例
    private void createZKInstance() throws IOException, InterruptedException {
    	CountDownLatch connectedSemaphore = new CountDownLatch(1);
        // 连接到ZK服务，多个可以用逗号分割写
    	zk = new ZooKeeper(CONNECT_ADDR, SESSION_TIMEOUT, new Watcher(){
			
			public void process(WatchedEvent event) {
				//获取事件的状态
				KeeperState keeperState = event.getState();
				EventType eventType = event.getType();
				//如果是建立连接
				if(KeeperState.SyncConnected == keeperState){
					if(EventType.None == eventType){
						//如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
						connectedSemaphore.countDown();
						System.out.println("zk 建立连接");
					}
				}
			}
		}); 
		//进行阻塞
    	System.out.println("zk 连接中");
		connectedSemaphore.await();
    }

    private void ZKOperations() throws IOException, InterruptedException, KeeperException {
        zk.create("/zoo2", "myData2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("\n1. 创建 ZooKeeper 节点 (znode ： zoo2, 数据： myData2 ，权限： OPEN_ACL_UNSAFE ，节点类型： Persistent");

        System.out.println("\n2. 查看是否创建成功： ");
        System.out.println(new String(zk.getData("/zoo2", false, null)));// 添加Watch

        // 前面一行我们添加了对/zoo2节点的监视，所以这里对/zoo2进行修改的时候，会触发Watch事件。
        zk.setData("/zoo2", "shanhy20160310".getBytes(), -1);
        System.out.println("\n3. 修改节点数据 ");

        // 这里再次进行修改，则不会触发Watch事件，这就是我们验证ZK的一个特性“一次性触发”，也就是说设置一次监视，只会对下次操作起一次作用。
        zk.setData("/zoo2", "shanhy20160310-ABCD".getBytes(), -1);
        System.out.println("\n3-1. 再次修改节点数据 ");

        System.out.println("\n4. 查看是否修改成功： ");
        System.out.println(new String(zk.getData("/zoo2", false, null)));

        zk.delete("/zoo2", -1);
        System.out.println("\n5. 删除节点 ");

        System.out.println("\n6. 查看节点是否被删除： ");
        System.out.println(" 节点状态： [" + zk.exists("/zoo2", false) + "]");
    }

    private void ZKClose() throws InterruptedException {
        zk.close();
    }

    public static void main(String[] args) {
    	ZKDemo dm = new ZKDemo();
    	try {
			dm.createZKInstance();
			dm.ZKOperations();
			dm.ZKClose();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

}
