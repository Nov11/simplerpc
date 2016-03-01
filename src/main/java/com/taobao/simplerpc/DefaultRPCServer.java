/**
 * $Id: DefaultRPCServer.java 887 2012-12-29 05:35:06Z shijia.wxr $
 */
package com.taobao.simplerpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Copyright 2016-3-1 vintage.wang@gmail.com shijia.wxr@taobao.com
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 529079634@qq.com made comments on the source code
 */

/**
 * 服务端实现
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class DefaultRPCServer implements RPCServer {
    private final int listenPort;
    private ServerSocketChannel serverSocketChannel;
    private Selector selector;
    private SocketAddress socketAddressListen;
    private RPCProcessor rpcServerProcessor;

    private List<Connection> connectionList = new LinkedList<Connection>();
    private final AcceptSocketService acceptSocketService = new AcceptSocketService();
    private final ThreadPoolExecutor executor;

    class AcceptSocketService extends ServiceThread {

        public void run() {
            System.out.println(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    DefaultRPCServer.this.selector.select(1000);
                    Set<SelectionKey> selected = DefaultRPCServer.this.selector.selectedKeys();
                    //转list为了做shuffle，shuffle影响链接被处理的优先级，这么做应该是增加随机性吧
                    //个人感觉作用不大
                    ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(selected);
                    Collections.shuffle(selectedList);
                    for (SelectionKey k : selectedList) {
                        //与isAcceptable()等价
                        if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                            SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();
                            System.out.println("receive new connection, " + sc.socket().getRemoteSocketAddress());
                            Connection newConnection =
                                    new Connection(sc, DefaultRPCServer.this.rpcServerProcessor,
                                        DefaultRPCServer.this.executor);

                            // if (DefaultRPCServer.this.clientConnection !=
                            // null) {
                            // System.out.println("close old client connection, "
                            // +
                            // DefaultRPCServer.this.clientConnection.getSocketChannel().socket()
                            // .getRemoteSocketAddress());
                            // DefaultRPCServer.this.clientConnection.shutdown();
                            // }

                            DefaultRPCServer.this.connectionList.add(newConnection);
                            newConnection.start();
                        }
                        // TODO， CLOSE SOCKET
                        else {
                            System.out.println("Unexpected ops in select " + k.readyOps());
                        }
                    }

                    selected.clear();
                }
                catch (Exception e) {
                    System.out.println(this.getServiceName() + " service has exception.");
                    System.out.println(e.getMessage());
                }
            }

            System.out.println(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }


    public DefaultRPCServer(final int listenPort, final int minPoolSize, final int maxPoolSize) throws IOException {
        this.listenPort = listenPort;
        this.socketAddressListen = new InetSocketAddress(this.listenPort);
        this.serverSocketChannel = ServerSocketChannel.open();
        this.selector = Selector.open();
        //SO_REUSEADDR 得在bind之前设置，否则能否生效是不确定的
        this.serverSocketChannel.socket().setReuseAddress(true);
        //给关联的socket一个地址，本机的listenPort端口
        this.serverSocketChannel.socket().bind(this.socketAddressListen);
        //设置成非阻塞的
        //监听设置成非阻塞
        /**
         * 为啥accept也要设置成非阻塞？
         * quote:
         * accept() - Unix, Linux System Call
         * There may not always be a connection waiting after a SIGIO is delivered or select(2) or poll(2) return a readability event
         * because the connection might have been removed by an asynchronous network error or another thread before accept() is called.
         * If this happens then the call will block waiting for the next connection to arrive.
         * To ensure that accept() never blocks, the passed socket sockfd needs to have the O_NONBLOCK flag set (see socket(7)).
         */
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);

        //线程池
        this.executor =
                new ThreadPoolExecutor(minPoolSize, maxPoolSize, 60L, TimeUnit.SECONDS,
                    new SynchronousQueue<Runnable>(), new ThreadFactory() {
                        private volatile long threadCnt = 0;


                        public Thread newThread(Runnable r) {
                            return new Thread(r, "RPCHandleThreadPool_" + String.valueOf(this.threadCnt++));
                        }
                    });
    }


    public void start() {
        this.acceptSocketService.start();
    }


    public void shutdown() {
        //关掉监听端口的线程
        this.acceptSocketService.shutdown();

        //关闭建立的链接
        for (Connection c : this.connectionList) {
            c.shutdown();
        }

        //清掉selector
        try {
            this.selector.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        //关闭server channel
        try {
            this.serverSocketChannel.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void registerProcessor(RPCProcessor processor) {
        this.rpcServerProcessor = processor;
    }
}
