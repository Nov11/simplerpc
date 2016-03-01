/**
 * $Id: Connection.java 912 2013-01-04 03:13:49Z shijia.wxr $
 */
package com.taobao.simplerpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import com.taobao.simplerpc.LinkedByteBufferList.ByteBufferNode;

/**
 * Copyright 2016-3-1 vintage.wang@gmail.com shijia.wxr@taobao.com
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * 529079634@qq.com made comments on the source code
 * <p>
 * 529079634@qq.com made comments on the source code
 */

/**
 * 529079634@qq.com made comments on the source code
 */

/**
 * 一个Socket连接对象，Client与Server通用
 *
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class Connection {
    private static final int ReadMaxBufferSize = 1024 * 1024 * 4;

    private final SocketChannel socketChannel;
    private final RPCProcessor rpcServerProcessor;
    private final ThreadPoolExecutor executor;
    private final LinkedByteBufferList linkedByteBufferList = new LinkedByteBufferList();
    //下一个要处理的位置
    private int dispatchPosition = 0;
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(ReadMaxBufferSize);

    private WriteSocketService writeSocketService;
    private ReadSocketService readSocketService;

    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;

        /**
         * 把socketchannel加到selector里 关注write事件
         * 问题是，如果一直可写，比如缓冲很大的情况，每次select这个channel都是可写的，多路复用会变成轮询吧？
         * @param socketChannel 要处理的socketchannel
         * @throws IOException
         */
        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = Selector.open();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
        }

        public void run() {
            System.out.println(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    /**
                     * 单位是毫秒
                     */
                    this.selector.select(1000);
                    int writeSizeZeroTimes = 0;
                    while (true) {
                        ByteBufferNode node = Connection.this.linkedByteBufferList.waitForPut(100);
                        if (node != null) {
                            //写了多少读多少
                            node.getByteBufferRead().limit(node.getWriteOffset().get());
                            int writeSize = this.socketChannel.write(node.getByteBufferRead());
                            if (writeSize > 0) {
                                //空白
                            } else if (writeSize == 0) {
                                if (++writeSizeZeroTimes >= 3) {
                                    break;
                                }
                            } else {
                                //空白
                            }
                        } else {
                            break;
                        }
                    }
                } catch (Exception e) {
                    System.out.println(this.getServiceName() + " service has exception.");
                    System.out.println(e.getMessage());
                    break;
                }
            }

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }


        @Override
        public void shutdown() {
            super.shutdown();
        }
    }

    class ReadSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;


        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = Selector.open();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
        }


        public void run() {
            System.out.println(this.getServiceName() + " service started");

            while (!this.isStoped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = Connection.this.processReadEvent();
                    if (!ok) {
                        System.out.println("processReadEvent error");
                        break;
                    }
                } catch (Exception e) {
                    System.out.println(this.getServiceName() + " service has exception.");
                    System.out.println(e.getMessage());
                    break;
                }
            }

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println(this.getServiceName() + " service end");
        }


        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }
    }

    /**
     * 存下通信的socketchannel，请求处理器，还有线程池
     * 把socketchannel设置成非阻塞的，关闭linger和nagle‘s算法 给一个建议的recv send buf
     * 再为读写socketchannel各分配一个线程
     * @param socketChannel socket
     * @param rpcServerProcessor 请求处理器
     * @param executor 线程池
     */
    public Connection(final SocketChannel socketChannel, final RPCProcessor rpcServerProcessor,
                      final ThreadPoolExecutor executor) {
        this.socketChannel = socketChannel;
        this.rpcServerProcessor = rpcServerProcessor;
        this.executor = executor;

        try {
            this.socketChannel.configureBlocking(false);
            this.socketChannel.socket().setSoLinger(false, -1);
            this.socketChannel.socket().setTcpNoDelay(true);
            this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
            this.socketChannel.socket().setSendBufferSize(1024 * 64);
            this.writeSocketService = new WriteSocketService(this.socketChannel);
            this.readSocketService = new ReadSocketService(this.socketChannel);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }


    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }


    /**
     * 处理select读事件
     *
     * @return 返回处理结果
     */
    public boolean processReadEvent() {
        int readSizeZeroTimes = 0;
        while (this.byteBufferRead.hasRemaining()) {
            try {
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize > 0) {
                    readSizeZeroTimes = 0;
                    this.dispatchReadRequest();
                } else if (readSize == 0) {
                    if (++readSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    // TODO ERROR
                    System.out.println("read socket < 0");
                    return false;
                }
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }


    private void dispatchReadRequest() {
        int writePosition = this.byteBufferRead.position();
        // 针对线程池优化
        final List<ByteBuffer> requestList = new LinkedList<ByteBuffer>();

        /**
         * 反复从byteBufferRead里读数据
         * 如果buff未读长度小于8放不下一个消息头，或者能放下消息头，但是放不下消息体，就退出循环
         * 每次都在这个buffer上做一个视图，圈定一个请求的消息体
         * 每个请求交给线程池搞定，如果没设置线程池，那就在调用线程里搞定
         */
        while (true) {
            int diff = this.byteBufferRead.position() - this.dispatchPosition;
            if (diff >= 8) {
                // msgSize不包含消息reqId
                int msgSize = this.byteBufferRead.getInt(this.dispatchPosition);
                final Integer reqId = this.byteBufferRead.getInt(this.dispatchPosition + 4);
                // 可以凑够一个请求
                if (diff >= (8 + msgSize)) {
                    //做一个request视图，position置成当前请求开始的位置，limit设置到请求结束
                    this.byteBufferRead.position(0);
                    final ByteBuffer request = this.byteBufferRead.slice();
                    request.position(this.dispatchPosition + 8);
                    request.limit(this.dispatchPosition + 8 + msgSize);
                    this.byteBufferRead.position(writePosition);
                    this.dispatchPosition += 8 + msgSize;//msgSize, reqId已经处理过了，更新位置

                    /**
                     * 有工作线程池就把工作放进去，要么在当前线程里做
                     */
                    if (this.executor != null) {
                        // if (this.executor.getActiveCount() >=
                        // (this.executor.getMaximumPoolSize() - 16)) {
                        // requestList.add(request);
                        // continue;
                        // }

                        try {
                            this.executor.execute(new Runnable() {
                                public void run() {
                                    try {
                                        byte[] response =
                                                Connection.this.rpcServerProcessor.process(reqId, request);
                                        if (response != null) {
                                            Connection.this.linkedByteBufferList.putData(reqId, response);
                                        }
                                    } catch (Throwable e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            requestList.add(request);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        byte[] response = Connection.this.rpcServerProcessor.process(reqId, request);
                        if (response != null) {
                            Connection.this.linkedByteBufferList.putData(reqId, response);
                        }
                    }

                    continue;
                }
                // 无法凑够一个请求
                else {
                    // ByteBuffer满了，分配新的内存
                    if (!this.byteBufferRead.hasRemaining()) {
                        this.reallocateByteBuffer();
                    }
                    break;
                }
            } else if (!this.byteBufferRead.hasRemaining()) {
                this.reallocateByteBuffer();
            }

            break;
        }

        // 一个线程内运行多个任务
        //集中清理while里放不进executor的请求
        //我猜是这样的。上面while里放不进去的任务才放进requestList，这里再用上面的executor，难保能成功
        //再者，executor是final的，即使构造的时候传了null，这里也没法改变
        ThreadPoolExecutor local = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);

        //感觉这里要么遍历requestList，如果任务放不进去，睡眠重试，直到所有的任务都放到executor里为止
        //或者，等executor有空余线程（构造时传入了执行器），等到线程后把requestList的处理任务交给它
        //原意因该是后一种
        for (boolean retry = true; retry; ) {
            try {
                if (!requestList.isEmpty()) {
                    local.execute(new Runnable() { //从this.executor改过来的
                        public void run() {
                            for (ByteBuffer request : requestList) {
                                try {
                                    //这个地方，依赖于从request放到list里到现在，都没被写过
                                    //request是视图，只标示位置，和byteBufferRead是共用内存的
                                    final int reqId = request.getInt(request.position() - 4);
                                    byte[] response = Connection.this.rpcServerProcessor.process(reqId, request);
                                    if (response != null) {
                                        Connection.this.linkedByteBufferList.putData(reqId, response);
                                    }
                                } catch (Throwable e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    });
                }

                retry = false;
            } catch (RejectedExecutionException e) {
                try {
                    //reject的处理方式，等于放弃这个请求
                    Thread.sleep(1);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    /**
     * 像channel的compact
     */
    private void reallocateByteBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(ReadMaxBufferSize);
        int remain = this.byteBufferRead.limit() - this.dispatchPosition;
        bb.put(this.byteBufferRead.array(), this.dispatchPosition, remain);
        this.dispatchPosition = 0;
        this.byteBufferRead = bb;
    }

//    private void reallocateByteBuffer() {
//        int remain = this.byteBufferRead.limit() - this.dispatchPosition;
//        if (remain > 0) {
//            byte[] remainData = new byte[remain];
//            this.byteBufferRead.position(this.dispatchPosition);
//            this.byteBufferRead.get(remainData);
//            this.byteBufferRead.position(0);
//            this.byteBufferRead.put(remainData, 0, remain);
//        }
//
//        this.byteBufferRead.position(remain);
//        this.byteBufferRead.limit(ReadMaxBufferSize);
//        this.dispatchPosition = 0;
//    }


    public void putRequest(final int reqId, final byte[] data) {
        this.linkedByteBufferList.putData(reqId, data);
    }


    public int getWriteByteBufferCnt() {
        return this.linkedByteBufferList.getNodeTotal();
    }


    public SocketChannel getSocketChannel() {
        return socketChannel;
    }


    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
