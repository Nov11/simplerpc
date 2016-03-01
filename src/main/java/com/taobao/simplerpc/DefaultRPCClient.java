/**
 * $Id: DefaultRPCClient.java 875 2012-12-24 09:30:59Z shijia.wxr $
 */
package com.taobao.simplerpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
 * 客户端实现
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class DefaultRPCClient implements RPCClient {
    // private Connection connection;
    private List<Connection> connectionList = new ArrayList<Connection>();
    //应该是为了给多线程调用准备的
    private final AtomicInteger requestId = new AtomicInteger(0);

    //key是request id， value是responseId == requestId的 CallResponse 对象
    private final ConcurrentHashMap<Integer, CallResponse> callRepTable =
            new ConcurrentHashMap<Integer, CallResponse>(1000000);

    private final ClientRPCProcessor clientRPCProcessor = new ClientRPCProcessor();

    /**
     * 处理server返回给client的响应
     */
    class ClientRPCProcessor implements RPCProcessor {
        /**
         * 找出响应体，把server传回的东西填进去
         * latch上countdown，让阻塞的rpc发起线程继续
         * @param repId
         * @param response
         * @return
         */
        public byte[] process(int repId, ByteBuffer response) {
            CallResponse cr = DefaultRPCClient.this.callRepTable.get(repId);
            if (cr != null) {
                cr.setResponseId(repId);
                cr.setResponseBody(response);
                cr.getCountDownLatch().countDown();
            }
            return null;
        }
    }

    class CallResponse {
        private int responseId;
        private ByteBuffer responseBody;
        //用来同步的
        //rpc发起者在这个变量上等待 变成0则读取服务端的响应 否则超时
        //收到响应后，countdown操作在RPCProcessor里
        private CountDownLatch countDownLatch = new CountDownLatch(1);


        public int getResponseId() {
            return responseId;
        }


        public void setResponseId(int responseId) {
            this.responseId = responseId;
        }


        public ByteBuffer getResponseBody() {
            return responseBody;
        }


        public void setResponseBody(ByteBuffer responseBody) {
            this.responseBody = responseBody;
        }


        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }


        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }


        public CallResponse(int reponseId) {
            this.responseId = reponseId;
        }
    }


    public DefaultRPCClient() {

    }

    /**
     * 开启链接对应的工作线程
     */
    public void start() {
        for (Connection c : this.connectionList) {
            c.start();
        }
    }

    /**
     * 依次关闭链接
     */
    public void shutdown() {
        for (Connection c : this.connectionList) {
            c.shutdown();
        }
    }

    /**
     * 从链接池里找个出来用
     * @param id
     * @return
     */
    private Connection findConnection(int id) {
        int pos = Math.abs(id) % this.connectionList.size();
        return this.connectionList.get(pos);
    }

    /**
     * 生成client类的request编号
     * 从client的角度看，callresponse叫request更贴切
     * id做key，把request放到callRepTable，这里是要向server发的请求
     * 从connectionList里面拿个链接出来，把request放到连接的发送缓冲里
     * 等5000毫秒，期望这段时间内countdownlatch变成0
     * 返回true则表示收到回应，返回responsebody
     * 5000毫秒以后移出当前请求，call里不做重试
     */
    public ByteBuffer call(byte[] request) throws InterruptedException {
        int id = this.requestId.incrementAndGet();
        CallResponse response = new CallResponse(id);
        this.callRepTable.put(id, response);
        this.findConnection(id).putRequest(id, request);
        boolean waitOK = response.getCountDownLatch().await(5000, TimeUnit.MILLISECONDS);
        ByteBuffer result = null;
        if (waitOK) {
            result = response.getResponseBody();
        }
        else {
            System.out.println("timeout, reqId = " + id);
        }

        this.callRepTable.remove(id);
        return result;
    }


    public boolean connect(InetSocketAddress remote) {
        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(true);
            /**
             * 不配置linger 主动关闭会进入time_wait
             */
            sc.socket().setSoLinger(false, -1);
            /**
             * 关nagle's算法 不会等
             */
            sc.socket().setTcpNoDelay(true);

            boolean connected = sc.connect(remote);
            if (connected) {
                sc.configureBlocking(false);
                Connection c = new Connection(sc, this.clientRPCProcessor, null);
                this.connectionList.add(c);
            }
            else {
                /**
                 * 阻塞connect应该不会返回false，失败抛异常
                 */
                sc.close();
            }

            return connected;
        }
        catch (IOException e) {
            if (sc != null) {
                try {
                    sc.close();
                }
                catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        return false;
    }


    /**
     * 可能是带重试的connect
     * 针对非阻塞channel准备的吧
     * 个人认为返回值写成 return i<cnt更好
     * 目前cnt应该传入1
     * @param remote
     * @param cnt
     * @return
     */
    public boolean connect(InetSocketAddress remote, int cnt) {
        int i;

        for (i = 0; i < cnt && this.connect(remote); i++) {
        }

        return i == cnt;
    }
}
