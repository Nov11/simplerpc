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
 * �ͻ���ʵ��
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class DefaultRPCClient implements RPCClient {
    // private Connection connection;
    private List<Connection> connectionList = new ArrayList<Connection>();
    //Ӧ����Ϊ�˸����̵߳���׼����
    private final AtomicInteger requestId = new AtomicInteger(0);

    //key��request id�� value��responseId == requestId�� CallResponse ����
    private final ConcurrentHashMap<Integer, CallResponse> callRepTable =
            new ConcurrentHashMap<Integer, CallResponse>(1000000);

    private final ClientRPCProcessor clientRPCProcessor = new ClientRPCProcessor();

    /**
     * ����server���ظ�client����Ӧ
     */
    class ClientRPCProcessor implements RPCProcessor {
        /**
         * �ҳ���Ӧ�壬��server���صĶ������ȥ
         * latch��countdown����������rpc�����̼߳���
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
        //����ͬ����
        //rpc����������������ϵȴ� ���0���ȡ����˵���Ӧ ����ʱ
        //�յ���Ӧ��countdown������RPCProcessor��
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
     * �������Ӷ�Ӧ�Ĺ����߳�
     */
    public void start() {
        for (Connection c : this.connectionList) {
            c.start();
        }
    }

    /**
     * ���ιر�����
     */
    public void shutdown() {
        for (Connection c : this.connectionList) {
            c.shutdown();
        }
    }

    /**
     * �����ӳ����Ҹ�������
     * @param id
     * @return
     */
    private Connection findConnection(int id) {
        int pos = Math.abs(id) % this.connectionList.size();
        return this.connectionList.get(pos);
    }

    /**
     * ����client���request���
     * ��client�ĽǶȿ���callresponse��request������
     * id��key����request�ŵ�callRepTable��������Ҫ��server��������
     * ��connectionList�����ø����ӳ�������request�ŵ����ӵķ��ͻ�����
     * ��5000���룬�������ʱ����countdownlatch���0
     * ����true���ʾ�յ���Ӧ������responsebody
     * 5000�����Ժ��Ƴ���ǰ����call�ﲻ������
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
             * ������linger �����رջ����time_wait
             */
            sc.socket().setSoLinger(false, -1);
            /**
             * ��nagle's�㷨 �����
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
                 * ����connectӦ�ò��᷵��false��ʧ�����쳣
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
     * �����Ǵ����Ե�connect
     * ��Է�����channel׼���İ�
     * ������Ϊ����ֵд�� return i<cnt����
     * ĿǰcntӦ�ô���1
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
