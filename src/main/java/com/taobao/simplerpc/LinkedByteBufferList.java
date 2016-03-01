/**
 * $Id: LinkedByteBufferList.java 877 2012-12-24 09:35:06Z shijia.wxr $
 */
package com.taobao.simplerpc;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;
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
 * ���д�Ż���ByteBuffer����
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class LinkedByteBufferList {
    class ByteBufferNode {
        public static final int NODE_SIZE = 1024 * 1024 * 4;
        private final AtomicInteger writeOffset = new AtomicInteger(0);
        private ByteBuffer byteBufferWrite;
        private ByteBuffer byteBufferRead;//���read buf ��write�ϵ���ͼ�����߹���һ��byte[]
        private volatile ByteBufferNode nextByteBufferNode;

        /**
         * ����position��limit��û�иı��д״̬
         * ��clear�е���
         * @return
         */
        public ByteBufferNode clearAndReturnNew() {
            this.writeOffset.set(0);
            this.byteBufferWrite.position(0);
            this.byteBufferWrite.limit(NODE_SIZE);
            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(NODE_SIZE);
            this.nextByteBufferNode = null;
            return this;
        }

        public boolean isReadable() {
            return this.byteBufferRead.position() < this.writeOffset.get();
        }

        public boolean isReadOver() {
            return this.byteBufferRead.position() == NODE_SIZE;
        }

        /**
         * ������java �����ϵ�buffer
         * @param size
         * @return
         */
        private ByteBuffer allocateNewByteBuffer(final int size) {
            return ByteBuffer.allocate(size);
        }

        public ByteBufferNode() {
            LinkedByteBufferList.this.nodeTotal++;
            this.nextByteBufferNode = null;
            this.byteBufferWrite = allocateNewByteBuffer(NODE_SIZE);
            this.byteBufferRead = this.byteBufferWrite.slice();
        }

        public ByteBuffer getByteBufferWrite() {
            return byteBufferWrite;
        }

        public void setByteBufferWrite(ByteBuffer byteBufferWrite) {
            this.byteBufferWrite = byteBufferWrite;
        }

        public ByteBuffer getByteBufferRead() {
            return byteBufferRead;
        }

        public void setByteBufferRead(ByteBuffer byteBufferRead) {
            this.byteBufferRead = byteBufferRead;
        }

        public ByteBufferNode getNextByteBufferNode() {
            return nextByteBufferNode;
        }

        public void setNextByteBufferNode(ByteBufferNode nextByteBufferNode) {
            this.nextByteBufferNode = nextByteBufferNode;
        }

        public AtomicInteger getWriteOffset() {
            return writeOffset;
        }
    }

    private volatile int nodeTotal = 0;
    private ByteBufferNode currentWriteNode;
    private ByteBufferNode currentReadNode;
    private final LinkedBlockingDeque<ByteBufferNode> bbnIdleList =
            new LinkedBlockingDeque<LinkedByteBufferList.ByteBufferNode>();

    // �Ƿ��Ѿ���Notify��
    // ���putdata���ҿɶ���nodeͨ��notifyͬ��
    // ÿ��putdata����hasNotified��true ÿ�λ�ȡreadNode����false
    protected volatile boolean hasNotified = false;

    public LinkedByteBufferList() {
        this.currentWriteNode = new ByteBufferNode();
        this.currentReadNode = this.currentWriteNode;
    }

    // TODO ������Ҫ����

    /**
     * ��дheader ��дbody������������ٷ���һ��buffer����ûд���˵�д��ȥ
     * ����head��ǰ��body�����ں�Ĳ���
     * @param reqId
     * @param data
     */
    public void putData(final int reqId, final byte[] data) {
        final int HEADER_SIZE = 8;
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putInt(data.length);
        header.putInt(reqId);
        header.flip();
        synchronized (this) {
            int minHeader = Math.min(HEADER_SIZE, this.currentWriteNode.getByteBufferWrite().remaining());
            int minData = 0;
            // ����д��ͷ
            if (minHeader > 0) {
                this.currentWriteNode.getByteBufferWrite().put(header.array(), 0, minHeader);
                this.currentWriteNode.getWriteOffset().addAndGet(minHeader);
            }
            // ����д����
            if (minHeader == HEADER_SIZE) {//��Ϣͷȫ��д����
                minData = Math.min(data.length, this.currentWriteNode.getByteBufferWrite().remaining());
                if (minData > 0) {
                    this.currentWriteNode.getByteBufferWrite().put(data, 0, minData);
                    this.currentWriteNode.getWriteOffset().addAndGet(minData);
                }
            }

            // ��Ҫ�����µ�Buffer
            if (!this.currentWriteNode.getByteBufferWrite().hasRemaining()) {
                ByteBufferNode newNode = null;
                // ���Դӿ��д�ȡ
                newNode = this.bbnIdleList.poll();
                if (null == newNode) {
                    newNode = new ByteBufferNode();
                }

                this.currentWriteNode.setNextByteBufferNode(newNode.clearAndReturnNew());
                this.currentWriteNode = newNode;

                // ����Header
                // ����writeOffSetû���£�����һ��node��ɶ��ĳ����ۼӵ�
                int remainHeaderPut = HEADER_SIZE - minHeader;
                int remainDataPut = data.length - minData;
                if (remainHeaderPut > 0) {
                    this.currentWriteNode.getByteBufferWrite().put(header.array(), minHeader, remainHeaderPut);
                    this.currentWriteNode.getWriteOffset().addAndGet(remainHeaderPut);
                }

                // ����Data
                if (remainDataPut > 0) {
                    this.currentWriteNode.getByteBufferWrite().put(data, minData, remainDataPut);
                    this.currentWriteNode.getWriteOffset().addAndGet(remainDataPut);
                }
            }

            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }

    /**
     * �����Ҹ������ݿɶ���node
     * �Ҿ�����Ҫ�Ӹ���
     * @return
     * ��ǰnode�ɶ��򷵻���
     * ��һ��node�ɶ�����ѵ�ǰnode�ŵ������������¸�nodeΪ��ǰnode����������
     * ���򷵻�null
     */
    public ByteBufferNode findReadableNode() {
        if (this.getCurrentReadNode().isReadable()) {
            return this.getCurrentReadNode();
        }

        if (this.getCurrentReadNode().isReadOver()) {
            if (this.getCurrentReadNode().getNextByteBufferNode() != null) {
                this.bbnIdleList.add(this.getCurrentReadNode());                        //����ط������߳̿��ܻ������
                this.setCurrentReadNode(this.getCurrentReadNode().getNextByteBufferNode());
                return this.getCurrentReadNode();
            }
        }

        return null;
    }

    /**
     * �Ҳ���readNode�͵�һ�����ʵ�ڵȲ����ͷ���null
     * ȥ��hasNotified��Ӧ����ʵ��һ����Ч��
     * �����Ҳ�����wait����ʱ���߱�����������һ�Σ����ؽ��
     * @param interval �ȴ���ʱ��
     * @return
     */
    public ByteBufferNode waitForPut(long interval) {
        //findReadableNode�ǲ������ġ�����put�ڽ����У����node�ͱ������ˡ�
        ByteBufferNode found = this.findReadableNode();
        if (found != null) {
            return found;
        }

        synchronized (this) {
            if (this.hasNotified) {
                //�������if��Ӧ����putdata��������һ�����this�����̡߳�
                //ֻ��putdata����hasNotifiedΪtrue��������false�൱�ڱ��notify��������
                this.hasNotified = false;
                found = this.findReadableNode();
                if (found != null) {                      //����ط�������null�ģ��������if��hasNotified��false���ǾͲ������е����д���
                    return found;
                }
            }

            try {
                //��notifyһ��ʱ��
                //�����ѻ��ߵȴ�ʱ�䵼�ˣ�����һ��readNode����
                this.wait(interval);
                return this.findReadableNode();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                this.hasNotified = false;
            }
        }

        return null;
    }


    public ByteBufferNode getCurrentReadNode() {
        return currentReadNode;
    }


    public void setCurrentReadNode(ByteBufferNode currentReadNode) {
        this.currentReadNode = currentReadNode;
    }


    public LinkedBlockingDeque<ByteBufferNode> getBbnIdleList() {
        return bbnIdleList;
    }


    public int getNodeTotal() {
        return nodeTotal;
    }
}
