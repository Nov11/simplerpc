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
 * 针对写优化的ByteBuffer序列
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class LinkedByteBufferList {
    class ByteBufferNode {
        public static final int NODE_SIZE = 1024 * 1024 * 4;
        private final AtomicInteger writeOffset = new AtomicInteger(0);
        private ByteBuffer byteBufferWrite;
        private ByteBuffer byteBufferRead;//这个read buf 是write上的视图，两者共用一个byte[]
        private volatile ByteBufferNode nextByteBufferNode;

        /**
         * 设置position和limit，没有改变读写状态
         * 跟clear有点像
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
         * 建立在java 数组上的buffer
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

    // 是否已经被Notify过
    // 这个putdata和找可读的node通过notify同步
    // 每次putdata结束hasNotified置true 每次获取readNode都置false
    protected volatile boolean hasNotified = false;

    public LinkedByteBufferList() {
        this.currentWriteNode = new ByteBufferNode();
        this.currentReadNode = this.currentWriteNode;
    }

    // TODO 可能需要流控

    /**
     * 先写header 再写body，如果不够就再分配一个buffer，把没写完了的写进去
     * 保持head在前，body紧接在后的布局
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
            // 尝试写入头
            if (minHeader > 0) {
                this.currentWriteNode.getByteBufferWrite().put(header.array(), 0, minHeader);
                this.currentWriteNode.getWriteOffset().addAndGet(minHeader);
            }
            // 尝试写入体
            if (minHeader == HEADER_SIZE) {//消息头全部写完了
                minData = Math.min(data.length, this.currentWriteNode.getByteBufferWrite().remaining());
                if (minData > 0) {
                    this.currentWriteNode.getByteBufferWrite().put(data, 0, minData);
                    this.currentWriteNode.getWriteOffset().addAndGet(minData);
                }
            }

            // 需要创建新的Buffer
            if (!this.currentWriteNode.getByteBufferWrite().hasRemaining()) {
                ByteBufferNode newNode = null;
                // 尝试从空闲处取
                newNode = this.bbnIdleList.poll();
                if (null == newNode) {
                    newNode = new ByteBufferNode();
                }

                this.currentWriteNode.setNextByteBufferNode(newNode.clearAndReturnNew());
                this.currentWriteNode = newNode;

                // 补偿Header
                // 这里writeOffSet没更新，从上一个node里可读的长度累加的
                int remainHeaderPut = HEADER_SIZE - minHeader;
                int remainDataPut = data.length - minData;
                if (remainHeaderPut > 0) {
                    this.currentWriteNode.getByteBufferWrite().put(header.array(), minHeader, remainHeaderPut);
                    this.currentWriteNode.getWriteOffset().addAndGet(remainHeaderPut);
                }

                // 补偿Data
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
     * 尽量找个有内容可读的node
     * 我觉得需要加个锁
     * @return
     * 当前node可读则返回它
     * 下一个node可读，则把当前node放到空闲链表，置下个node为当前node，并返回它
     * 否则返回null
     */
    public ByteBufferNode findReadableNode() {
        if (this.getCurrentReadNode().isReadable()) {
            return this.getCurrentReadNode();
        }

        if (this.getCurrentReadNode().isReadOver()) {
            if (this.getCurrentReadNode().getNextByteBufferNode() != null) {
                this.bbnIdleList.add(this.getCurrentReadNode());                        //这个地方两个线程可能会加两次
                this.setCurrentReadNode(this.getCurrentReadNode().getNextByteBufferNode());
                return this.getCurrentReadNode();
            }
        }

        return null;
    }

    /**
     * 找不到readNode就等一会儿，实在等不到就返回null
     * 去掉hasNotified，应该能实现一样的效果
     * 例如找不到就wait，超时或者被唤醒了再找一次，返回结果
     * @param interval 等待的时长
     * @return
     */
    public ByteBufferNode waitForPut(long interval) {
        //findReadableNode是不加锁的。可能put在进行中，这个node就被返回了。
        ByteBufferNode found = this.findReadableNode();
        if (found != null) {
            return found;
        }

        synchronized (this) {
            if (this.hasNotified) {
                //进入这个if的应该是putdata结束，第一个获得this锁的线程。
                //只有putdata会置hasNotified为true，这里置false相当于标记notify被消耗了
                this.hasNotified = false;
                found = this.findReadableNode();
                if (found != null) {                      //这个地方不会是null的，否则外层if里hasNotified是false，那就不会运行到这行代码
                    return found;
                }
            }

            try {
                //等notify一段时间
                //被唤醒或者等待时间导了，再找一次readNode试试
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
