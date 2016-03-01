/**
 * $Id: ServiceThread.java 889 2012-12-29 05:49:42Z shijia.wxr $
 */
package com.taobao.simplerpc;

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
 * ��̨�����̻߳���
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public abstract class ServiceThread implements Runnable {
    // ִ���߳�
    protected final Thread thread;
    // �̻߳���ʱ�䣬Ĭ��90S
    private static final long JoinTime = 90 * 1000;
    // �Ƿ��Ѿ���Notify��
    protected volatile boolean hasNotified = false;
    // �߳��Ƿ��Ѿ�ֹͣ
    protected volatile boolean stoped = false;


    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }


    public abstract String getServiceName();


    /**
     *�߳̿�ʼִ��
     */
    public void start() {
        this.thread.start();
    }


    /**
     * ����interrupt��ֹ���ȴ�jointime��ô����ʱ��
     */
    public void shutdown() {
        this.shutdown(false);
    }


    public void stop() {
        this.stop(false);
    }


    public void makeStop() {
        this.stoped = true;
    }


    public void stop(final boolean interrupt) {
        this.stoped = true;
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                /**
                 * ���ѵȴ����߳�
                 */
                this.notify();
            }
        }
        /**
         *        �ر��߳�
         */
        if (interrupt) {
            this.thread.interrupt();
        }
    }


    public void shutdown(final boolean interrupt) {
        this.stoped = true;
        /**
         * ���ѵȴ������ServiceThread�߳��ϵ������̣߳�����һ��
         */
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }

        try {
            /**
             * �ǲ�����ֹ
             */
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            /**
             * �ȴ��߳̽���
             */
            this.thread.join(this.getJointime());
            long eclipseTime = System.currentTimeMillis() - beginTime;
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *    ���ѵȴ���this�ϵ������̣߳�����һ��
     */
    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }

    /**�������Ψһһ����ServiceThread��wait�ĵط�
     * ���Լ���֪ͨ����״̬���
     * �ȴ�intervalʱ��
     * �ڼ���ܱ������̵߳��øö����������������
     */
    protected void waitForRunning(long interval) {
        synchronized (this) {
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }

    /**
     *    �����Ѻ�ִ�еĹ���
     */
    protected void onWaitEnd() {
    }


    public boolean isStoped() {
        return stoped;
    }


    public long getJointime() {
        return JoinTime;
    }
}
