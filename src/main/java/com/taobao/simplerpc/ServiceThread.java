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
 * 后台服务线程基类
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public abstract class ServiceThread implements Runnable {
    // 执行线程
    protected final Thread thread;
    // 线程回收时间，默认90S
    private static final long JoinTime = 90 * 1000;
    // 是否已经被Notify过
    protected volatile boolean hasNotified = false;
    // 线程是否已经停止
    protected volatile boolean stoped = false;


    public ServiceThread() {
        this.thread = new Thread(this, this.getServiceName());
    }


    public abstract String getServiceName();


    /**
     *线程开始执行
     */
    public void start() {
        this.thread.start();
    }


    /**
     * 不用interrupt终止，等待jointime这么长的时间
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
                 * 唤醒等待的线程
                 */
                this.notify();
            }
        }
        /**
         *        关闭线程
         */
        if (interrupt) {
            this.thread.interrupt();
        }
    }


    public void shutdown(final boolean interrupt) {
        this.stoped = true;
        /**
         * 唤醒等待在这个ServiceThread线程上的其他线程，唤醒一个
         */
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }

        try {
            /**
             * 是不是终止
             */
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            /**
             * 等待线程结束
             */
            this.thread.join(this.getJointime());
            long eclipseTime = System.currentTimeMillis() - beginTime;
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *    唤醒等待在this上的其他线程，唤醒一个
     */
    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }

    /**这好像是唯一一个在ServiceThread上wait的地方
     * 把自己被通知过的状态清掉
     * 等待interval时间
     * 期间可能被其他线程调用该对象的其他方法唤醒
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
     *    被唤醒后执行的钩子
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
