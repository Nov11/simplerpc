/**
 * $Id: Client.java 864 2012-12-24 07:21:58Z shijia.wxr $
 */
package com.taobao.simplerpc.benchmark;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.taobao.simplerpc.DefaultRPCClient;
import com.taobao.simplerpc.RPCClient;

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
 */

/**
 * 529079634@qq.com made comments on the source code
 */

/**
 * 简单功能测试，Client端
 *
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class Client {
    public static void main(String[] args) {
        RPCClient rpcClient = new DefaultRPCClient();
        boolean connectOK = rpcClient.connect(new InetSocketAddress("127.0.0.1", 2012), 1);
        System.out.println("connect server " + (connectOK ? "OK" : "Failed"));
        rpcClient.start();//这个开启connect的处理线程

        for (long i = 0; ; i++) {
            try {
                String reqstr = "nice" + i;
                ByteBuffer repdata = rpcClient.call(reqstr.getBytes());//call把返回结果拆出来
                if (repdata != null) {
                    String repstr =
                            new String(repdata.array(), repdata.position(), repdata.limit() - repdata.position());
                    System.out.println("call result, " + repstr);
                } else {//server返回null的时候退出
                    return;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
