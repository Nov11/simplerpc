/**
 * $Id: Server.java 863 2012-12-24 07:08:25Z shijia.wxr $
 */
package com.taobao.simplerpc.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.taobao.simplerpc.DefaultRPCServer;
import com.taobao.simplerpc.RPCProcessor;
import com.taobao.simplerpc.RPCServer;

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
 * �򵥹��ܲ��ԣ�Server��
 *
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class Server {
    static class ServerRPCProcessor implements RPCProcessor {
        //���ô�������
        private final AtomicLong invokeTimesTotal = new AtomicLong(0);

        //upstreamת��byte[]�������أ�echo server
        //ע������getAndIncrement����������������ȥ��ע��
        public byte[] process(int upId, ByteBuffer upstream) {
            // String upstr =
            // new String(upstream.array(), upstream.position(),
            // upstream.limit() - upstream.position());
            // Long value = this.invokeTimesTotal.getAndIncrement();
            // //System.out.println("server process, receive [" + upstr + "], "
            // + value);
            // return value.toString().getBytes();

            int length = upstream.limit() - upstream.position();
            byte[] response = new byte[length];
            upstream.get(response);
            return response;
        }

        public AtomicLong getInvokeTimesTotal() {
            return invokeTimesTotal;
        }
    }


    public static void main(String[] args) {
        try {
            if (args.length > 2) {
                System.err.println("Useage: mtclient [listenPort] [threadCnt]");
                return;
            }

            // args
            int listenPort = args.length > 0 ? Integer.valueOf(args[0]) : 2012;
            int threadCnt = args.length > 1 ? Integer.valueOf(args[1]) : 256;

            //�ڲ�׼���ý����˿ڣ��̳߳أ�Ȼ��ע��һ���������������ʼ����
            RPCServer rpcServer = new DefaultRPCServer(listenPort, threadCnt, threadCnt);
            ServerRPCProcessor serverRPCProcessor = new ServerRPCProcessor();
            rpcServer.registerProcessor(serverRPCProcessor);
            rpcServer.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
