/**
 * $Id: RPCProcessor.java 839 2012-12-22 04:55:58Z shijia.wxr $
 */
package com.taobao.simplerpc;

import java.nio.ByteBuffer;

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
 * Server与Client的读事件处理
 * 
 * @author vintage.wang@gmail.com  shijia.wxr@taobao.com
 */
public interface RPCProcessor {
    /**
     *    这个是处理请求的
     */
    byte[] process(final int upId, final ByteBuffer upstream);
}
