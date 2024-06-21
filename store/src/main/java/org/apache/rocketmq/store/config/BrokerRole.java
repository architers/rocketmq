/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.config;

public enum BrokerRole {
    /**
     * 指客户端发送消息到Master，Master将消息同步复制到Slave
     */
    ASYNC_MASTER,
    /**
     * 指客户端发送消息到Master，再由异步线程HAService异步同步到Slvae的过程
     */
    SYNC_MASTER,
    /**
     * 从节点
     */
    SLAVE;
}
