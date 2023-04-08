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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 故障转移策略（默认是没有开启的）
 * <li>broker不可用的时间就是根据latencyMax判断的，大于哪个下标，然后就从notAvailableDuration取不可用时间。</li>
 * <li>对于延迟时间为550L毫秒以下的，不可用时间都是0</li>
 */
public class MQFaultStrategy {
    private final static Logger log = LoggerFactory.getLogger(MQFaultStrategy.class);
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    /**
     * 最大延迟时间
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    /**
     * 不可用时间
     */
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            try {
                //遍历topic路由信息中所有的messageQueue，采用轮训的方式，可用就返回
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = index++ % tpInfo.getMessageQueueList().size();
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //判断broker是否可用,这个是本地updateFaultItem设置的时间
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }
                /*
                 *到这里说明，说明所有的broker都不可用，接下来：
                 * 1.选择其中broker（pickOneAtLeast方法中会选取相对可用的broker）
                 * 2.然后从broker获取当前topic写队列数据数量
                 * 3.再轮训负载，选一个messageQueue
                 */
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        //防止不同broker的writeQueue不同，就对QueueId重新负载
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    //说明这个broker中，该topic根本不能写数据，就从latencyFaultTolerance中移除这个broker,
                    // 一定时刻pickOneAtLeast就不会有他了，但是
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            /*
             *  上边两个步骤，还没有得到合适的broker,只能在所有的messageQueue中轮训选择了，造成的情况：
             * 1.pickOneAtLeast的broker中，该topic中writeQueueNums<=0
             * 2.上述故障转移选择messageQueue的过程中出现异常，直接用selectOneMessageQueue兜底
             */
            return tpInfo.selectOneMessageQueue();
        }
        //没有开启故障转移，就直接轮训选择一个
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新故障项
     * <li>如果开启了故障转移，在当前类selectOneMessageQueue方法中，就会判断当前messageQueue可用</li>
     *
     * @param brokerName     broker名称
     * @param currentLatency 当前延迟时间
     * @param isolation      是否隔离（true的时候，计算延迟时间为30秒，也就说明broker30秒不可用）
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        //发送延迟故障启用(默认为false)
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //更新故障项
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算不可用持续时间：据currentLatency判断在latencyMax哪个时间段，从而从notAvailableDuration得到不可用时间
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
