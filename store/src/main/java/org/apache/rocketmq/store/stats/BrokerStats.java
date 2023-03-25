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
package org.apache.rocketmq.store.stats;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageStore;

/**
 * broker状态记录
 * <li>每天凌晨0点执行(也就是12点执行），记录累计到前天、昨天的生产以及消费消息的总数。</li>
 */
public class BrokerStats {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final MessageStore defaultMessageStore;

    /**
     * 累计到昨天早上put消息数量
     */
    private volatile long msgPutTotalYesterdayMorning;

    /**
     * 累计到今天早上消息put的数量
     */
    private volatile long msgPutTotalTodayMorning;

    /**
     * 累计到昨天早上get消息数量（也就是到前天晚上累计get消息的总数量）
     */
    private volatile long msgGetTotalYesterdayMorning;

    /**
     * 累计到今天早上消息put的数量(也就是到昨晚累计get消息的数量）
     */
    private volatile long msgGetTotalTodayMorning;

    public BrokerStats(MessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public void record() {
        //12点执行，新的一天，累计到前天的数据=昨天累计的数据
        this.msgPutTotalYesterdayMorning = this.msgPutTotalTodayMorning;
        this.msgGetTotalYesterdayMorning = this.msgGetTotalTodayMorning;

        this.msgPutTotalTodayMorning =
            this.defaultMessageStore.getBrokerStatsManager().getBrokerPutNumsWithoutSystemTopic();
        this.msgGetTotalTodayMorning =
            this.defaultMessageStore.getBrokerStatsManager().getBrokerGetNumsWithoutSystemTopic();

        log.info("yesterday put message total: {}", msgPutTotalTodayMorning - msgPutTotalYesterdayMorning);
        log.info("yesterday get message total: {}", msgGetTotalTodayMorning - msgGetTotalYesterdayMorning);
    }

    public long getMsgPutTotalYesterdayMorning() {
        return msgPutTotalYesterdayMorning;
    }

    public void setMsgPutTotalYesterdayMorning(long msgPutTotalYesterdayMorning) {
        this.msgPutTotalYesterdayMorning = msgPutTotalYesterdayMorning;
    }

    public long getMsgPutTotalTodayMorning() {
        return msgPutTotalTodayMorning;
    }

    public void setMsgPutTotalTodayMorning(long msgPutTotalTodayMorning) {
        this.msgPutTotalTodayMorning = msgPutTotalTodayMorning;
    }

    public long getMsgGetTotalYesterdayMorning() {
        return msgGetTotalYesterdayMorning;
    }

    public void setMsgGetTotalYesterdayMorning(long msgGetTotalYesterdayMorning) {
        this.msgGetTotalYesterdayMorning = msgGetTotalYesterdayMorning;
    }

    public long getMsgGetTotalTodayMorning() {
        return msgGetTotalTodayMorning;
    }

    public void setMsgGetTotalTodayMorning(long msgGetTotalTodayMorning) {
        this.msgGetTotalTodayMorning = msgGetTotalTodayMorning;
    }

    public long getMsgPutTotalTodayNow() {
        return this.defaultMessageStore.getBrokerStatsManager().getBrokerPutNumsWithoutSystemTopic();
    }

    public long getMsgGetTotalTodayNow() {
        return this.defaultMessageStore.getBrokerStatsManager().getBrokerGetNumsWithoutSystemTopic();
    }
}
