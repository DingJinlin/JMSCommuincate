package com.raycom.communicate.service;

import backtype.storm.utils.RotatingMap;
import com.raycom.jmsBaseV2.IJMSListenerProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ding.jinlin on 14-12-6.
 */
public abstract class BaseJMSProcessor<T> implements IJMSListenerProcessor<T> {
    static final Logger LOG = LoggerFactory.getLogger(BaseJMSProcessor.class);

    protected JMSMessageCenter jmsMessageCenter;
    protected RotatingMap<String, BaseServiceCommunicateBusinessHandler> businessHandlerCache;

    public BaseJMSProcessor(JMSMessageCenter jmsMessageCenter, int replayTimeout) {
        this.jmsMessageCenter = jmsMessageCenter;
        businessHandlerCache = new RotatingMap<String, BaseServiceCommunicateBusinessHandler>(replayTimeout);
    }

    public void setJmsMessageCenter(JMSMessageCenter jmsMessageCenter) {
        this.jmsMessageCenter = jmsMessageCenter;
    }
}
