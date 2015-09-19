package com.raycom.communicate.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.raycom.comm.proto.service.ServiceCommunicate;
import com.raycom.communicate.service.com.raycom.communicate.service.IBaseServiceCommunicateBusinessHandlerFactory;
import com.raycom.jmsBaseV2.JMSClient;
import com.raycom.util.codec.HexCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JMS消息处理中心，用于发送消息，以及接收JMS消息的入口
 * Created by dingjinlin on 2015/9/1.
 */
public class JMSMessageCenter {
    private static final Logger LOG = LoggerFactory.getLogger(JMSMessageCenter.class);

    private JMSClient jmsClient;
    int businessSessionTimeOut;
    int processorPoolSize;
    private BusinessJMSSenderAndReceiverProcessor businessJMSProcessor;
    private Map<String, BusinessJMSReceiverProcessor> receiverJMSProcessorCache;
    public JMSMessageCenter(String url, String user, String password, int processorPoolSize, int replayTimeout) {
        // TODO: 去除JMSBase中的定时清除生产者MAP,加入手功清除函数
        jmsClient = JMSClient.getInstance(url, user, password, 10000);
        jmsClient.start();
        this.processorPoolSize = processorPoolSize;
        businessSessionTimeOut = replayTimeout;

        businessJMSProcessor = new BusinessJMSSenderAndReceiverProcessor(this, businessSessionTimeOut);
        receiverJMSProcessorCache = new ConcurrentHashMap<String, BusinessJMSReceiverProcessor>();
    }

    public boolean start() {
        jmsClient.start();
        return true;
    }

    public void addReceiver(String receiverQueueName, IBaseServiceCommunicateBusinessHandlerFactory handlerFactory, int timeout) throws JMSException {
        BusinessJMSReceiverProcessor processor = new BusinessJMSReceiverProcessor(this, handlerFactory, timeout);
        receiverJMSProcessorCache.put(receiverQueueName, processor);
        jmsClient.addQueueProcessor(receiverQueueName, processor, processorPoolSize);
    }

    public void removeReceiver(String receiverQueueName) throws JMSException {
        receiverJMSProcessorCache.remove(receiverQueueName);
        jmsClient.closeConsumer(receiverQueueName);
    }

    public void sentMessage(String destQueueName, byte[] msg) throws JMSException {
        jmsClient.sendData(destQueueName, msg);
        LOG.trace("JMS sent data- destAddress: " + destQueueName + ", data: " + HexCodec.byte2String(msg));
    }

    public void sentAndReceive(String receiverQueueName, BaseServiceCommunicateBusinessHandler handler, String destQueueName, byte[] msg) throws JMSException {
        businessJMSProcessor.addBusinessHandler(new String(handler.getBusinessID()), handler);
        jmsClient.addQueueProcessor(receiverQueueName, businessJMSProcessor, processorPoolSize);

        jmsClient.sendData(destQueueName, msg);
        LOG.trace("JMS sent data- destAddress: " + destQueueName + ", responseAddress:" + receiverQueueName + ", data: " + HexCodec.byte2String(msg));
    }

    class BusinessJMSReceiverProcessor extends BaseJMSProcessor<byte[]> {
        IBaseServiceCommunicateBusinessHandlerFactory handlerFactory;
        public BusinessJMSReceiverProcessor(JMSMessageCenter jmsMessageCenter, IBaseServiceCommunicateBusinessHandlerFactory handlerFactory, int timeout) {
            super(jmsMessageCenter, timeout);
            this.handlerFactory = handlerFactory;
        }

        @Override
        public void process(byte[] message) {
            try {
                ServiceCommunicate.ServiceCommunicateMsg communicateMsg = ServiceCommunicate.ServiceCommunicateMsg.parseFrom(message);
                String name = communicateMsg.getName();
                String responseAddress = communicateMsg.getResponseAddress();
                ServiceCommunicate.ServiceCommunicateMsg.MessageType msgType = communicateMsg.getMessageType();
                String log = "received from " + name + ",s " + msgType.name() + " message, ";
                String businessID = new String(communicateMsg.getBusinessID().toByteArray());
                int messageID = communicateMsg.getMessageID();
                String handleKey = businessID + messageID;
                BaseServiceCommunicateBusinessHandler businessHandler = businessHandlerCache.get(handleKey);
                if(businessHandler == null) {
                    businessHandler = handlerFactory.createHandler();
                    businessHandler.setJmsMessageCenter(jmsMessageCenter);
                    businessHandlerCache.put(handleKey, businessHandler);
                }
                switch (communicateMsg.getMessageType()) {
                    case NOTIFICATION: {
                        businessHandler.onNotification(communicateMsg.getName(), communicateMsg);
                        break;
                    }
                    case REQUEST: {
                        businessHandler.onRequest(communicateMsg.getName(), communicateMsg);
                        break;
                    }
                    case SUBMIT_REQUEST: {
                        LOG.info(log + "response address: " + responseAddress);
                        businessHandler.onSubmitRequest(communicateMsg.getName(), communicateMsg);
                        break;
                    }
                    case CANCEL_REQUEST: {
                        businessHandler.onCancelRequest(communicateMsg.getName(), communicateMsg);
                        break;
                    }
                    case CONFIRM_REQUEST: {
                        businessHandler.onConfirmRequest(communicateMsg.getName(), communicateMsg);
                        break;
                    }
                    default:
                        LOG.info("Message type not support, messageType: " + msgType.name());
                        break;
                }
            } catch (InvalidProtocolBufferException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    class BusinessJMSSenderAndReceiverProcessor extends BaseJMSProcessor<byte[]> {
        public BusinessJMSSenderAndReceiverProcessor(JMSMessageCenter jmsMessageCenter, int timeout) {
            super(jmsMessageCenter, timeout);
        }

        public void addBusinessHandler(String businessId, BaseServiceCommunicateBusinessHandler handler) {
            businessHandlerCache.put(businessId, handler);
        }

        @Override
        public void process(byte[] message) {
            try {
                ServiceCommunicate.ServiceCommunicateMsg communicateMsg = ServiceCommunicate.ServiceCommunicateMsg.parseFrom(message);
                String name = communicateMsg.getName();
                String responseAddress = communicateMsg.getResponseAddress();
                ServiceCommunicate.ServiceCommunicateMsg.MessageType msgType = communicateMsg.getMessageType();
                String businessID = new String(communicateMsg.getBusinessID().toByteArray());
                String log = "received from " + name + ",s " + msgType.name() + " message, response address: " + responseAddress;
                LOG.info(log);
                BaseServiceCommunicateBusinessHandler businessHandler = businessHandlerCache.get(businessID);
                if (businessHandler != null) {
                    switch (msgType) {
                        case RESPONSE: {
                            businessHandler.onResponse(communicateMsg.getName(), communicateMsg);
                            break;
                        }
                        case SUBMIT_RESPONSE: {
                            businessHandler.onSubmitResponse(communicateMsg.getName(), communicateMsg);
                            break;
                        }
                        case CANCEL_RESPONSE: {
                            businessHandler.onCancelResponse(communicateMsg.getName(), communicateMsg);
                            break;
                        }
                        case CONFIRM_RESPONSE: {
                            businessHandler.onConfirmResponse(communicateMsg.getName(), communicateMsg);
                            break;
                        }
                        default:
                            LOG.info("Message type not support, messageType: " + msgType.name());
                            break;
                    }
                } else {
                    LOG.info("Not found business request Handler by business id: " + businessID);
                }


            } catch (InvalidProtocolBufferException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}
