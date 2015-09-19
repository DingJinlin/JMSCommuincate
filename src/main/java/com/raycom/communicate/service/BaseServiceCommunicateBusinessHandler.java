package com.raycom.communicate.service;

import com.google.protobuf.ByteString;
import com.raycom.comm.proto.service.ServiceCommunicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dingjinlin on 2015/8/26.
 * 内部业务处理
 */
public abstract class BaseServiceCommunicateBusinessHandler<UP_LEVEL_SESSION_INFO> implements IServiceCommunicateBusinessHandler<ServiceCommunicate.ServiceCommunicateMsg, byte[]> {
    Logger LOG = LoggerFactory.getLogger(BaseServiceCommunicateBusinessHandler.class);
    protected Map<Integer, String> originalServerNameCache = new ConcurrentHashMap<Integer, String>();
    protected Map<Object, String> destAddressCache = new ConcurrentHashMap<Object, String>();
    protected Map<Integer, byte[]> submitDataCache = new ConcurrentHashMap<Integer, byte[]>();
    protected Map<Integer, Object> submitResponseCountCatch = new ConcurrentHashMap<Integer, Object>();
    protected Map<Integer, ServiceCommunicate.ServiceCommunicateMsg> responseDataCatch = new ConcurrentHashMap<Integer, ServiceCommunicate.ServiceCommunicateMsg>();
    protected Map<Integer, Object> responseResultCountCatch = new ConcurrentHashMap<Integer, Object>();

    protected String notificationCommandType;
    protected Map<String, String> businessIDCommandTypeCache = new ConcurrentHashMap<String, String>();
    protected Map<Integer, String> msgIDCommandTypeCache = new ConcurrentHashMap<Integer, String>();
    boolean SubmitResult = true;

    protected String serviceName;
    protected String serviceID;
    protected String businessID;
    protected int messageID;

    private AtomicInteger messageIDAtomic;

    protected UP_LEVEL_SESSION_INFO UpLevelSessionInfo;
    protected JMSMessageCenter jmsMessageCenter;

    public void setUpLevelSessionInfo(UP_LEVEL_SESSION_INFO upLevelSessionInfo) {
        this.UpLevelSessionInfo = upLevelSessionInfo;
    }

    public void setJmsMessageCenter(JMSMessageCenter jmsMessageCenter) {
        this.jmsMessageCenter = jmsMessageCenter;
    }

    public JMSMessageCenter getJmsMessageCenter() {
        return jmsMessageCenter;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getServiceID() {
        return serviceID;
    }

    public byte[] getBusinessID() {
        return businessID.getBytes();
    }

    public BaseServiceCommunicateBusinessHandler(String serviceName, String serviceID) {
        this.serviceName = serviceName;
        this.serviceID = serviceID;
        messageIDAtomic = new AtomicInteger();
    }

    public void notification(String destAddress, byte[] data, String commandType) {
        notificationCommandType = commandType;
    }

    @Override
    public void notification(String destAddress, byte[] data) {
        try {
            String log = "NOTIFICATION----  dest address: " + destAddress;
            if (notificationCommandType != null) {
                log += ", command Type: " + notificationCommandType;
            }
            LOG.info(log);
            sentNotificate(destAddress, serviceName, data);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    public void request(String destAddress, byte[] data, String commandType) {
        if (businessID == null) {
            businessID = UUID.randomUUID().toString();
        }

        businessIDCommandTypeCache.put(businessID, commandType);
        request(destAddress, data);
    }

    @Override
    public void request(String destAddress, byte[] data) {
        if (businessID == null) {
            businessID = UUID.randomUUID().toString();
        }

        try {
            String log = "REQUEST----  service " + destAddress;
            String commandType = businessIDCommandTypeCache.get(businessID);
            if (commandType != null) {
                log += ", command Type: " + commandType;
            }
            LOG.info(log);
            sentRequest(destAddress, serviceName, businessID.getBytes(), data);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }


    @Override
    public void onRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        businessID = new String(msg.getBusinessID().toByteArray());
        String sourceAddress = new String(msg.getResponseAddress().getBytes());
        LOG.info("ON_REQUEST----  source address " + sourceAddress);
        destAddressCache.put(businessID, msg.getResponseAddress());
    }

    public void response(byte[] data, String commandType) {
        businessIDCommandTypeCache.put(businessID, commandType);
        response(data);
    }

    @Override
    public void response(byte[] data) {
        String destAddress = destAddressCache.get(businessID);
        try {
            String log = "RESPONSE----  dest address: " + destAddress;
            String commandType = businessIDCommandTypeCache.get(businessID);
            if (commandType != null) {
                log += ", command Type: " + commandType;
            }
            LOG.info(log);
            sentResponse(destAddress, this.serviceName, businessID.getBytes(), data);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public void addSubmitRequest(String destAddress, byte[] data, String commandType) {
        int msgID = messageIDAtomic.incrementAndGet();
        msgIDCommandTypeCache.put(msgID, commandType);
        addSubmitRequest(msgID, destAddress, data);
    }

    @Override
    public void addSubmitRequest(int msgID, String destAddress, byte[] data) {
        if (businessID == null) {
            businessID = UUID.randomUUID().toString();
        }

        originalServerNameCache.put(msgID, destAddress);
        destAddressCache.put(msgID, destAddress);
        submitDataCache.put(msgID, data);
        submitResponseCountCatch.put(msgID, msgID);
    }

    @Override
    public void submitSubmitRequest() {
        try {
            for (Map.Entry<Integer, byte[]> entry : submitDataCache.entrySet()) {
                int msgID = entry.getKey();
                String destAddress = destAddressCache.get(msgID);
                byte[] data = entry.getValue();

                String log = "SUBMIT_REQUEST----  dest address: " + destAddress;
                String commandType = msgIDCommandTypeCache.get(msgID);
                if (commandType != null) {
                    log += ", command Type: " + commandType;
                }
                LOG.info(log);

                sentBusinessRequest(RequestBusinessType.SUBMIT_REQUEST, destAddress, this.serviceName, businessID.getBytes(), msgID, data);
                submitDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onSubmitRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        businessID = new String(msg.getBusinessID().toByteArray());
        messageID = msg.getMessageID();

        String sourceAddress = new String(msg.getResponseAddress().getBytes());
        LOG.info("ON_SUBMIT_REQUEST----  source address " + sourceAddress);

        destAddressCache.put(messageID, msg.getResponseAddress());
    }

    @Override
    public void submitResponse(byte[] data) {
        String destAddress = destAddressCache.get(messageID);
//        String serviceName = serverNameCache.get(messageID);
        try {
            String log = "SUBMIT_REQUEST----  dest address: " + destAddress;
            String commandType = msgIDCommandTypeCache.get(messageID);
            if (commandType != null) {
                log += ", command Type: " + commandType;
            }
            LOG.info(log);

            sentBusinessResponse(ResponseBusinessType.SUBMIT_RESPONSE, destAddress, this.serviceName, businessID.getBytes(), messageID, data);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void submitErrorResponse(String errMsg) {
        String destAddress = destAddressCache.get(messageID);
//        String serviceName = serverNameCache.get(messageID);
        try {
            String log = "SUBMIT_ERROR_REQUEST----  dest address: " + destAddress;
            String commandType = msgIDCommandTypeCache.get(messageID);
            if (commandType != null) {
                log += ", command Type: " + commandType;
            }
            LOG.info(log);

            sentBusinessErrorResponse(ResponseBusinessType.SUBMIT_RESPONSE, destAddress, this.serviceName, businessID.getBytes(), messageID, errMsg);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void onSubmitResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode result = msg.getResult().getResultCode();
        String sourceAddress = new String(msg.getResponseAddress().getBytes());

        int msgID = msg.getMessageID();
        if (result != ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode.SUCCESS) {
            String log = "ON_SUBMIT_RESPONSE----  source address " + sourceAddress;
            String commandType = msgIDCommandTypeCache.get(msgID);
            if (commandType != null) {
                log += ", command Type: " + commandType + ", commnet: " + msg.getResult().getErrorComment();
            }
            LOG.info(log);
            SubmitResult &= false;
        } else {
            String log = "ON_SUBMIT_ERROR_RESPONSE----  source address " + sourceAddress;
            String commandType = msgIDCommandTypeCache.get(msgID);
            if (commandType != null) {
                log += ", command Type: " + commandType;
            }
            LOG.info(log);
            if (destAddressCache.containsKey(msgID)) {
                destAddressCache.put(msgID, msg.getName());
            }
            responseDataCatch.put(msgID, msg);
            submitResponseCountCatch.remove(msgID);
        }

        if (submitResponseCountCatch.isEmpty()) {
            submitComplete();
        }
    }

    @Override
    public void submitComplete() {
        if (SubmitResult) {
            LOG.debug("ON_SUBMIT_COMPLETE---- SUCCESS");
            submitConfirmRequest();
        } else {
            LOG.debug("ON_SUBMIT_COMPLETE---- FALSE");
            submitCancelRequest();
        }
    }

    @Override
    public void submitConfirmRequest() {
        try {
            for (Map.Entry<Object, String> entry : destAddressCache.entrySet()) {
                int msgID = (Integer) entry.getKey();
                String destAddress = entry.getValue();
//                String serviceName = serverNameCache.get(msgID);
                String log = "SUBMIT_CONFIRM_REQUEST----  dest address: " + destAddress;
                String commandType = msgIDCommandTypeCache.get(msgID);
                if (commandType != null) {
                    log += ", command Type: " + commandType;
                }
                LOG.info(log);

                sentBusinessRequest(RequestBusinessType.CONFIRM_REQUEST, destAddress, this.serviceName, businessID.getBytes(), msgID, null);
                submitDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onConfirmRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        messageID = msg.getMessageID();
        String sourceAddress = new String(msg.getResponseAddress().getBytes());

        String log = "ON_CONFIRM_REQUEST----  source address " + sourceAddress;
        String commandType = msgIDCommandTypeCache.get(messageID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        destAddressCache.put(messageID, msg.getResponseAddress());
    }

    @Override
    public void confirmResponse() {
        String destAddress = destAddressCache.get(messageID);
        try {
            LOG.debug("CONFIRM_RESPONSE----  dest address: " + destAddress);
            sentBusinessResponse(ResponseBusinessType.CONFIRM_RESPONSE, destAddress, this.serviceName, businessID.getBytes(), messageID, null);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void confirmErrorResponse(String errMsg) {
        String destAddress = destAddressCache.get(messageID);
        try {
            LOG.debug("CONFIRM_ERROR_RESPONSE---- des: " + destAddress);
            sentBusinessErrorResponse(ResponseBusinessType.CONFIRM_RESPONSE, destAddress, this.serviceName, businessID.getBytes(), messageID, errMsg);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void onConfirmResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        String sourceAddress = new String(msg.getResponseAddress().getBytes());
        String log = "ON_CONFIRM_RESPONSE----  source address " + sourceAddress;
        String commandType = msgIDCommandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode result = msg.getResult().getResultCode();
        responseResultCountCatch.remove(msgID);
        if (responseResultCountCatch.isEmpty()) {
            List<String> originalServiceAddress = new ArrayList<String>();
            List<ServiceCommunicate.ServiceCommunicateMsg> responseDatas = new ArrayList<ServiceCommunicate.ServiceCommunicateMsg>();

            for(int responseMsgID: responseDataCatch.keySet()) {
                originalServiceAddress.add(originalServerNameCache.get(responseMsgID));
                responseDatas.add(responseDataCatch.get(responseMsgID));
            }
            confirmComplete(originalServiceAddress, responseDatas);
        }

    }

    @Override
    public void submitCancelRequest() {
        try {
            for (Map.Entry<Object, String> entry : destAddressCache.entrySet()) {
                int msgID = (Integer) entry.getKey();
                String destAddress = destAddressCache.get(msgID);
                sentBusinessRequest(RequestBusinessType.CANCEL_REQUEST, destAddress, serviceName, businessID.getBytes(), msgID, null);
                submitDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onCancelRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        messageID = msg.getMessageID();
        LOG.info("CANCEL_REQUEST----  service " + msg.getName());
        destAddressCache.put(messageID, msg.getResponseAddress());
    }

    @Override
    public void cancelResponse() {
        String destAddress = destAddressCache.get(messageID);
        try {
            sentBusinessResponse(ResponseBusinessType.CANCEL_RESPONSE, destAddress, serviceName, businessID.getBytes(), messageID, null);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void cancelErrorResponse(String errMsg) {
        String destAddress = destAddressCache.get(messageID);
        try {
            sentBusinessErrorResponse(ResponseBusinessType.CANCEL_RESPONSE, destAddress, serviceName, businessID.getBytes(), messageID, errMsg);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void onCancelResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        String commandType = msgIDCommandTypeCache.get(msgID);
        LOG.info("CANCEL_RESPONSE----  service " + msg.getName() + " response success, command type: " + commandType);

        ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode result = msg.getResult().getResultCode();
        responseResultCountCatch.remove(msgID);

        if (responseResultCountCatch.isEmpty()) {
            cancelComplete();
        }
    }


    /**
     * 组包数据
     *
     * @param msgType
     * @param serviceName
     * @param responseAddress
     * @param businessID
     * @param msgID
     * @param data
     * @return
     * @throws JMSException
     */
    public byte[] createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType msgType, String serviceName, String responseAddress, byte[] businessID, Integer msgID, ServiceCommunicate.ServiceCommunicateMsg.Result result, byte[] data) throws JMSException {
        ServiceCommunicate.ServiceCommunicateMsg.Builder serviceMsgBuild = ServiceCommunicate.ServiceCommunicateMsg.newBuilder();
        serviceMsgBuild.setVersion(ServiceCommunicate.ServiceCommunicateMsg.getDefaultInstance().getVersion());
        serviceMsgBuild.setMessageType(msgType);
        serviceMsgBuild.setBusinessID(ByteString.copyFrom(businessID));

        if (msgID != null) {
            serviceMsgBuild.setMessageID(msgID);
        }

        serviceMsgBuild.setName(serviceName);
        serviceMsgBuild.setResponseAddress(responseAddress);

        if (result != null) {
            serviceMsgBuild.setResult(result);
        }

        if (data != null) {
            serviceMsgBuild.setContent(ByteString.copyFrom(data));
        }

        return serviceMsgBuild.build().toByteArray();
    }

    /**
     * @param destAddress
     * @param data
     * @throws JMSException
     */
    public void sentNotificate(String destAddress, String serverName, byte[] data) throws JMSException {
        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.NOTIFICATION, "", destAddress, new byte[0], 0, null, data);
        jmsMessageCenter.sentMessage(destAddress, communicateMsgData);
    }

    /**
     * 发送申请数据.REQUEST
     *
     * @param destAddress
     * @param serviceName
     * @param businessID
     * @param data
     * @throws JMSException
     */
    public void sentRequest(String destAddress, String serviceName, byte[] businessID, byte[] data) throws JMSException {
        String responseAddress = destAddress + "To" + this.serviceName + serviceID;
        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.REQUEST, serviceName, responseAddress, businessID, null, null, data);
        jmsMessageCenter.sentAndReceive(responseAddress, this, destAddress, communicateMsgData);
    }

    /**
     * 发送回复消息 RESPONSE
     *
     * @param destAddress
     * @param serviceName
     * @param businessID
     * @param data
     * @throws JMSException
     */
    public void sentResponse(String destAddress, String serviceName, byte[] businessID, byte[] data) throws JMSException {
        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.RESPONSE, serviceName, destAddress, businessID, 0, null, data);
        jmsMessageCenter.sentMessage(destAddress, communicateMsgData);
    }

    /**
     * 发送错误回复 ERROR RESPONSE
     *
     * @param serviceName
     * @param destAddress
     * @param businessID
     * @param errComment
     * @throws JMSException
     */
    public void sentErrorResponse(String destAddress, String serviceName, byte[] businessID, String errComment) throws JMSException {
        ServiceCommunicate.ServiceCommunicateMsg.Result.Builder resultBuilder = ServiceCommunicate.ServiceCommunicateMsg.Result.getDefaultInstance().toBuilder();
        resultBuilder.setResultCode(ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode.FAIL);
        resultBuilder.setErrorComment(errComment);

        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.RESPONSE, serviceName, destAddress, businessID, null, resultBuilder.build(), null);
        jmsMessageCenter.sentMessage(destAddress, communicateMsgData);
    }

    public enum RequestBusinessType {
        SUBMIT_REQUEST,
        CONFIRM_REQUEST,
        CANCEL_REQUEST,
    }

    private ServiceCommunicate.ServiceCommunicateMsg.MessageType convertRequestBusinessType(RequestBusinessType businessType) {
        return ServiceCommunicate.ServiceCommunicateMsg.MessageType.valueOf(businessType.name());
    }

    public enum ResponseBusinessType {
        SUBMIT_RESPONSE,
        CONFIRM_RESPONSE,
        CANCEL_RESPONSE
    }

    private ServiceCommunicate.ServiceCommunicateMsg.MessageType convertResponseBusinessType(ResponseBusinessType businessType) {
        return ServiceCommunicate.ServiceCommunicateMsg.MessageType.valueOf(businessType.name());
    }

    /**
     * 发送业务申请 SUBMIT_REQUEST/CONFIRM_REQUEST/CANCEL_REQUEST
     *
     * @param businessType
     * @param destAddress
     * @param serviceName
     * @param businessID
     * @param msgID
     * @param data
     * @throws JMSException
     */
    public void sentBusinessRequest(RequestBusinessType businessType, String destAddress, String serviceName, byte[] businessID, Integer msgID, byte[] data) throws JMSException {
        String responseAddress = destAddress + "To" + this.serviceName + serviceID;
        ServiceCommunicate.ServiceCommunicateMsg.MessageType msgType = convertRequestBusinessType(businessType);
        byte[] communicateMsgData = createCommunicateMessageData(msgType, serviceName, responseAddress, businessID, msgID, null, data);
        jmsMessageCenter.sentAndReceive(responseAddress, this, destAddress, communicateMsgData);
    }

    /**
     * 发送业务回复 SUBMIT_RESPONSE/CONFIRM_RESPONSE/CANCEL_RESPONSE
     *
     * @param businessType
     * @param destAddress
     * @param serviceName
     * @param businessID
     * @param msgID
     * @param data
     * @throws JMSException
     */
    public void sentBusinessResponse(ResponseBusinessType businessType, String destAddress, String serviceName, byte[] businessID, Integer msgID, byte[] data) throws JMSException {
        String responseAddress = destAddress + "To" + this.serviceName + serviceID;
        ServiceCommunicate.ServiceCommunicateMsg.MessageType msgType = convertResponseBusinessType(businessType);
        byte[] communicateMsgData = createCommunicateMessageData(msgType, serviceName, responseAddress, businessID, msgID, null, data);
        jmsMessageCenter.sentAndReceive(responseAddress, this, destAddress, communicateMsgData);
    }

    /**
     * 发送业务错误回复 ERROR SUBMIT_RESPONSE / ERROR CONFIRM_RESPONSE / ERROR CANCEL_RESPONSE
     *
     * @param businessType
     * @param serviceName
     * @param destAddress
     * @param businessID
     * @param msgID
     * @param errComment
     * @throws JMSException
     */
    public void sentBusinessErrorResponse(ResponseBusinessType businessType, String destAddress, String serviceName, byte[] businessID, Integer msgID, String errComment) throws JMSException {
        ServiceCommunicate.ServiceCommunicateMsg.Result.Builder resultBuilder = ServiceCommunicate.ServiceCommunicateMsg.Result.getDefaultInstance().toBuilder();
        resultBuilder.setResultCode(ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode.FAIL);
        resultBuilder.setErrorComment(errComment);

        ServiceCommunicate.ServiceCommunicateMsg.MessageType msgType = convertResponseBusinessType(businessType);
        byte[] communicateMsgData = createCommunicateMessageData(msgType, serviceName, destAddress, businessID, msgID, resultBuilder.build(), null);
        jmsMessageCenter.sentMessage(destAddress, communicateMsgData);
    }
}
