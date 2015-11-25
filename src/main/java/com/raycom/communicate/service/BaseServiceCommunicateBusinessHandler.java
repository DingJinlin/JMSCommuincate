package com.raycom.communicate.service;

import backtype.storm.utils.RotatingMap;
import com.google.protobuf.ByteString;
import com.raycom.comm.proto.service.ServiceCommunicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dingjinlin on 2015/8/26.
 * 内部业务处理
 */
public abstract class BaseServiceCommunicateBusinessHandler<UP_LEVEL_SESSION_INFO> implements IServiceCommunicateBusinessHandler<ServiceCommunicate.ServiceCommunicateMsg, byte[]> {
    Logger LOG = LoggerFactory.getLogger(BaseServiceCommunicateBusinessHandler.class);
    private static final int MAP_TIMEOUT = 600;

    private static class BusinessInfo {
        String businessID;

        // businessID/msgID, command type
        Map<Object, String> commandTypeCache = new HashMap<Object, String>();

        // msgID, dest service name
        Map<Integer, String> originalServerNameCache = new HashMap<Integer, String>();

        // businessID/msgID, dest service address
        Map<Object, String> destAddressCache = new HashMap<Object, String>();

        // msgID, submit data
        Map<Integer, byte[]> submitDataCache = new HashMap<Integer, byte[]>();

        // msgID
        Set<Integer> submitResponseCount = new HashSet<Integer>();

        // msgID, msg
        Map<Integer, ServiceCommunicate.ServiceCommunicateMsg> responseDataCatch = new HashMap<Integer, ServiceCommunicate.ServiceCommunicateMsg>(MAP_TIMEOUT);

        // msgID
        Set<Integer> responseCount = new HashSet<Integer>();

        BusinessInfo(String businessID) {
            this.businessID = businessID;
        }
    }

    static RotatingMap<String, BusinessInfo> BusinessInfoCache = new RotatingMap<String, BusinessInfo>(MAP_TIMEOUT);

    protected String notificationCommandType;

    protected boolean SubmitResult = true;
    protected String SubmitErrorComment = "";

    protected String serviceName;
    protected String serviceID;
    protected String sourceServiceName;
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

    public int getMessageID() {
        return messageID;
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
        businessID = UUID.randomUUID().toString();
        BusinessInfo businessInfo = new BusinessInfo(businessID);
        businessInfo.commandTypeCache.put(businessID, commandType);

        BusinessInfoCache.put(businessID, businessInfo);

        request(destAddress, data);
    }

    @Override
    public void request(String destAddress, byte[] data) {
        BusinessInfo businessInfo;
        if (businessID == null) {
            businessID = UUID.randomUUID().toString();
            businessInfo = new BusinessInfo(businessID);
            BusinessInfoCache.put(businessID, businessInfo);
        } else {
            businessInfo = BusinessInfoCache.get(businessID);
            if(businessInfo == null) {
                LOG.warn("Not found business by id: " + businessID);
                return;
            }
        }

        try {
            String log = "REQUEST----  service " + destAddress;
            String businessType = businessInfo.commandTypeCache.get(businessID);
            if (businessType != null) {
                log += ", command Type: " + businessType;
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
        this.sourceServiceName = serviceName;
        businessID = new String(msg.getBusinessID().toByteArray());
        BusinessInfo businessInfo = new BusinessInfo(businessID);
        BusinessInfoCache.put(businessID, businessInfo);


        String sourceAddress = new String(msg.getResponseAddress().getBytes());
        LOG.info("ON_REQUEST----  source address " + sourceAddress);

        businessInfo.destAddressCache.put(businessID, msg.getResponseAddress());
    }

    public void response(byte[] data, String commandType) {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        businessInfo.commandTypeCache.put(businessID, commandType);
        response(data);
    }

    @Override
    public void response(byte[] data) {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String destAddress = businessInfo.destAddressCache.get(businessID);
        try {
            String log = "RESPONSE----  dest address: " + destAddress;
            String commandType = businessInfo.commandTypeCache.get(businessID);

            if (commandType != null) {
                log += ", command Type: " + commandType;
            }
            LOG.info(log);
            sentResponse(destAddress, this.serviceName, businessID.getBytes(), data);
            BusinessInfoCache.remove(businessID);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void onResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        String businessID = new String(msg.getBusinessID().toByteArray());
        BusinessInfoCache.remove(businessID);
    }

    public void addSubmitRequest(String destAddress, byte[] data, String commandType) {
        businessID = UUID.randomUUID().toString();
        BusinessInfo businessInfo = new BusinessInfo(businessID);
        BusinessInfoCache.put(businessID, businessInfo);

        int msgID = messageIDAtomic.incrementAndGet();
        businessInfo.commandTypeCache.put(msgID, commandType);
        addSubmitRequest(msgID, destAddress, data);
    }

    @Override
    public void addSubmitRequest(int msgID, String destAddress, byte[] data) {
        BusinessInfo businessInfo;
        if (businessID == null) {
            businessID = UUID.randomUUID().toString();
            businessInfo = new BusinessInfo(businessID);
            BusinessInfoCache.put(businessID, businessInfo);
        } else {
            businessInfo = BusinessInfoCache.get(businessID);
            if(businessInfo == null) {
                LOG.warn("Not found business by id: " + businessID);
                return;
            }
        }

        businessInfo.originalServerNameCache.put(msgID, destAddress);
        businessInfo.destAddressCache.put(msgID, destAddress);
        businessInfo.submitDataCache.put(msgID, data);
        businessInfo.submitResponseCount.add(msgID);
    }

    @Override
    public void submitSubmitRequest() {
        try {
            BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
            if(businessInfo == null) {
                LOG.warn("Not found business by id: " + businessID);
                return;
            }

            for (Map.Entry<Integer, byte[]> entry : businessInfo.submitDataCache.entrySet()) {
                int msgID = entry.getKey();
                String destServiceName = businessInfo.originalServerNameCache.get(msgID);
                String destAddress = businessInfo.destAddressCache.get(msgID);
                byte[] data = entry.getValue();

                String log = "SUBMIT_REQUEST----  dest address: " + destAddress;
                String commandType = businessInfo.commandTypeCache.get(msgID);
                if (commandType != null) {
                    log += ", command Type: " + commandType;
                }
                LOG.info(log);

                sentBusinessRequest(RequestBusinessType.SUBMIT_REQUEST, destServiceName, destAddress, this.serviceName, businessID.getBytes(), msgID, data);
                businessInfo.submitDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onSubmitRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        this.sourceServiceName = serviceName;
        this.businessID = new String(msg.getBusinessID().toByteArray());
        this.messageID = msg.getMessageID();

        String sourceAddress = new String(msg.getResponseAddress().getBytes());
        LOG.info("ON_SUBMIT_REQUEST----  source address " + sourceAddress);

        BusinessInfo businessInfo = new BusinessInfo(businessID);
        BusinessInfoCache.put(businessID, businessInfo);
        businessInfo.destAddressCache.put(messageID, msg.getResponseAddress());

        businessInfo.submitResponseCount.add(messageID);
    }

    @Override
    public void submitResponse(byte[] data) {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String destAddress = businessInfo.destAddressCache.get(messageID);
        try {
            String log = "SUBMIT_REQUEST----  dest address: " + destAddress;
            String commandType = businessInfo.commandTypeCache.get(messageID);
            if (commandType != null) {
                log += ", command Type: " + commandType;
            }
            LOG.info(log);

            sentBusinessResponse(ResponseBusinessType.SUBMIT_RESPONSE, sourceServiceName, destAddress, this.serviceName, businessID.getBytes(), messageID, data);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void submitErrorResponse(String errMsg) {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String destAddress = businessInfo.destAddressCache.get(messageID);
        try {
            String log = "SUBMIT_ERROR_REQUEST----  dest address: " + destAddress;
            String commandType = businessInfo.commandTypeCache.get(messageID);
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
        String businessID = new String(msg.getBusinessID().toByteArray());
        int msgID = msg.getMessageID();

        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String commandType = businessInfo.commandTypeCache.get(msgID);
        businessInfo.submitResponseCount.remove(msgID);

        if (result != ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode.SUCCESS) {
            String log = "ON_SUBMIT_RESPONSE----  source address " + sourceAddress;
            if (commandType != null) {
                log += ", command Type: " + commandType + ", commnet: " + msg.getResult().getErrorComment();
            }
            LOG.error(log);
            SubmitResult &= false;
            SubmitErrorComment = msg.getResult().getErrorComment();
        } else {
            String log = "ON_SUBMIT_ERROR_RESPONSE----  source address " + sourceAddress;
            if (commandType != null) {
                log += ", command Type: " + commandType;
            }
            LOG.info(log);
            if (businessInfo.destAddressCache.containsKey(msgID)) {
                businessInfo.destAddressCache.put(msgID, msg.getName());
            }
            businessInfo.responseDataCatch.put(msgID, msg);
        }

        if (businessInfo.submitResponseCount.isEmpty()) {
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
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        try {
            for (Map.Entry<Object, String> entry : businessInfo.destAddressCache.entrySet()) {
                int msgID = (Integer) entry.getKey();
                String destServiceName = businessInfo.originalServerNameCache.get(msgID);
                String destAddress = entry.getValue();
                String log = "SUBMIT_CONFIRM_REQUEST----  dest address: " + destAddress;
                String commandType = businessInfo.commandTypeCache.get(msgID);
                if (commandType != null) {
                    log += ", command Type: " + commandType;
                }
                LOG.info(log);

                sentBusinessRequest(RequestBusinessType.CONFIRM_REQUEST, destServiceName, destAddress, this.serviceName, businessID.getBytes(), msgID, null);
                businessInfo.responseCount.add(msgID);
                businessInfo.submitDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onConfirmRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        businessID = new String(msg.getBusinessID().toByteArray());
        messageID = msg.getMessageID();
        String sourceAddress = new String(msg.getResponseAddress().getBytes());
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String log = "ON_CONFIRM_REQUEST----  source address " + sourceAddress;
        String commandType = businessInfo.commandTypeCache.get(messageID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        businessInfo.destAddressCache.put(messageID, msg.getResponseAddress());
        businessInfo.submitResponseCount.remove(messageID);
    }

    @Override
    public void confirmResponse() {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String destAddress = businessInfo.destAddressCache.get(messageID);
        try {
            LOG.debug("CONFIRM_RESPONSE----  dest address: " + destAddress);
            sentBusinessResponse(ResponseBusinessType.CONFIRM_RESPONSE, sourceServiceName, destAddress, this.serviceName, businessID.getBytes(), messageID, null);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }

        if(businessInfo.submitResponseCount.isEmpty()) {
            BusinessInfoCache.remove(businessID);
        }
    }

    @Override
    public void confirmErrorResponse(String errMsg) {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String destAddress = businessInfo.destAddressCache.get(messageID);
        try {
            LOG.debug("CONFIRM_ERROR_RESPONSE---- des: " + destAddress);
            sentBusinessErrorResponse(ResponseBusinessType.CONFIRM_RESPONSE, destAddress, this.serviceName, businessID.getBytes(), messageID, errMsg);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }

        if(businessInfo.submitResponseCount.isEmpty()) {
            BusinessInfoCache.remove(businessID);
        }
    }

    @Override
    public void onConfirmResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        String businessID = new String(msg.getBusinessID().toByteArray());
        int msgID = msg.getMessageID();

        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String sourceAddress = new String(msg.getResponseAddress().getBytes());

        String log = "ON_CONFIRM_RESPONSE----  source address " + sourceAddress;
        String commandType = businessInfo.commandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode result = msg.getResult().getResultCode();
        businessInfo.responseCount.remove(msgID);
        if (businessInfo.responseCount.isEmpty()) {
            List<String> originalServiceAddress = new ArrayList<String>();
            List<ServiceCommunicate.ServiceCommunicateMsg> responseDatas = new ArrayList<ServiceCommunicate.ServiceCommunicateMsg>();

            for (int responseMsgID : businessInfo.responseDataCatch.keySet()) {
                originalServiceAddress.add(businessInfo.originalServerNameCache.get(responseMsgID));
                responseDatas.add(businessInfo.responseDataCatch.get(responseMsgID));
            }
            confirmComplete(originalServiceAddress, responseDatas);
        }

    }

    @Override
    public void confirmComplete(List<String> sourceAddress, List<ServiceCommunicate.ServiceCommunicateMsg> serviceCommunicateMsgs) {
        BusinessInfoCache.remove(businessID);
    }

    @Override
    public void submitCancelRequest() {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        try {
            for (Map.Entry<Object, String> entry : businessInfo.destAddressCache.entrySet()) {
                int msgID = (Integer) entry.getKey();
                String destServiceName = businessInfo.originalServerNameCache.get(msgID);
                String destAddress = businessInfo.destAddressCache.get(msgID);
                sentBusinessRequest(RequestBusinessType.CANCEL_REQUEST, destServiceName, destAddress, serviceName, businessID.getBytes(), msgID, null);
                businessInfo.responseCount.add(msgID);
                businessInfo.submitDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onCancelRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        String businessID = new String(msg.getBusinessID().toByteArray());
        messageID = msg.getMessageID();
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        LOG.info("CANCEL_REQUEST----  service " + msg.getName());
        businessInfo.destAddressCache.put(messageID, msg.getResponseAddress());
        businessInfo.submitResponseCount.remove(messageID);
    }

    @Override
    public void cancelResponse() {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String destAddress = businessInfo.destAddressCache.get(messageID);
        try {
            sentBusinessResponse(ResponseBusinessType.CANCEL_RESPONSE, sourceServiceName, destAddress, serviceName, businessID.getBytes(), messageID, null);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }

        if(businessInfo.submitResponseCount.isEmpty()) {
            BusinessInfoCache.remove(businessID);
        }
    }

    @Override
    public void cancelErrorResponse(String errMsg) {
        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }

        String destAddress = businessInfo.destAddressCache.get(messageID);
        try {
            sentBusinessErrorResponse(ResponseBusinessType.CANCEL_RESPONSE, destAddress, serviceName, businessID.getBytes(), messageID, errMsg);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void onCancelResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        String businessID = new String(msg.getBusinessID().toByteArray());
        int msgID = msg.getMessageID();

        BusinessInfo businessInfo = BusinessInfoCache.get(businessID);
        if(businessInfo == null) {
            LOG.warn("Not found business by id: " + businessID);
            return;
        }
        String commandType = businessInfo.commandTypeCache.get(msgID);
        LOG.info("CANCEL_RESPONSE----  service " + msg.getName() + " response success, command type: " + commandType);

        ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode result = msg.getResult().getResultCode();
        businessInfo.responseCount.remove(msgID);

        if (businessInfo.responseCount.isEmpty()) {
            cancelComplete();
        }
    }

    @Override
    public void cancelComplete() {
        BusinessInfoCache.remove(businessID);
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
        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.NOTIFICATION, serverName, "", new byte[0], 0, null, data);
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
    public void sentBusinessRequest(RequestBusinessType businessType, String destServiceName, String destAddress, String serviceName, byte[] businessID, Integer msgID, byte[] data) throws JMSException {
        String responseAddress = destServiceName + "To" + this.serviceName + serviceID;
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
    public void sentBusinessResponse(ResponseBusinessType businessType, String destServiceName, String destAddress, String serviceName, byte[] businessID, Integer msgID, byte[] data) throws JMSException {
        String responseAddress = destServiceName + "To" + this.serviceName + serviceID;
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
