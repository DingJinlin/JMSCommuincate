package com.raycom.communicate.service;

import com.google.protobuf.ByteString;
import com.raycom.comm.proto.service.ServiceCommunicate;
import com.raycom.support.convert.IntegerConvert;
import com.raycom.util.codec.HexCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dingjinlin on 2015/8/26.
 * 内部业务处理
 */
public abstract class BaseServiceCommunicateBusinessHandler<UP_LEVEL_SESSION_INFO> implements IServiceCommunicateBusinessHandler<ServiceCommunicate.ServiceCommunicateMsg, byte[]> {
    Logger LOG = LoggerFactory.getLogger(BaseServiceCommunicateBusinessHandler.class);
    private static final int MAP_TIMEOUT = 600;

    protected static class BusinessInfo {
        public UUID businessID;

        // msgID, command type
        public Map<Integer, String> commandTypeCache = new HashMap<Integer, String>();

        // msgID, opposite service name
        public Map<Integer, String> oppositeServerNameCache = new HashMap<Integer, String>();

        // msgID, dest service address
        public Map<Integer, String> destAddressCache = new HashMap<Integer, String>();

        // msgID, submit data
        public Map<Integer, byte[]> sentDataCache = new HashMap<Integer, byte[]>();

        // msgID
        public Set<Integer> submitResponseCount = new HashSet<Integer>();

        // msgID, msg
        public Map<Integer, ServiceCommunicate.ServiceCommunicateMsg> responseDataCatch = new HashMap<Integer, ServiceCommunicate.ServiceCommunicateMsg>(MAP_TIMEOUT);

        // msgID
        public Set<Integer> responseCount = new HashSet<Integer>();

        public BusinessInfo(UUID businessID) {
            this.businessID = businessID;
        }
    }

    protected String serviceName;
    protected String serviceID;
    private AtomicInteger messageIDAtomic;
    protected BusinessInfo businessInfo;

    private boolean SubmitResult = false;
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
        return uuid2bytes(businessInfo.businessID);
    }

    public BaseServiceCommunicateBusinessHandler(String serviceName, String serviceID) {
        this.serviceName = serviceName;
        this.serviceID = serviceID;
        messageIDAtomic = new AtomicInteger();
    }

    public void notification(String destAddress, byte[] data, String commandType) {
        int msgID = 1;
        businessInfo = new BusinessInfo(UUID.randomUUID());
        businessInfo.commandTypeCache.put(msgID, commandType);

        notification(destAddress, data);
    }

    @Override
    public void notification(String destAddress, byte[] data) {
        int msgID = 1;
        if(businessInfo == null) {
            businessInfo = new BusinessInfo(UUID.randomUUID());
        }

        businessInfo.oppositeServerNameCache.put(msgID, destAddress);
        businessInfo.destAddressCache.put(msgID, destAddress);
        businessInfo.sentDataCache.put(msgID, data);

        String log = "NOTIFICATION----  dest address: " + destAddress;
        String commandType = businessInfo.commandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        try {
            sentNotification();
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onNotification(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        if(businessInfo == null) {
            UUID businessID = bytes2uuid(msg.getBusinessID().toByteArray());
            businessInfo = new BusinessInfo(businessID);
        }
        int msgID = msg.getMessageID();
        businessInfo.destAddressCache.put(msgID, msg.getResponseAddress());
        businessInfo.oppositeServerNameCache.put(msgID, msg.getName());
        LOG.info("ON_Notification----  source Service " + msg.getName());
    }

    public void request(String destAddress, byte[] data, String commandType) {
        int msgID = 1;
        businessInfo.commandTypeCache.put(msgID, commandType);

        request(destAddress, data);
    }

    @Override
    public void request(String destAddress, byte[] data) {
        int msgID = 1;
        if(businessInfo == null) {
            businessInfo = new BusinessInfo(UUID.randomUUID());
        }

        businessInfo.oppositeServerNameCache.put(msgID, destAddress);
        businessInfo.destAddressCache.put(msgID, destAddress);
        businessInfo.sentDataCache.put(msgID, data);

        String log = "REQUEST----  service " + destAddress;
        String commandType = businessInfo.commandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        try {
            sentRequest();
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        if(businessInfo == null) {
            UUID businessID = bytes2uuid(msg.getBusinessID().toByteArray());
            businessInfo = new BusinessInfo(businessID);
        }
        int msgID = msg.getMessageID();
        businessInfo.destAddressCache.put(msgID, msg.getResponseAddress());
        businessInfo.oppositeServerNameCache.put(msgID, msg.getName());
        LOG.info("ON_REQUEST----  source Service " + msg.getName());
    }

    public void response(byte[] data, String commandType) {
        int msgID = 1;
        businessInfo.commandTypeCache.put(msgID, commandType);
        response(data);
    }

    @Override
    public void response(byte[] data) {
        int msgID = 1;
        String destAddress = businessInfo.destAddressCache.get(msgID);
        businessInfo.sentDataCache.put(msgID, data);

        String log = "RESPONSE----  dest address: " + destAddress;
        String commandType = businessInfo.commandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        try {
            sentResponse();
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void onResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        businessInfo.responseDataCatch.put(msgID, msg);
    }

    public void addSubmitRequest(String destAddress, byte[] data, String commandType) {
        int msgID = messageIDAtomic.incrementAndGet();
        businessInfo.commandTypeCache.put(msgID, commandType);
        addSubmitRequest(msgID, destAddress, data);
    }

    @Override
    public void addSubmitRequest(int MsgID, String destAddress, byte[] data) {
        int msgID = MsgID;
        if(msgID < 1) {
            msgID = messageIDAtomic.incrementAndGet();
        }

        businessInfo.oppositeServerNameCache.put(msgID, destAddress);
        businessInfo.destAddressCache.put(msgID, destAddress);
        businessInfo.sentDataCache.put(msgID, data);
        businessInfo.submitResponseCount.add(msgID);
    }

    @Override
    public void submitSubmitRequest() {
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);

        try {
            for (Map.Entry<Integer, byte[]> entry : businessInfo.sentDataCache.entrySet()) {
                int msgID = entry.getKey();
                String destServiceName = businessInfo.oppositeServerNameCache.get(msgID);
                String destAddress = businessInfo.destAddressCache.get(msgID);
                byte[] data = entry.getValue();

                String log = "SUBMIT_REQUEST----  dest address: " + destAddress;
                String commandType = businessInfo.commandTypeCache.get(msgID);
                if (commandType != null) {
                    log += ", command Type: " + commandType;
                }
                LOG.info(log);

                sentBusinessRequest(RequestBusinessType.SUBMIT_REQUEST, destServiceName, destAddress, this.serviceName, bBusinessID, msgID, data);
                businessInfo.sentDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onSubmitRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        businessInfo.oppositeServerNameCache.put(msgID, msg.getName());
        businessInfo.responseDataCatch.put(msgID, msg);
        businessInfo.destAddressCache.put(msgID, msg.getResponseAddress());
        businessInfo.submitResponseCount.add(msgID);

        LOG.info("ON_SUBMIT_REQUEST----  source service name " + msg.getName());
    }

    @Override
    public void submitResponse(int msgID, byte[] data) {
        businessInfo.sentDataCache.put(msgID, data);

        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);
        String sourceServiceName = businessInfo.oppositeServerNameCache.get(msgID);

        String log = "SUBMIT_REQUEST----  dest address: " + destAddress;
        String commandType = businessInfo.commandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);
        try {
            sentBusinessResponse(ResponseBusinessType.SUBMIT_RESPONSE, sourceServiceName, destAddress, this.serviceName, bBusinessID, msgID, data);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }

        businessInfo.submitResponseCount.remove(msgID);
        if(businessInfo.submitResponseCount.isEmpty()) {
            submitComplete(businessInfo.businessID);
        }
    }

    @Override
    public void submitErrorResponse(int msgID, String errMsg) {
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);

        String log = "SUBMIT_ERROR_REQUEST----  dest address: " + destAddress;
        String commandType = businessInfo.commandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        try {
            sentBusinessErrorResponse(ResponseBusinessType.SUBMIT_RESPONSE, destAddress, this.serviceName, bBusinessID, msgID, errMsg);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void onSubmitResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        businessInfo.oppositeServerNameCache.put(msgID, msg.getName());
        businessInfo.destAddressCache.put(msgID, msg.getResponseAddress());
        businessInfo.submitResponseCount.remove(msgID);

        ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode result = msg.getResult().getResultCode();
        String commandType = businessInfo.commandTypeCache.get(msgID);

        if (result != ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode.SUCCESS) {
            String log = "ON_SUBMIT_ERROR_RESPONSE----  source service name " + msg.getName();
            if (commandType != null) {
                log += ", command Type: " + commandType + ", comment: " + msg.getResult().getErrorComment();
            }
            LOG.error(log);
            SubmitResult &= false;
        } else {
            String log = "ON_SUBMIT_RESPONSE----  source service name " + msg.getName();
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
            submitComplete(businessInfo.businessID);
        }
    }

    @Override
    public void submitComplete(UUID businessID) {
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
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        try {
            for (Map.Entry<Integer, String> entry : businessInfo.destAddressCache.entrySet()) {
                int msgID = entry.getKey();
                String destServiceName = businessInfo.oppositeServerNameCache.get(msgID);
                String destAddress = entry.getValue();
                String log = "SUBMIT_CONFIRM_REQUEST----  dest address: " + destAddress;
                String commandType = businessInfo.commandTypeCache.get(msgID);
                if (commandType != null) {
                    log += ", command Type: " + commandType;
                }
                LOG.info(log);

                sentBusinessRequest(RequestBusinessType.CONFIRM_REQUEST, destServiceName, destAddress, this.serviceName, bBusinessID, msgID, null);
                businessInfo.responseCount.add(msgID);
                businessInfo.sentDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onConfirmRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        businessInfo.oppositeServerNameCache.put(msgID, msg.getName());
        businessInfo.destAddressCache.put(msgID, msg.getResponseAddress());

        String log = "ON_CONFIRM_REQUEST----  source service name " + msg.getName();
        String commandType = businessInfo.commandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        businessInfo.responseCount.add(msgID);
    }

    @Override
    public void confirmResponse(int msgID) {
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);
        try {
            LOG.debug("CONFIRM_RESPONSE----  dest address: " + destAddress);
            sentBusinessResponse(ResponseBusinessType.CONFIRM_RESPONSE, destAddress, this.serviceName, bBusinessID, msgID);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
        businessInfo.responseCount.remove(msgID);

        if (businessInfo.responseCount.isEmpty()) {
            confirmComplete(businessInfo.businessID);
        }
    }

    @Override
    public void confirmErrorResponse(int msgID, String errMsg) {
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);
        try {
            LOG.debug("CONFIRM_ERROR_RESPONSE---- des: " + destAddress);
            sentBusinessErrorResponse(ResponseBusinessType.CONFIRM_RESPONSE, destAddress, this.serviceName, bBusinessID, msgID, errMsg);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
        businessInfo.responseCount.remove(msgID);
        if (businessInfo.responseCount.isEmpty()) {
           cancelComplete(businessInfo.businessID);
        }
    }

    @Override
    public void onConfirmResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        businessInfo.oppositeServerNameCache.put(msgID, msg.getName());


        String log = "ON_CONFIRM_RESPONSE----  source service name " + msg.getName();
        String commandType = businessInfo.commandTypeCache.get(msgID);
        if (commandType != null) {
            log += ", command Type: " + commandType;
        }
        LOG.info(log);

        ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode result = msg.getResult().getResultCode();
        if(result == ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode.SUCCESS) {

        }
        businessInfo.responseCount.remove(msgID);
        if (businessInfo.responseCount.isEmpty()) {
            List<String> originalServiceAddress = new ArrayList<String>();
            List<ServiceCommunicate.ServiceCommunicateMsg> responseDatas = new ArrayList<ServiceCommunicate.ServiceCommunicateMsg>();

            for (int responseMsgID : businessInfo.responseDataCatch.keySet()) {
                originalServiceAddress.add(businessInfo.oppositeServerNameCache.get(responseMsgID));
                responseDatas.add(businessInfo.responseDataCatch.get(responseMsgID));
            }
            confirmComplete(businessInfo.businessID, originalServiceAddress, responseDatas);
        }

    }

    @Override
    public void submitCancelRequest() {
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        try {
            for (Map.Entry<Integer, String> entry : businessInfo.destAddressCache.entrySet()) {
                int msgID = entry.getKey();
                String destServiceName = businessInfo.oppositeServerNameCache.get(msgID);
                String destAddress = businessInfo.destAddressCache.get(msgID);
                sentBusinessRequest(RequestBusinessType.CANCEL_REQUEST, destServiceName, destAddress, serviceName, bBusinessID, msgID, null);
                businessInfo.responseCount.add(msgID);
                businessInfo.sentDataCache.remove(msgID);
            }
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
            // TODO: 回复数据发送失败
        }
    }

    @Override
    public void onCancelRequest(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        businessInfo.oppositeServerNameCache.put(msgID, msg.getName());
        businessInfo.destAddressCache.put(msgID, msg.getResponseAddress());
        businessInfo.responseCount.add(msgID);

        LOG.info("CANCEL_REQUEST----  service name" + msg.getName());
    }

    @Override
    public void cancelResponse(int msgID) {
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);

        String destAddress = businessInfo.destAddressCache.get(msgID);
        try {
            sentBusinessResponse(ResponseBusinessType.CANCEL_RESPONSE, destAddress, serviceName, bBusinessID, msgID);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
        businessInfo.responseCount.remove(msgID);

        if (businessInfo.responseCount.isEmpty()) {
            cancelComplete(businessInfo.businessID);
        }
    }

    @Override
    public void cancelErrorResponse(int msgID, String errMsg) {
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);

        try {
            sentBusinessErrorResponse(ResponseBusinessType.CANCEL_RESPONSE, destAddress, serviceName, bBusinessID, msgID, errMsg);
        } catch (JMSException e) {
            LOG.error(e.getMessage(), e);
        }
        businessInfo.responseCount.remove(msgID);

        if (businessInfo.responseCount.isEmpty()) {
            cancelComplete(businessInfo.businessID);
        }
    }

    @Override
    public void onCancelResponse(String serviceName, ServiceCommunicate.ServiceCommunicateMsg msg) {
        int msgID = msg.getMessageID();
        businessInfo.oppositeServerNameCache.put(msgID, msg.getName());


        String commandType = businessInfo.commandTypeCache.get(msgID);
        LOG.info("CANCEL_RESPONSE----  service " + msg.getName() + " response success, command type: " + commandType);

        ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode result = msg.getResult().getResultCode();
        businessInfo.responseCount.remove(msgID);

        if (businessInfo.responseCount.isEmpty()) {
            cancelComplete(businessInfo.businessID);
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
        if(responseAddress != null) {
            serviceMsgBuild.setResponseAddress(responseAddress);
        }

        if (result != null) {
            serviceMsgBuild.setResult(result);
        } else {
            ServiceCommunicate.ServiceCommunicateMsg.Result.Builder resultBuilder = ServiceCommunicate.ServiceCommunicateMsg.Result.getDefaultInstance().toBuilder();
            resultBuilder.setResultCode(ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode.SUCCESS);
            serviceMsgBuild.setResult(resultBuilder);
        }

        if (data != null) {
            serviceMsgBuild.setContent(ByteString.copyFrom(data));
        }

        return serviceMsgBuild.build().toByteArray();
    }

    /**
     * 发送通知
     *
     * @throws JMSException
     */
    public void sentNotification() throws JMSException {
        int msgID = 1;
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);
        byte[] data = businessInfo.sentDataCache.get(msgID);


        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.NOTIFICATION, serviceName, "", bBusinessID, msgID, null, data);
        jmsMessageCenter.sentMessage(destAddress, communicateMsgData);
    }

    /**
     * 发送申请数据
     *
     * @throws JMSException
     */
    public void sentRequest() throws JMSException {
        int msgID = 1;
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);
        byte[] data = businessInfo.sentDataCache.get(msgID);

        String responseAddress = destAddress + "To" + this.serviceName + serviceID;
        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.REQUEST, serviceName, responseAddress, bBusinessID, msgID, null, data);
        jmsMessageCenter.sentAndReceive(responseAddress, this, destAddress, communicateMsgData);
    }

    /**
     * 发送回复消息 RESPONSE
     *
     * @throws JMSException
     */
    public void sentResponse() throws JMSException {
        int msgID = 1;
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);
        byte[] data = businessInfo.sentDataCache.get(msgID);

        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.RESPONSE, serviceName, destAddress, bBusinessID, msgID, null, data);
        jmsMessageCenter.sentMessage(destAddress, communicateMsgData);
    }

    /**
     * 发送错误回复 ERROR RESPONSE
     *
     * @param errComment
     * @throws JMSException
     */
    public void sentErrorResponse(String errComment) throws JMSException {
        int msgID = 1;
        byte[] bBusinessID = uuid2bytes(businessInfo.businessID);
        String destAddress = businessInfo.destAddressCache.get(msgID);

        ServiceCommunicate.ServiceCommunicateMsg.Result.Builder resultBuilder = ServiceCommunicate.ServiceCommunicateMsg.Result.getDefaultInstance().toBuilder();
        resultBuilder.setResultCode(ServiceCommunicate.ServiceCommunicateMsg.Result.ResultCode.FAIL);
        resultBuilder.setErrorComment(errComment);

        byte[] communicateMsgData = createCommunicateMessageData(ServiceCommunicate.ServiceCommunicateMsg.MessageType.RESPONSE, serviceName, destAddress, bBusinessID, msgID, resultBuilder.build(), null);
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
     * 发送业务回复 SUBMIT_RESPONSE
     *
     * @param businessType      业务类型
     * @param destServiceName   目的服务名
     * @param destAddress       目的地址
     * @param serviceName       本地服务名
     * @param businessID        业务ID
     * @param msgID             消息ID
     * @param data              发送数据
     * @throws JMSException
     */
    public void sentBusinessResponse(ResponseBusinessType businessType, String destServiceName, String destAddress, String serviceName, byte[] businessID, Integer msgID, byte[] data) throws JMSException {
        String responseAddress = destServiceName + "To" + this.serviceName + serviceID;
        ServiceCommunicate.ServiceCommunicateMsg.MessageType msgType = convertResponseBusinessType(businessType);
        byte[] communicateMsgData = createCommunicateMessageData(msgType, serviceName, responseAddress, businessID, msgID, null, data);
        jmsMessageCenter.sentAndReceive(responseAddress, this, destAddress, communicateMsgData);
    }

    /**
     * 发送业务回复 CONFIRM_RESPONSE/CANCEL_RESPONSE
     * @param businessType      业务类型
     * @param destAddress       目的地址
     * @param serviceName       本地服务名
     * @param businessID        业务ID
     * @param msgID             消息ID
     * @throws JMSException
     */
    public void sentBusinessResponse(ResponseBusinessType businessType, String destAddress, String serviceName, byte[] businessID, Integer msgID) throws JMSException {
        ServiceCommunicate.ServiceCommunicateMsg.MessageType msgType = convertResponseBusinessType(businessType);
        byte[] communicateMsgData = createCommunicateMessageData(msgType, serviceName, null, businessID, msgID, null, null);
        jmsMessageCenter.sentMessage(destAddress, communicateMsgData);
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

    public byte[] uuid2bytes(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();

        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.put(IntegerConvert.long2bytes(ByteOrder.BIG_ENDIAN, msb));
        byteBuffer.put(IntegerConvert.long2bytes(ByteOrder.BIG_ENDIAN, lsb));

        return byteBuffer.array();
    }

    public UUID bytes2uuid(byte[] bUuid) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bUuid);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);

        long msb = byteBuffer.getLong();
        long lsb = byteBuffer.getLong();

        return new UUID(msb, lsb);
    }
}
