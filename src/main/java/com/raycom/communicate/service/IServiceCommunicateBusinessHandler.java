package com.raycom.communicate.service;

import java.util.List;
import java.util.UUID;

/**
 *
 * Created by dingjinlin on 2015/8/26.
 */
public interface IServiceCommunicateBusinessHandler<REQUEST_DATA, RESPONSE_DATA> {
    void notification(String destAddress, RESPONSE_DATA data);
    void onNotification(String serviceName, REQUEST_DATA data);

    void request(String destAddress, RESPONSE_DATA data);
    void onRequest(String serviceName, REQUEST_DATA data);
    void response(RESPONSE_DATA data);
    void onResponse(String serviceName, REQUEST_DATA data);

    void addSubmitRequest(int msgID, String destAddress, RESPONSE_DATA data);
    void submitSubmitRequest();
    void onSubmitRequest(String serviceName, REQUEST_DATA data);
    void submitResponse(int msgID, RESPONSE_DATA data);
    void submitErrorResponse(int msgID, String errMsg);
    void onSubmitResponse(String serviceName, REQUEST_DATA data);

    /**
     * 业务申请与响应端提交回复完成
     */
    void submitComplete(UUID businessID);

    void submitConfirmRequest();
    void onConfirmRequest(String serviceName, REQUEST_DATA data);
    void confirmResponse(int msgID);
    void confirmErrorResponse(int msgID, String errMsg);
    void onConfirmResponse(String serviceName, REQUEST_DATA data);

    /**
     * 业务申请端确认回复完成
     * @param sourceAddress 源地址
     * @param datas 数据
     */
    void confirmComplete(UUID businessID, List<String> sourceAddress, List<REQUEST_DATA> datas);

    /**
     * 业务响应端确认回复完成
     */
    void confirmComplete(UUID businessID);

    void submitCancelRequest();
    void onCancelRequest(String serviceName, REQUEST_DATA data);
    void cancelResponse(int msgID);
    void cancelErrorResponse(int msgID, String errMsg);
    void onCancelResponse(String serviceName, REQUEST_DATA data);

    /**
     * 业务申请与响应端取消回复完成
     */
    void cancelComplete(UUID businessID);
}
