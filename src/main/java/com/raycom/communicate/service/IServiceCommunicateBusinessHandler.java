package com.raycom.communicate.service;

import java.util.List;

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
    void submitResponse(RESPONSE_DATA data);
    void submitErrorResponse(String errMsg);
    void onSubmitResponse(String serviceName, REQUEST_DATA data);
    void submitComplete();

    void submitConfirmRequest();
    void onConfirmRequest(String serviceName, REQUEST_DATA data);
    void confirmResponse();
    void confirmErrorResponse(String errMsg);
    void onConfirmResponse(String serviceName, REQUEST_DATA data);
    void confirmComplete(List<String> sourceAddress, List<REQUEST_DATA> datas);

    void submitCancelRequest();
    void onCancelRequest(String serviceName, REQUEST_DATA data);
    void cancelResponse();
    void cancelErrorResponse(String errMsg);
    void onCancelResponse(String serviceName, REQUEST_DATA data);
    void cancelComplete();
}
