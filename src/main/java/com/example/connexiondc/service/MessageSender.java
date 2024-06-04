package com.example.connexiondc.service;

import java.net.Socket;

public interface MessageSender {

    void sendLogon(int oePartitionID, int queueingIndicator);

    void sendLogonMessage(Socket socket, int logicalAccessID, int oePartitionID, int queueingIndicator) ;
    void processLogonResponse(byte[] responseBuffer) ;

    void sendHeartbeat(Socket socket) ;
    void startHeartbeat() ;

    void listenForMessages(Socket socket) ;

    }
