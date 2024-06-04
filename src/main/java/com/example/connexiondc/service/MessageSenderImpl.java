package com.example.connexiondc.service;

import com.euronext.optiq.dd.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

@Slf4j
@Service
public class MessageSenderImpl implements MessageSender {

    private static final int PORT_START = 30001;
    private static final int PORT_END = 30001;
    private static final String SERVER_ADDRESS = "192.168.64.26";
    private static final Map<Integer, Integer> portToLogicalAccessIdMap = new HashMap<>();
    private static final int HEARTBEAT_INTERVAL = 3000; // 3 seconds

    private final Timer heartbeatTimer = new Timer(true);

    static {
        for (int i = 0; i < 10; i++) {
            portToLogicalAccessIdMap.put(PORT_START + i, 1101 + i);
        }
    }

    @PostConstruct
    public void init() {
        log.info("Initialisation de la connexion");
        try {
            sendLogon(10, 1);
            startHeartbeat();
        } catch (Exception e) {
            log.error("Failed to initialize MessageSender", e);
        }
    }

    @Override
    public void sendLogon(int oePartitionID, int queueingIndicator) {
        for (int port = PORT_START; port <= PORT_END; port++) {
            int logicalAccessID = portToLogicalAccessIdMap.getOrDefault(port, 0);
            if (logicalAccessID == 0) {
                log.warn("No logicalAccessID defined for port {}", port);
                continue;
            }
            try (Socket socket = new Socket(SERVER_ADDRESS, port)) {
                sendLogonMessage(socket, logicalAccessID, oePartitionID, queueingIndicator);
                listenForMessages(socket);
            } catch (Exception e) {
                log.error("Failed to send logon message on port {}", port, e);
            }
        }
    }
    @Override
    public void sendLogonMessage(Socket socket, int logicalAccessID, int oePartitionID, int queueingIndicator) {
        try (OutputStream outputStream = socket.getOutputStream(); InputStream inputStream = socket.getInputStream()) {
            int totalMessageLength = 2 + MessageHeaderEncoder.ENCODED_LENGTH + LogonEncoder.BLOCK_LENGTH;
            ByteBuffer byteBuffer = ByteBuffer.allocate(totalMessageLength);
            UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

            buffer.putShort(0, (short) totalMessageLength);
            MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
            messageHeaderEncoder.wrap(buffer, 2)
                    .blockLength(LogonEncoder.BLOCK_LENGTH)
                    .templateId(LogonEncoder.TEMPLATE_ID)
                    .schemaId(LogonEncoder.SCHEMA_ID)
                    .version(LogonEncoder.SCHEMA_VERSION);

            LogonEncoder logonEncoder = new LogonEncoder();
            logonEncoder.wrap(buffer, 2 + MessageHeaderEncoder.ENCODED_LENGTH)
                    .logicalAccessID(logicalAccessID)
                    .oEPartitionID(oePartitionID)
                    .queueingIndicator((short) queueingIndicator);

            log.info("buffer: {}", buffer.byteArray());
            outputStream.write(buffer.byteArray(), 0, totalMessageLength);
            outputStream.flush();

            byte[] responseBuffer = new byte[22];
            int bytesRead = inputStream.read(responseBuffer);
            if (bytesRead == -1) {
                log.warn("No response received from server on port {}", socket.getPort());
                return;
            }

            processLogonResponse(responseBuffer);
        } catch (Exception e) {
            log.error("Failed to send logon message", e);
        }
    }
    @Override
    public void processLogonResponse(byte[] responseBuffer) {
        UnsafeBuffer responseUnsafeBuffer = new UnsafeBuffer(responseBuffer);
        MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        messageHeaderDecoder.wrap(responseUnsafeBuffer, 2);
        int templateId = messageHeaderDecoder.templateId();
        log.info("Response Template ID: {}", templateId);

        if (templateId == LogonAckDecoder.TEMPLATE_ID) {
            LogonAckDecoder logonAckDecoder = new LogonAckDecoder();
            logonAckDecoder.wrap(responseUnsafeBuffer, 10, LogonDecoder.BLOCK_LENGTH, LogonAckDecoder.SCHEMA_VERSION);
            String exchangeID = logonAckDecoder.exchangeID();
            long lastMsgClientSeqNum = logonAckDecoder.lastClMsgSeqNum();
            log.info("Logon ACK received: exchangeID: {}  lastMsgClientSeqNum: {}", exchangeID, lastMsgClientSeqNum);
        } else if (templateId == LogonRejectDecoder.TEMPLATE_ID) {
            LogonRejectDecoder logonRejectDecoder = new LogonRejectDecoder();
            logonRejectDecoder.wrap(responseUnsafeBuffer, 10, LogonDecoder.BLOCK_LENGTH, LogonRejectDecoder.SCHEMA_VERSION);
            String exchangeID = logonRejectDecoder.exchangeID();
            int logonRejectCodeRaw = logonRejectDecoder.logonRejectCodeRaw();
            long lastMsgClientSeqNum = logonRejectDecoder.lastMsgSeqNum();
            log.info("Logon reject received: exchangeID: {}, logonRejectCodeRaw: {}, lastMsgClientSeqNum: {}", exchangeID, logonRejectCodeRaw, lastMsgClientSeqNum);
        } else {
            log.warn("Unrecognized message template ID: {} received from server", templateId);
        }
    }
    @Override
    public void sendHeartbeat(Socket socket) {
        try (OutputStream outputStream = socket.getOutputStream()) {
            int totalMessageLength = 2 + MessageHeaderEncoder.ENCODED_LENGTH + HeartbeatEncoder.BLOCK_LENGTH;
            ByteBuffer byteBuffer = ByteBuffer.allocate(totalMessageLength);
            UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

            buffer.putShort(0, (short) totalMessageLength);
            MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
            messageHeaderEncoder.wrap(buffer, 2)
                    .blockLength(HeartbeatEncoder.BLOCK_LENGTH)
                    .templateId(HeartbeatEncoder.TEMPLATE_ID)
                    .schemaId(HeartbeatEncoder.SCHEMA_ID)
                    .version(HeartbeatEncoder.SCHEMA_VERSION);

            HeartbeatEncoder heartbeatEncoder = new HeartbeatEncoder();
            heartbeatEncoder.wrap(buffer, 2 + MessageHeaderEncoder.ENCODED_LENGTH);

            log.info("Sending heartbeat");
            outputStream.write(buffer.byteArray(), 0, totalMessageLength);
            outputStream.flush();
        } catch (Exception e) {
            log.error("Failed to send heartbeat", e);
        }
    }
    @Override
    public void startHeartbeat() {
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try (Socket socket = new Socket(SERVER_ADDRESS, PORT_START)) {
                    sendHeartbeat(socket);
                } catch (Exception e) {
                    log.error("Failed to establish socket for heartbeat", e);
                }
            }
        }, 0, HEARTBEAT_INTERVAL);
    }

    @Override
    public void listenForMessages(Socket socket) {
        int maxReads = 10; // Limite du nombre de lectures pour  tests
        int readCount = 0;

        try (InputStream inputStream = socket.getInputStream()) {
            byte[] buffer = new byte[1024];
            while (readCount < maxReads) {
                int bytesRead = inputStream.read(buffer);
                if (bytesRead == -1) {
                    log.warn("No more data to read from server");
                    break;
                }

                MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
                messageHeaderDecoder.wrap(new UnsafeBuffer(buffer), 2);
                int templateId = messageHeaderDecoder.templateId();

                log.info("Template ID: {}", templateId);
                if (templateId == DCMarketStatusChangeDecoder.TEMPLATE_ID) {
                    DCMarketStatusChangeDecoder dcMarketStatusChangeDecoder = new DCMarketStatusChangeDecoder();
                    dcMarketStatusChangeDecoder.wrap(new UnsafeBuffer(buffer), 10, DCMarketStatusChangeDecoder.BLOCK_LENGTH, DCMarketStatusChangeDecoder.SCHEMA_VERSION);
                    long numSeq = dcMarketStatusChangeDecoder.msgSeqNum();
                    long symbolIndex = dcMarketStatusChangeDecoder.symbolIndex();
                    log.info("Received SeqmessageNum: {} , SymbolIndex  {} par port {}", numSeq, symbolIndex, socket.getPort());
                } else if (templateId == DCLongOrderDecoder.TEMPLATE_ID) {
                    DCLongOrderDecoder dcLongOrderDecoder = new DCLongOrderDecoder();
                    dcLongOrderDecoder.wrap(new UnsafeBuffer(buffer), 0, DCLongOrderDecoder.BLOCK_LENGTH, DCLongOrderDecoder.SCHEMA_VERSION);
                    long numSeq = dcLongOrderDecoder.msgSeqNum();
                    log.info("Received message: {} par port {}", numSeq, socket.getPort());
                }
                readCount++;
            }
        } catch (Exception e) {
            log.error("Failed to listen for messages", e);
        }
    }

}
