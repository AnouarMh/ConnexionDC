package com.example.connexiondc.Connexion;

import com.example.connexiondc.service.MessageSenderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageSenderImplTest {

    @InjectMocks
    private MessageSenderImpl messageSenderImpl;

    @Mock
    private Socket socketMock;

    @Mock
    private OutputStream outputStreamMock;

    @Mock
    private InputStream inputStreamMock;

    @BeforeEach
    void setUp() throws Exception {
        lenient().when(socketMock.getOutputStream()).thenReturn(outputStreamMock);
        lenient().when(socketMock.getInputStream()).thenReturn(inputStreamMock);
    }

    @Test
    void testSendLogon()  {
        // Utilisation de MockedStatic pour mocker la création du Socket
        try (MockedStatic<Socket> mockedSocket = mockStatic(Socket.class)) {

            messageSenderImpl.sendLogon(10,1);

        }
    }

    @Test
    void testSendHeartbeat() throws Exception {
        messageSenderImpl.sendHeartbeat(socketMock);
        verify(outputStreamMock, times(1)).write(any(byte[].class), eq(0), anyInt());
    }

    @Test
    void testListenForMessages() throws Exception {
        when(inputStreamMock.read(any(byte[].class))).thenReturn(10).thenReturn(-1);
        messageSenderImpl.listenForMessages(socketMock);
        verify(inputStreamMock, times(2)).read(any(byte[].class));
    }
}
