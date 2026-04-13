package com.aegis.gateway.soap;

import com.aegis.gateway.grpc.LedgerGrpcClient;
import com.aegis.ledger.grpc.Ledger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;
import org.springframework.ws.transport.context.TransportContextHolder;
import org.springframework.ws.transport.http.HttpServletConnection;
import jakarta.servlet.http.HttpServletRequest;
import java.util.UUID;

@Endpoint
public class TransferEndpoint {

    private static final String NAMESPACE_URI = "http://aegis.com/banking";
    private final LedgerGrpcClient ledgerGrpcClient;

    @Autowired
    public TransferEndpoint(LedgerGrpcClient ledgerGrpcClient) {
        this.ledgerGrpcClient = ledgerGrpcClient;
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "PostTransferRequest")
    @ResponsePayload
    public PostTransferResponse handleTransfer(@RequestPayload PostTransferRequest request) {
        
        ObjectFactory objectFactory = new ObjectFactory();
        PostTransferResponse response = objectFactory.createPostTransferResponse();

        try {
            // 1. Extract Metadata from HTTP Transport
            HttpServletRequest httpRequest = null;
            var connection = TransportContextHolder.getTransportContext().getConnection();
            if (connection instanceof HttpServletConnection httpServletConnection) {
                httpRequest = httpServletConnection.getHttpServletRequest();
            }

            String ipAddress = httpRequest != null ? httpRequest.getRemoteAddr() : "unknown";
            String userAgent = httpRequest != null ? httpRequest.getHeader("User-Agent") : "unknown";
            String txnId = "AEGIS-" + UUID.randomUUID().toString().replace("-", "").toUpperCase();

            // 2. Call gRPC Client
            Ledger.TransferResponse grpcResponse = ledgerGrpcClient.executeTransfer(
                    txnId,
                    request.getFrom(),
                    request.getTo(),
                    request.getAmount(),
                    "SOAP-UI-CLIENT", // Device ID
                    ipAddress,
                    userAgent != null ? userAgent : "Unknown"
            );

            // 3. Map Response
            response.setSuccess(grpcResponse.getSuccess());
            // Optionally: If your PostTransferResponse has a setTransactionId, use it here:
            // response.setTransactionId(grpcResponse.getTransactionId());
            
            response.setMessage(grpcResponse.getSuccess()
                    ? "Transaction completed. ID: " + grpcResponse.getTransactionId()
                    : "Transaction failed: " + grpcResponse.getMessage());

        } catch (Exception e) {
            response.setSuccess(false);
            response.setMessage("Error processing transfer: " + e.getMessage());
        }

        return response;
    }
}