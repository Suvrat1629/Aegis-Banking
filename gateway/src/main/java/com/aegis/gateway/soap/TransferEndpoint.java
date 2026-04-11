package com.aegis.gateway.soap;

import com.aegis.gateway.grpc.LedgerGrpcClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;

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

        ObjectFactory factory = new ObjectFactory();
        PostTransferResponse response = factory.createPostTransferResponse();

        try {
            boolean success = ledgerGrpcClient.executeTransfer(
                    request.getFrom(),
                    request.getTo(),
                    request.getAmount()
            );

            response.setSuccess(success);
            response.setMessage(success
                    ? "Transaction completed successfully"
                    : "Transaction failed due to insufficient balance or invalid account");

        } catch (Exception e) {
            response.setSuccess(false);
            response.setMessage("Error processing transfer: " + e.getMessage());
        }

        return response;
    }
}