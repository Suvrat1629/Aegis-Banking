package com.aegis.gateway.grpc;

import com.aegis.fraud.grpc.Fraud;
import com.aegis.fraud.grpc.FraudServiceGrpc;
import io.grpc.StatusRuntimeException;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;

@Service
public class FraudGrpcClient {

    @GrpcClient("fraud")
    private FraudServiceGrpc.FraudServiceBlockingStub fraudStub;

    public Fraud.FraudCheckResponse checkTransfer(String txnId, String from, String to, double amount,
                                                    String deviceId, String ipAddress) {
        Fraud.FraudCheckRequest request = Fraud.FraudCheckRequest.newBuilder()
                .setTransactionId(txnId)
                .setFromAccount(from)
                .setToAccount(to)
                .setAmount(amount)
                .setDeviceId(deviceId != null ? deviceId : "")
                .setIpAddress(ipAddress != null ? ipAddress : "")
                .build();

        try {
            return fraudStub.checkTransfer(request);
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC call to fraud-service failed: " + e.getStatus().getDescription(), e);
        }
    }
}
