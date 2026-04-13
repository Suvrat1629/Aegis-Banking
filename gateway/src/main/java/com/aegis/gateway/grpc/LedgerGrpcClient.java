package com.aegis.gateway.grpc;

import com.aegis.ledger.grpc.Ledger;
import com.aegis.ledger.grpc.LedgerServiceGrpc;
import io.grpc.StatusRuntimeException;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;

@Service
public class LedgerGrpcClient {


    @GrpcClient("ledger")
    private LedgerServiceGrpc.LedgerServiceBlockingStub ledgerStub;

public Ledger.TransferResponse executeTransfer(String txnId, String from, String to, double amount, 
                                                   String deviceId, String ipAddress, String userAgent) {
        Ledger.TransferRequest request = Ledger.TransferRequest.newBuilder()
                .setTransactionId(txnId)
                .setFromAccount(from)
                .setToAccount(to)
                .setAmount(amount)
                .setDeviceId(deviceId != null ? deviceId : "")
                .setIpAddress(ipAddress != null ? ipAddress : "")
                .setUserAgent(userAgent != null ? userAgent : "")
                .build();

        try {
            return ledgerStub.executeTransfer(request);
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC call to ledger failed: " + e.getStatus().getDescription(), e);
        }
    }
}
