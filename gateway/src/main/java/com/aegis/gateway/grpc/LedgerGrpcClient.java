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

    public boolean executeTransfer(String fromAccount, String toAccount, double amount) {
        Ledger.TransferRequest request = Ledger.TransferRequest.newBuilder()
                .setFromAccount(fromAccount)
                .setToAccount(toAccount)
                .setAmount(amount)
                .build();

        try {
            Ledger.TransferResponse response = ledgerStub.executeTransfer(request);
            return response.getSuccess();
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("gRPC call to ledger failed: " + e.getStatus().getDescription(), e);
        }
    }
}
