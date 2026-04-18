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
            response.setTransactionId(grpcResponse.getTransactionId());
            response.setMessage(grpcResponse.getSuccess()
                    ? "Transaction completed. ID: " + grpcResponse.getTransactionId()
                    : "Transaction failed: " + grpcResponse.getMessage());

        } catch (Exception e) {
            response.setSuccess(false);
            response.setMessage("Error processing transfer: " + e.getMessage());
        }

        return response;
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "GetAccountBalanceRequest")
    @ResponsePayload
    public GetAccountBalanceResponse handleGetBalance(@RequestPayload GetAccountBalanceRequest request) {

        ObjectFactory objectFactory = new ObjectFactory();
        GetAccountBalanceResponse response = objectFactory.createGetAccountBalanceResponse();

        try {
            Ledger.BalanceResponse grpcResp = ledgerGrpcClient.getAccountBalance(request.getAccountId());

            response.setAccountId(grpcResp.getAccountId());
            response.setOwnerName(grpcResp.getOwnerName());
            response.setBalance(grpcResp.getBalance());
            response.setLastUpdated(grpcResp.getLastUpdated());

        } catch (Exception e) {
            response.setAccountId(request.getAccountId());
            response.setOwnerName("");
            response.setBalance(0.0);
            response.setLastUpdated("");
            // TODO: return a SOAP fault depending on requirements
        }

        return response;
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "GetAccountHistoryRequest")
    @ResponsePayload
    public GetAccountHistoryResponse handleGetHistory(@RequestPayload GetAccountHistoryRequest request) {

        ObjectFactory objectFactory = new ObjectFactory();
        GetAccountHistoryResponse response = objectFactory.createGetAccountHistoryResponse();

        try {
        int limit = request.getLimit();
        if (limit <= 0) {
        limit = 10;
        }
        Ledger.HistoryResponse grpcResp = ledgerGrpcClient.getAccountHistory(
            request.getAccountId(),
            limit
        );

            // Map each gRPC TransactionEntry to the JAXB TransactionEntry
            for (Ledger.TransactionEntry ge : grpcResp.getEntriesList()) {
                TransactionEntry je = objectFactory.createTransactionEntry();
                je.setTransactionId(ge.getTransactionId());
                je.setAmount(ge.getAmount());
                je.setEntryType(ge.getEntryType());
                je.setDescription(ge.getDescription());
                je.setCreatedAt(ge.getCreatedAt());
                response.getEntries().add(je);
            }

        } catch (Exception e) {
            // on error return empty history for now; consider SOAP fault
        }

        return response;
    }

    // Helper to get HTTP request from Spring-WS context
    private HttpServletRequest getHttpServletRequest() {
        var connection = TransportContextHolder.getTransportContext().getConnection();
        if (connection instanceof HttpServletConnection httpConn) {
            return httpConn.getHttpServletRequest();
        }
        return null;
    }
}