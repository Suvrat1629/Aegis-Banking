package com.aegis.gateway.soap;

import com.aegis.gateway.grpc.LedgerGrpcClient;
import com.aegis.ledger.grpc.Ledger;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
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
    private final Tracer tracer;
    
    // Prometheus Metrics
    private final Counter transferRequestCounter;
    private final Counter transferSuccessCounter;
    private final Counter transferFailureCounter;
    private final Timer transferDurationTimer;

    @Autowired
    public TransferEndpoint(LedgerGrpcClient ledgerGrpcClient, Tracer tracer, MeterRegistry meterRegistry) {
        this.ledgerGrpcClient = ledgerGrpcClient;
        this.tracer = tracer;
        
        // Initialize metrics
        this.transferRequestCounter = Counter.builder("aegis.gateway.transfer.requests.total")
            .description("Total transfer requests received")
            .register(meterRegistry);
            
        this.transferSuccessCounter = Counter.builder("aegis.gateway.transfer.success.total")
            .description("Successful transfers")
            .register(meterRegistry);
            
        this.transferFailureCounter = Counter.builder("aegis.gateway.transfer.failure.total")
            .description("Failed transfers")
            .register(meterRegistry);
            
        this.transferDurationTimer = Timer.builder("aegis.gateway.transfer.duration")
            .description("Transfer processing time")
            .register(meterRegistry);
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "PostTransferRequest")
    @ResponsePayload
    public PostTransferResponse handleTransfer(@RequestPayload PostTransferRequest request) {
        transferRequestCounter.increment();
        
        ObjectFactory objectFactory = new ObjectFactory();
        PostTransferResponse response = objectFactory.createPostTransferResponse();
        
        // Start distributed trace
        Span span = tracer.spanBuilder("handle-soap-transfer")
            .setAttribute("transfer.from", request.getFrom())
            .setAttribute("transfer.to", request.getTo())
            .setAttribute("transfer.amount", request.getAmount())
            .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            
            return transferDurationTimer.record(() -> {
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

                    span.setAttribute("transaction.id", txnId);
                    span.setAttribute("client.ip", ipAddress);
                    span.setAttribute("client.user_agent", userAgent);

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

                    if (grpcResponse.getSuccess()) {
                        transferSuccessCounter.increment();
                        span.setAttribute("transfer.status", "success");
                    } else {
                        transferFailureCounter.increment();
                        span.setAttribute("transfer.status", "failure");
                        span.setAttribute("failure.reason", grpcResponse.getMessage());
                    }

                } catch (Exception e) {
                    transferFailureCounter.increment();
                    span.recordException(e);
                    span.setAttribute("transfer.status", "error");
                    
                    response.setSuccess(false);
                    response.setMessage("Error processing transfer: " + e.getMessage());
                }
                
                return response;
            });

        } finally {
            span.end();
        }
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "GetAccountBalanceRequest")
    @ResponsePayload
    public GetAccountBalanceResponse handleGetBalance(@RequestPayload GetAccountBalanceRequest request) {

        ObjectFactory objectFactory = new ObjectFactory();
        GetAccountBalanceResponse response = objectFactory.createGetAccountBalanceResponse();

        Span span = tracer.spanBuilder("get-account-balance")
            .setAttribute("account.id", request.getAccountId())
            .startSpan();

        try (Scope scope = span.makeCurrent()) {
            Ledger.BalanceResponse grpcResp = ledgerGrpcClient.getAccountBalance(request.getAccountId());

            response.setAccountId(grpcResp.getAccountId());
            response.setOwnerName(grpcResp.getOwnerName());
            response.setBalance(grpcResp.getBalance());
            response.setLastUpdated(grpcResp.getLastUpdated());

            span.setAttribute("balance", grpcResp.getBalance());

        } catch (Exception e) {
            span.recordException(e);
            response.setAccountId(request.getAccountId());
            response.setOwnerName("");
            response.setBalance(0.0);
            response.setLastUpdated("");
        } finally {
            span.end();
        }

        return response;
    }

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "GetAccountHistoryRequest")
    @ResponsePayload
    public GetAccountHistoryResponse handleGetHistory(@RequestPayload GetAccountHistoryRequest request) {

        ObjectFactory objectFactory = new ObjectFactory();
        GetAccountHistoryResponse response = objectFactory.createGetAccountHistoryResponse();

        Span span = tracer.spanBuilder("get-account-history")
            .setAttribute("account.id", request.getAccountId())
            .setAttribute("limit", request.getLimit())
            .startSpan();

        try (Scope scope = span.makeCurrent()) {
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

            span.setAttribute("entries.count", response.getEntries().size());

        } catch (Exception e) {
            span.recordException(e);
        } finally {
            span.end();
        }

        return response;
    }
}