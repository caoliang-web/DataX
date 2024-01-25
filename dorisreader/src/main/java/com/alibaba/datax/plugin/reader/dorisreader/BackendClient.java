package com.alibaba.datax.plugin.reader.dorisreader;


import org.apache.doris.sdk.thrift.*;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client to request Doris BE. */
public class BackendClient {
    private static Logger logger = LoggerFactory.getLogger(BackendClient.class);

    private Routing routing;

    private TDorisExternalService.Client client;
    private TTransport transport;

    private boolean isConnected = false;
    private final int retries;
    private final int socketTimeout;
    private final int connectTimeout;

    public BackendClient(Routing routing,  Keys options) {
        this.routing = routing;
        this.connectTimeout = options.getDorisRequestConnectTimeout();
        this.socketTimeout = options.getDorisRequestReadTimeout();
        this.retries = options.getRetries();
        logger.trace(
                "connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                this.connectTimeout,
                this.socketTimeout,
                this.retries);
        open();
    }

    private void open() {
        logger.debug("Open client to Doris BE '{}'.", routing);
        TException ex = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to connect {}.", attempt, routing);
            try {
                TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
                transport =
                        new TSocket(
                                new TConfiguration(),
                                routing.getHost(),
                                routing.getPort(),
                                socketTimeout,
                                connectTimeout);
                TProtocol protocol = factory.getProtocol(transport);
                client = new TDorisExternalService.Client(protocol);
                logger.trace(
                        "Connect status before open transport to {} is '{}'.",
                        routing,
                        isConnected);
                if (!transport.isOpen()) {
                    transport.open();
                    isConnected = true;
                    logger.info("Success connect to {}.", routing);
                    break;
                }
            } catch (TTransportException e) {
                logger.warn("Connect to doris {} failed.", routing, e);
                ex = e;
            }
        }
        if (!isConnected) {
            logger.error("Connect to doris {} failed.", routing);
            throw new DorisReadException(routing.toString(), ex);
        }
    }

    private void close() {
        logger.trace("Connect status before close with '{}' is '{}'.", routing, isConnected);
        isConnected = false;
        if ((transport != null) && transport.isOpen()) {
            transport.close();
            logger.info("Closed a connection to {}.", routing);
        }
        if (null != client) {
            client = null;
        }
    }

    /**
     * Open a scanner for reading Doris data.
     *
     * @param openParams thrift struct to required by request
     * @return scan open result
     *  @throws DorisReadException throw if cannot connect to Doris BE
     */
    public TScanOpenResult openScanner(TScanOpenParams openParams) {
        logger.debug("OpenScanner to '{}', parameter is '{}'.", routing, openParams);
        if (!isConnected) {
            open();
        }
        TException ex = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to openScanner {}.", attempt, routing);
            try {
                TScanOpenResult result = client.openScanner(openParams);
                if (result == null) {
                    logger.warn("Open scanner result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    logger.warn(
                            "The status of open scanner result from {} is '{}', error message is: {}.",
                            routing,
                            result.getStatus().getStatusCode(),
                            result.getStatus().getErrorMsgs());
                    continue;
                }
                return result;
            } catch (TException e) {
                logger.warn("Open scanner from {} failed.", routing, e);
                ex = e;
            }
        }
        logger.error("Connect to doris {} failed.", routing);
        throw new DorisReadException(routing.toString(), ex);
    }

    /**
     * get next row batch from Doris BE.
     *
     * @param nextBatchParams thrift struct to required by request
     * @return scan batch result
     * @throws DorisReadException throw if cannot connect to Doris BE
     */
    public TScanBatchResult getNext(TScanNextBatchParams nextBatchParams) {
        logger.debug("GetNext to '{}', parameter is '{}'.", routing, nextBatchParams);
        if (!isConnected) {
            open();
        }
        TException ex = null;
        TScanBatchResult result = null;
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to getNext {}.", attempt, routing);
            try {
                result = client.getNext(nextBatchParams);
                if (result == null) {
                    logger.warn("GetNext result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    logger.warn(
                            "The status of get next result from {} is '{}', error message is: {}.",
                            routing,
                            result.getStatus().getStatusCode(),
                            result.getStatus().getErrorMsgs());
                    continue;
                }
                return result;
            } catch (TException e) {
                logger.warn("Get next from {} failed.", routing, e);
                ex = e;
            }
        }
        if (result != null && (TStatusCode.OK != (result.getStatus().getStatusCode()))) {
            logger.error(
                    "Doris server '{}' internal failed, status is '{}', error message is '{}'",
                    routing,
                    result.getStatus().getStatusCode(),
                    result.getStatus().getErrorMsgs());
            throw new DorisReadException(
                    routing.toString(),
                    result.getStatus().getStatusCode(),
                    result.getStatus().getErrorMsgs());
        }
        logger.error("Connect to doris {} failed.", routing);
        throw new DorisReadException(routing.toString(), ex);
    }

    /**
     * close an scanner.
     *
     * @param closeParams thrift struct to required by request
     */
    public void closeScanner(TScanCloseParams closeParams) {
        logger.debug("CloseScanner to '{}', parameter is '{}'.", routing, closeParams);
        for (int attempt = 0; attempt < retries; ++attempt) {
            logger.debug("Attempt {} to closeScanner {}.", attempt, routing);
            try {
                TScanCloseResult result = client.closeScanner(closeParams);
                if (result == null) {
                    logger.warn("CloseScanner result from {} is null.", routing);
                    continue;
                }
                if (!TStatusCode.OK.equals(result.getStatus().getStatusCode())) {
                    logger.warn(
                            "The status of get next result from {} is '{}', error message is: {}.",
                            routing,
                            result.getStatus().getStatusCode(),
                            result.getStatus().getErrorMsgs());
                    continue;
                }
                break;
            } catch (TException e) {
                logger.warn("Close scanner from {} failed.", routing, e);
            }
        }
        logger.info("CloseScanner to Doris BE '{}' success.", routing);
        close();
    }
}
