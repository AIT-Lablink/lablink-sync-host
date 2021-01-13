//
// Copyright (c) AIT Austrian Institute of Technology GmbH.
// Distributed under the terms of the Modified BSD License.
//

package at.ac.ait.lablink.clients.sync;

import at.ac.ait.lablink.core.connection.ILlConnection;
import at.ac.ait.lablink.core.connection.encoding.encodables.Header;
import at.ac.ait.lablink.core.connection.encoding.encodables.IPayload;
import at.ac.ait.lablink.core.connection.rpc.RpcHeader;
import at.ac.ait.lablink.core.connection.rpc.request.IRpcRequestCallback;
import at.ac.ait.lablink.core.connection.topic.MsgSubject;
import at.ac.ait.lablink.core.connection.topic.RpcSubject;
import at.ac.ait.lablink.core.payloads.ErrorMessage;
import at.ac.ait.lablink.core.payloads.StatusMessage;
import at.ac.ait.lablink.core.payloads.StringMessage;
import at.ac.ait.lablink.core.service.sync.ELlSyncHostState;
import at.ac.ait.lablink.core.service.sync.ISyncHostNotifier;
import at.ac.ait.lablink.core.service.sync.ex.SyncServiceRuntimeException;
import at.ac.ait.lablink.core.service.sync.impl.SyncHostServiceImpl;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Remote control functionality for the sync host client.
 *
 * <p>This interface provides a Lablink message with the current state of the sync host. It will
 * also provide an RPC call to start a simulation with a defined scenario.
 */
public class SyncHostRemoteControl {

  /** Logger. */
  private Logger logger = LoggerFactory.getLogger(SyncHostRemoteControl.class);

  /** Interface to Lablink connection. */
  private ILlConnection lablinkConnection;

  /** Lablink synchronization host service implementation. */
  private SyncHostServiceImpl syncHostService;

  /** Interface to synchronization host notifier. */
  private SyncHostNotifierImpl syncNotifier;

  /** Subject for synchronization state messages. */
  private MsgSubject msgStateSubject;

  /** Timeout. */
  private long stopTimeout;

  /** Flag for waiting. */
  private boolean stopWaitingFlag = false;

  /** Mmonitor object for thread synchronisation (used for waiting). */
  private final Object stopWaitingMonitor = new Object();

  /** Executor service. */
  private ScheduledExecutorService stopWaitExecutor = Executors.newSingleThreadScheduledExecutor();

  /**
   * Constructor.
   *
   * @param lablinkConnection Lablink connection interface
   * @param syncHostService sync host service implementation to be controlled remotely
   * @param config configuration for the remote interface
   */
  public SyncHostRemoteControl(
      ILlConnection lablinkConnection,
      SyncHostServiceImpl syncHostService,
      Configuration config
  ) {
    if (config == null) {
      logger.info("No configuration set, use default values.");
      config = new BaseConfiguration();
    }

    // Retrieve timeout config parameter.
    stopTimeout = config.getLong("syncHost.stopTimeout", 10000);

    this.lablinkConnection = lablinkConnection;
    this.syncHostService = syncHostService;

    // Instantiate synchronization host notifier implementation.
    syncNotifier = new SyncHostNotifierImpl();
    this.syncHostService.setSyncHostNotifier(syncNotifier);
    
    // Register handlers.
    registerHandlers();
  }

  /**
   * Register handlers.
   */
  private void registerHandlers() {

    msgStateSubject =
        MsgSubject.getBuilder().addSubjectElement("syncHost").addSubjectElement("state").build();

    RpcSubject startSubject =
        RpcSubject.getBuilder().addSubjectElement("syncHost").addSubjectElement("control")
            .addSubjectElement("start").build();

    lablinkConnection.registerRequestHandler(startSubject, new SyncHostStartRequestHandler());

    RpcSubject stopSubject =
        RpcSubject.getBuilder().addSubjectElement("syncHost").addSubjectElement("control")
            .addSubjectElement("stop").build();

    lablinkConnection.registerRequestHandler(stopSubject, new SyncHostStopRequestHandler());
  }

  /**
   * Base class for dedicated RPC request callbacks for controlling a sync host client.
   */
  private abstract class SyncHostControlHandler implements IRpcRequestCallback {

    @Override
    public void handleError(Header header, List<ErrorMessage> errors) throws Exception {
      logger.error("SyncControlRPC error from [{}/{}]. Error-Header: {} " + "Errors: {}",
          header.getSourceGroupId(), header.getSourceClientId(), header, errors);
    }
  }

  /**
   * RPC callback for start requests.
   */
  private class SyncHostStartRequestHandler extends SyncHostControlHandler {

    @Override
    public List<IPayload> handleRequest(RpcHeader header, List<IPayload> payloads) {

      logger.info("Received a start command from [{}/{}]", header.getSourceGroupId(),
          header.getSourceClientId());

      IPayload returnValue = null;

      if (payloads.size() < 1) {
        returnValue =
            new StatusMessage(StatusMessage.StatusCode.NOK,
                "No payloads was given in request. Expect one payloads object.");
      }

      if (!(payloads.get(0) instanceof StringMessage)) {
        returnValue =
            new StatusMessage(StatusMessage.StatusCode.NOK,
                "A wrong payloads type was given for the message. Expected '" + StringMessage
                    .getClassType() + "', received '" + payloads.get(0).getType() + "'");
      }

      String scenario = ((StringMessage) payloads.get(0)).getValue();

      if (syncHostService.getHostState() == ELlSyncHostState.INIT
          || syncHostService.getHostState() == ELlSyncHostState.SIMULATING) {
        returnValue =
            new StatusMessage(StatusMessage.StatusCode.NOK,
                "The sync host already runs a simulation.");
      }

      synchronized (stopWaitingMonitor) {
        if (stopWaitingFlag) {
          returnValue =
              new StatusMessage(StatusMessage.StatusCode.NOK, "A stop process is already running.");
        }
      }

      if (returnValue == null) {
        try {
          syncHostService.init(scenario);
          syncHostService.start();
          returnValue = new StatusMessage(StatusMessage.StatusCode.OK);
        } catch (SyncServiceRuntimeException ex) {
          returnValue = new StatusMessage(StatusMessage.StatusCode.NOK, ex.getMessage());
        }
      }

      return Collections.singletonList(returnValue);
    }

  }

  /**
   * RPC callback for stop requests.
   */
  private class SyncHostStopRequestHandler extends SyncHostControlHandler {

    @Override
    public List<IPayload> handleRequest(RpcHeader header, List<IPayload> payloads) {

      logger.info("Received a stop command from [{}/{}]", header.getSourceGroupId(),
          header.getSourceClientId());

      IPayload returnValue = null;

      synchronized (stopWaitingMonitor) {
        if (stopWaitingFlag) {
          returnValue =
              new StatusMessage(StatusMessage.StatusCode.NOK, "A stop process is already running.");
        }

        stopWaitingFlag = true;
        stopWaitExecutor.schedule(new Runnable() {
          @Override
          public void run() {
            synchronized (stopWaitingMonitor) {
              stopWaitingFlag = false;
              syncNotifier.stateChanged(ELlSyncHostState.STOPPED);
            }
          }
        }, stopTimeout, TimeUnit.MILLISECONDS);
      }

      if (returnValue == null) {
        syncHostService.shutdown();
        returnValue = new StatusMessage(StatusMessage.StatusCode.OK);
      }
      return Collections.singletonList(returnValue);
    }
  }

  /**
  * Implementation of synchronization host notifier interface.
  */
  private class SyncHostNotifierImpl implements ISyncHostNotifier {

    @Override
    public void stateChanged(ELlSyncHostState state) {

      IPayload statePayload;
      if (state == ELlSyncHostState.STOPPED && stopWaitingFlag) {
        statePayload = new StringMessage("WAITING_TO_STOP");
      } else {
        statePayload = new StringMessage(state.toString());
      }

      IPayload participantsNumberPayload =
          new StringMessage(Integer.toString(syncHostService.getRegisteredClients().size()));

      lablinkConnection
          .publishMessage(msgStateSubject, Arrays.asList(statePayload, participantsNumberPayload));
    }
  }
}
