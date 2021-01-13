//
// Copyright (c) AIT Austrian Institute of Technology GmbH.
// Distributed under the terms of the Modified BSD License.
//

package at.ac.ait.lablink.clients.sync;

import at.ac.ait.lablink.core.connection.ILlConnection;
import at.ac.ait.lablink.core.connection.impl.LlConnectionFactory;
import at.ac.ait.lablink.core.ex.LlCoreRuntimeException;
import at.ac.ait.lablink.core.service.datapoint.IDataPointService;
import at.ac.ait.lablink.core.service.datapoint.consumer.IDataPointConsumerService;
import at.ac.ait.lablink.core.service.datapoint.impl.DataPointServiceManager;
import at.ac.ait.lablink.core.service.datapoint.impl.StringReadonlyDataPoint;
import at.ac.ait.lablink.core.service.sync.consumer.ISyncClientService;
import at.ac.ait.lablink.core.service.sync.impl.SyncServiceManager;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base class for sync host clients.
 *
 * <p>This class will instantiate all necessary controllers for the sync host client. It also 
 * registers a shutdown hook and terminates the sync host client the end of the session.
 */
public abstract class SyncHostBase extends Thread {

  /** Logger. */
  private static Logger logger = LogManager.getLogger(SyncHostBase.class);

  /** Configuration. */
  private Configuration config = null;

  /** Helper for thread synchronization. */
  private CountDownLatch shutdownLatch = new CountDownLatch(1);

  /** Termination monitor object for thread synchronisation (used for the shutdown hook). */
  private final Object terminationMonitor = new Object();

  /** Lablink datapoint service interface. */
  private IDataPointService dataPointService;
  
  /** Lablink datapoint consumer service interface. */
  private IDataPointConsumerService dataPointConsumerService;

  /** Lablink synchronization service interface. */
  private ISyncClientService syncClientService;

  /** Lablink connection interface. */
  protected ILlConnection lablinkConnection;

  StringReadonlyDataPoint clientStateDp;

  /**
   * Initialize the sync host client.
   *
   * @param lablinkConnection lablink connection interface
   * @param config client configuration
   */
  public abstract void initClient(ILlConnection lablinkConnection, Configuration config);

  /**
   * Perform any action that needs to be carried out after successfull connection to Lablink.
   */
  public abstract void afterBrokerConnect();

  /**
   * Perform any action that needs to be carried out before shutdown.
   */
  public abstract void shutdownClient();

  /**
   * Constructor.
   *
   * @param strConfigFileName file path and name of the config file
   */
  public SyncHostBase(String strConfigFileName) {
    logger.debug("SyncHostBase Constructor: Creating Controller");

    try {
      config = new PropertiesConfiguration(strConfigFileName);
    } catch (ConfigurationException ex) {
      throw new LlCoreRuntimeException("Can't load configuration.", ex);
    }

    // Retrieve configuration parameters.
    String prefix = config.getString("lablink.prefix");
    String appId = config.getString("lablink.appIdentifier");
    String groupId = config.getString("lablink.groupIdentifier");
    String clientId = config.getString("lablink.clientIdentifier");

    // Get Lablink connection controller implementation.
    lablinkConnection = LlConnectionFactory
        .getDefaultConnectionController(prefix, appId, groupId, clientId, config);

    // Get Lablink datapoint service implementation.
    dataPointService = DataPointServiceManager
        .getDataPointService(lablinkConnection, config);

    // Get Lablink datapoint consumer service implementation.
    dataPointConsumerService = DataPointServiceManager
        .getDataPointConsumerService(lablinkConnection, config);

    // Get Lablink sync client service implementation.
    syncClientService = SyncServiceManager
        .getSyncClientService(lablinkConnection, config);

    // Retrieve sync consumers from datapoint service and register them to the sync client service.
    syncClientService.registerSyncConsumer(dataPointService.getSyncConsumer());

    // Create new datapoint representing the clients' sync state.
    clientStateDp = new StringReadonlyDataPoint(
        Arrays.asList("clients", "status"),
        String.format("Client [{}/{}] status", groupId, clientId),
        "" );

    // Register new datapoint.
    dataPointService.registerDatapoint(clientStateDp);
  }

  /**
   * Main execution of the organisation controller class.
   */
  @Override
  public void run() {
    // Add shutdown hook.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        shutdown_client();
        try {
          shutdownLatch.await(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
          // This interuption is expected.
        }
      }
    });
    
    // Set the name of this thread.
    this.setName("SyncHostBase");

    logger.info("SyncHostBase is starting.");

    // Start all services.
    dataPointService.start();
    dataPointConsumerService.start();
    syncClientService.start();

    try {
      // Initialize the sync host client.
      this.initClient(lablinkConnection, config);

      // Connect to Lablink and set sync state to "INITIALIZING".
      lablinkConnection.connect();
      clientStateDp.setValue("INITIALIZING");

      // Perform any action that needs to be carried out after successfull connection to Lablink.
      this.afterBrokerConnect();

      // Set sync state to "RUNNING".
      clientStateDp.setValue("RUNNING");

      // Wait for termination.
      boolean quit = false;
      synchronized (this.terminationMonitor) {
        while (!quit) {
          try {
            this.terminationMonitor.wait();
          } catch (InterruptedException ex) {
            logger.debug(" interrupted");
          } finally {
            logger.debug(" performing shutdown");
            quit = true;
          }
        }
      }
      logger.info("SyncHostBase terminates internal controller.");

      // Perform any action that needs to be carried out before shutdown.
      this.shutdownClient();
    } catch (Exception ex) {
      logger.error("Exception during running the client is thrown.", ex);
    }

    // Set sync state to "STOPPED".
    if (lablinkConnection.isConnected()) {
      clientStateDp.setValue("STOPPED");
    }

    // Shut down services.
    dataPointService.shutdown();
    dataPointConsumerService.shutdown();
    syncClientService.shutdown();

    // Disconnect from Lablink.
    lablinkConnection.disconnect();
    lablinkConnection.shutdown();

    logger.info("Shutdown SyncHostBase");

    // Terminate thread.
    shutdownLatch.countDown();
  }

  /**
   * Termination of the main thread of the Lablink Starter.
   * It will shutdown all components in a series.
   */
  public void shutdown_client() {
    synchronized (this.terminationMonitor) {
      this.terminationMonitor.notify();
    }
  }
}
