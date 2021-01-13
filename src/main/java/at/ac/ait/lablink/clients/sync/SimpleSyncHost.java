//
// Copyright (c) AIT Austrian Institute of Technology GmbH.
// Distributed under the terms of the Modified BSD License.
//

package at.ac.ait.lablink.clients.sync;

import at.ac.ait.lablink.core.connection.ILlConnection;
import at.ac.ait.lablink.core.service.sync.impl.SyncHostServiceImpl;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple sync host client.
 *
 * <p>This sync host is a standalone Lablink client that provides the functionality to synchronize
 * the time between Lablink clients during a simulation run. It can load different scenarios and 
 * triggers the simulation execution.
 *
 * <p>It will call the implemented methods of the SyncConsumer interface of the connected Lablink 
 * clients. The data exchange between two or more Lablink clients that work in a simulation are not 
 * covered by this implementation. The data exchange will be executed asynchronously to the sync 
 * methods. Therefore, if a specially defined data exchange is necessary for a simulation, it has 
 * to be implemented by the simulation clients.
 */
public class SimpleSyncHost extends SyncHostBase {

  /** Logger. */
  private static Logger logger = LoggerFactory.getLogger(SimpleSyncHost.class);

  /** Synchronization service implementation. */
  private SyncHostServiceImpl syncHostService;

  /** Remote control for sync host. */
  private SyncHostRemoteControl syncHostRemoteControl;

  /** Flag indicating if the service should be started automatically. */
  private boolean automaticStart;

  /**
   * Constructor.
   *
   * @param strConfigFileName file path and name of the config file
   */
  public SimpleSyncHost(String strConfigFileName) {
    super(strConfigFileName);
  }


  /**
   * @see SyncHostBase#initClient(
   * at.ac.ait.lablink.core.connection.ILlConnection,
   * org.apache.commons.configuration.Configuration
   * )
   */
  public void initClient(ILlConnection lablinkConnection, Configuration config) {
    // Inititalize new sync host service.
    syncHostService = new SyncHostServiceImpl(lablinkConnection, config);

    // Inititalize new sync host remote control.
    syncHostRemoteControl = new SyncHostRemoteControl(lablinkConnection, syncHostService, config);

    if (config == null) {
      logger.info("No configuration set. Use default values");
      config = new BaseConfiguration();
    }

    // Retrieve scenario from config.
    String chosenScenario = config.getString("syncHost.scenario", "default");

    // Retrieve value for automatic start flag from config.
    this.automaticStart = config.getBoolean("syncHost.automaticStart", true);

    // In case of automatic start, initialize the sync host service for the selected scenario.
    if (automaticStart) {
      syncHostService.init(chosenScenario);
    }
  }

  /**
   * @see at.ac.ait.lablink.clients.sync.SyncHostBase#afterBrokerConnect
   */
  public void afterBrokerConnect() {
    // In case of automatic start, start the sync host service.
    if (automaticStart) {
      syncHostService.start();
    }
  }

  /**
   * @see at.ac.ait.lablink.clients.sync.SyncHostBase#shutdownClient
   */
  public void shutdownClient() {
    // In case of automatic start, shut down the sync host service.
    syncHostService.shutdown();
  }

  /**
   * Entry point for the simple sync host client.
   *
   * @param args The first element defines the configuration file.
   */
  public static void main(String[] args) {

    String configFile = "LablinkSyncHost.properties";
    if (args.length > 0) {
      configFile = args[0];
    } else {
      logger.info("No config file is given as command line argument.");
    }
    logger.info("Start simple sync host using configuration file '{}'", configFile);

    // Instantiate new sync host client.
    SimpleSyncHost client = new SimpleSyncHost(configFile);

    // Start the client.
    client.start();
  }
}
