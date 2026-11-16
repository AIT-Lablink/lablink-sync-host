Invoking the host from the command line
=======================================

The runnable JAR is produced as described in :doc:`installation`.
The first argument is the URI of the host properties.
When it is omitted, the host reads ``LablinkSyncHost.properties`` from the working directory.

On Windows:

.. code-block:: winbatch

   SET SYNC_JAR=target\assembly\sync-<VERSION>-jar-with-dependencies.jar
   SET LLCONFIG=http://localhost:10101/get?id=
   SET CONFIG_URI=%LLCONFIG%ait.demo.sync-host.properties
   SET LOGGER_CONFIG=-Dlog4j.configurationFile=%LLCONFIG%ait.all.log4j2

   java %LOGGER_CONFIG% -jar %SYNC_JAR% %CONFIG_URI%

On Linux:

.. code-block:: bash

   SYNC_JAR=target/assembly/sync-<VERSION>-jar-with-dependencies.jar
   LLCONFIG='http://localhost:10101/get?id='
   CONFIG_URI="${LLCONFIG}ait.demo.sync-host.properties"
   LOGGER_CONFIG="-Dlog4j.configurationFile=${LLCONFIG}ait.all.log4j2"

   java "$LOGGER_CONFIG" -jar "$SYNC_JAR" "$CONFIG_URI"

The properties name the scenario with ``syncHost.scenario`` (the default is ``default``) and the scenario file with ``syncHost.syncScenarioFile.<scenario>``.
That value is a filesystem path.
A relative path is resolved from the working directory, so the JSON file has to sit there under the name given in the properties.

Running with Docker
===================

The image ``ait1/lablink-sync-host:latest`` (see :doc:`installation`) starts the host with the properties URI set to ``${LLCONFIG}${SYNC_CONFIG}``.
The defaults are ``LLCONFIG=http://lablink-config:10101/get?id=`` and ``SYNC_CONFIG=lablink-sync-host.properties``, so the image expects a configuration server reachable as the host ``lablink-config``.
Override both variables to point at another server.
The logger configuration is loaded from ``${LLCONFIG}ait.all.log4j2``.
``MEMORY_FLAG`` is the JVM heap setting and defaults to ``-Xmx512m``.

The working directory inside the container is ``/app``.
Mount the scenario JSON at the path named by ``syncHost.syncScenarioFile.<scenario>``.
With a properties entry ``syncHost.syncScenarioFile.default = scenario.json``, that path is ``/app/scenario.json``.

On Windows, a configuration server on the host is reachable as ``host.docker.internal``:

.. code-block:: winbatch

   docker run --rm -e LLCONFIG=http://host.docker.internal:10101/get?id= -e SYNC_CONFIG=ait.demo.sync-host.properties -v C:\path\to\scenario.json:/app/scenario.json ait1/lablink-sync-host:latest

On Linux, add that hostname explicitly:

.. code-block:: bash

   docker run --rm --add-host=host.docker.internal:host-gateway -e LLCONFIG=http://host.docker.internal:10101/get?id= -e SYNC_CONFIG=ait.demo.sync-host.properties -v /path/to/scenario.json:/app/scenario.json ait1/lablink-sync-host:latest
