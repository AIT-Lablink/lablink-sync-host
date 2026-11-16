Maven project dependency
========================

The Lablink synchronization host's compiled Java package is available on the |MCR|_.
Use it in your local Maven_ setup by including the following dependency into your *pom.xml*:

.. code-block:: xml

   <dependency>
     <groupId>at.ac.ait.lablink.clients</groupId>
     <artifactId>sync</artifactId>
     <version>0.0.2</version>
   </dependency>

.. note:: You may have to adapt this snippet to use the latest version, please check the |MCR|_.

The dependency above is the library JAR.
To run the host from the command line, use the assembly *sync-<VERSION>-jar-with-dependencies.jar*, which is produced by the build below and published with the same coordinates and the classifier ``jar-with-dependencies``.

Installation from source
========================

Installation from source requires a local **Java Development Kit** installation, for instance the `Oracle Java SE Development Kit <https://www.oracle.com/java/technologies/downloads/>`_ or the `OpenJDK <https://openjdk.java.net/>`_.
The project compiles for Java 8, so JDK 8 or a newer JDK can be used.

Check out the project and compile it with Maven_:

.. code-block:: winbatch

   git clone https://github.com/AIT-Lablink/lablink-sync-host
   cd lablink-sync-host
   mvnw clean package

On Windows, run ``mvnw.cmd`` instead of ``mvnw``.

This will create the JAR file *sync-<VERSION>-jar-with-dependencies.jar* in subdirectory *target/assembly*.
The main class of that JAR is ``at.ac.ait.lablink.clients.sync.SimpleSyncHost``.

Docker image
============

A ready-to-run image is published on `Docker Hub <https://hub.docker.com/r/ait1/lablink-sync-host>`_ as ``ait1/lablink-sync-host:latest``:

.. code-block:: winbatch

   docker pull ait1/lablink-sync-host:latest

See :doc:`here <running>` for how to start the container.
Building the image locally with :github_blob:`docker/build.cmd <docker/build.cmd>` tags the result with the same name.


.. |MCR| replace:: Maven Central Repository
.. _MCR: https://search.maven.org/artifact/at.ac.ait.lablink.clients/sync
.. _Maven: https://maven.apache.org
