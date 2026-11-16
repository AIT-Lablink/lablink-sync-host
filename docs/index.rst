****************************
Lablink synchronization host
****************************

.. meta::
   :description lang=en: Lablink synchronization host

This package provides the Lablink synchronization host.
The host is a standalone Lablink client that keeps the clients of a simulation on a common time.
It loads a scenario and starts the run.
Each connected client implements a sync consumer, and the host calls that consumer at every synchronization step.
Datapoint values move between clients on their own, asynchronously to these sync steps.

Installation
============

Find information about the installation of the Lablink synchronization host :doc:`here <installation>`.

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Installation

   /installation

Running the host
================

Find basic instructions for running the host :doc:`here <running>`.

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Running the host

   /running
