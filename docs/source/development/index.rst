:orphan:

.. _development:

###########
Development
###########

How to profile import times
---------------------------

Install with development dependencies and then run:

.. code-block:: bash

   python -X importtime -m hpcflow.cli --run-time-info 2> import.log
   tuna import.log
