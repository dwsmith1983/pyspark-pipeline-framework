Resilience
==========

pyspark-pipeline-framework provides two resilience patterns for handling
transient failures: **retry with exponential backoff** and **circuit breaker**.
Both can be configured per-component in HOCON or used programmatically.

Retry Policy
------------

Configure per-component retries with exponential backoff and optional jitter:

.. code-block:: javascript

   components: [
     {
       name: "flaky_source"
       component_type: source
       class_path: "my.module.FlakySource"
       retry {
         max_attempts: 3
         initial_delay_seconds: 1.0
         max_delay_seconds: 30.0
         backoff_multiplier: 2.0
       }
     }
   ]

**Retry parameters:**

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Parameter
     - Default
     - Description
   * - ``max_attempts``
     - 3
     - Maximum number of attempts (including the first)
   * - ``initial_delay_seconds``
     - 1.0
     - Delay before the first retry
   * - ``max_delay_seconds``
     - 60.0
     - Upper bound on delay between retries
   * - ``backoff_multiplier``
     - 2.0
     - Multiply delay by this factor after each retry

Programmatic Usage
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pyspark_pipeline_framework.core.resilience.retry import RetryExecutor

   executor = RetryExecutor(
       max_attempts=3,
       initial_delay=1.0,
       max_delay=30.0,
       backoff_multiplier=2.0,
   )

   result = executor.execute(my_function, args=(arg1,))

Circuit Breaker
---------------

Prevent repeated calls to a failing component. The circuit breaker tracks
consecutive failures and opens the circuit when a threshold is reached,
rejecting calls until a timeout expires:

.. code-block:: javascript

   components: [
     {
       name: "external_api"
       component_type: source
       class_path: "my.module.ApiSource"
       circuit_breaker {
         failure_threshold: 5
         timeout_seconds: 60.0
       }
     }
   ]

**Circuit breaker parameters:**

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Parameter
     - Default
     - Description
   * - ``failure_threshold``
     - 5
     - Consecutive failures before opening the circuit
   * - ``timeout_seconds``
     - 60.0
     - Seconds to wait before allowing a test request

**State machine:**

.. code-block:: text

   ┌──────────┐  failure_threshold  ┌──────────┐  timeout expires  ┌───────────┐
   │  CLOSED  │ ─────────────────→ │   OPEN   │ ────────────────→ │ HALF_OPEN │
   └──────────┘                     └──────────┘                    └───────────┘
       ↑                                                                │
       │                              success                          │
       └────────────────────────────────────────────────────────────────┘
                                      failure → back to OPEN

Programmatic Usage
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pyspark_pipeline_framework.core.resilience.circuit_breaker import (
       CircuitBreaker,
   )

   breaker = CircuitBreaker(failure_threshold=5, timeout_seconds=60.0)

   try:
       result = breaker.call(my_function)
   except CircuitBreakerOpenError:
       # Circuit is open, handle gracefully
       pass

Combining Retry and Circuit Breaker
------------------------------------

Use both together for maximum resilience. The runner applies retry first,
then circuit breaker:

.. code-block:: javascript

   components: [
     {
       name: "external_api"
       component_type: source
       class_path: "my.module.ApiSource"
       retry {
         max_attempts: 3
         initial_delay_seconds: 1.0
       }
       circuit_breaker {
         failure_threshold: 5
         timeout_seconds: 60.0
       }
     }
   ]

See Also
--------

- :doc:`/user-guide/hooks` - Lifecycle hooks for monitoring retries
- :doc:`/user-guide/checkpoint` - Resume failed pipelines
