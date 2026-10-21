# Build analysis

## Observations

The controller reloads the queue-scoped Build, invokes the queue's issue detector, and persists each returned observation individually. Observations already stored for the same build, task, and unit within a queue are not persisted again, so repeated polls and retries after partial writes are safe. Detector results accompanying an error are discarded.

This controller only accumulates historical observations. An empty detection result neither deletes earlier observations nor establishes recovery. Open-issue projection, lifecycle history, and resolution are outside its scope.

## Polling and completion

Every delivery reloads the Build and its Request before detection. After a successful pass, a nonterminal Build holds the delivery for two minutes; a terminal Build completes the delivery after persistence. The status read before detection determines whether the pass holds: if the Build finishes during detection, another delivery is needed to analyze it after observing terminal status. More than one delivery may analyze the same terminal Build; observation persistence remains idempotent.

If the Request is terminal but the loaded Build is nonterminal, analysis reloads the Build before deciding to stop. The Build may have finished between the initial Build read and the Request read; reloading avoids skipping its final analysis. If it is still nonterminal, analysis stops without detection or a hold. This can occur when the status-polling DLQ handler fails the Request without learning the actual build result, leaving the Build at its last observed status.

Status polling and updates remain the responsibility of buildsignal. Analysis never changes Request state or releases build slots.

## Errors

Detection, persistence, configuration, state-loading, and message-validation errors are returned to the consumer for its normal retry or DLQ policy, whether the Build is running or terminal. Analysis errors do not use the two-minute polling hold. The consumer classifies errors and applies its configured retry limits and backoff; a final analysis can still fail and dead-letter.

The message and controller are defined without runtime producers or consumer registration. Host integration and detector implementations are separate work.
