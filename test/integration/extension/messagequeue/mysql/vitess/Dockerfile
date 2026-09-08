FROM vitess/vttestserver:v23.0.5-mysql80@sha256:79653b39ca8511ebe225018f32090e061b93c0ac5d40596a3c3da5abf921e272

COPY platform/extension/messagequeue/mysql/schema/*.sql /vt/test-schema/submitqueue/
COPY platform/extension/messagequeue/mysql/vitess/vschema.json /vt/test-schema/submitqueue/vschema.json
