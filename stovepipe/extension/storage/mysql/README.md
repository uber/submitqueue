# Schema upgrades

The SQL files in `schema/` define the schema for each revision. Consumers derive migrations by comparing the definitions at their deployed revision with those at the target revision, then apply the migrations before deploying the new application code. `CREATE TABLE IF NOT EXISTS` initializes fresh databases; it does not upgrade existing tables.

## Column order

Append every new column after all existing columns, before key and index declarations in the schema file. Preserve the order of existing columns. Migrations must use the default append behavior of `ADD COLUMN`, without `FIRST` or `AFTER` clauses. Add columns in the same order in schema definitions and migrations so fresh and upgraded databases have identical column order.

## Metadata updates

Metadata is namespaced extension state. Issue and request updates replace the complete mutable snapshot under the row's existing optimistic version guard. Preserve unrelated fields and namespaces, copy maps before mutation, and reload after a version conflict. Issue lifecycle reconciliation preserves metadata when an issue closes or reopens. Metadata-only request updates advance the request version; storage does not publish request history.
