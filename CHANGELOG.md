# Changelog


## 0.6.0
  * Validate API credentials before discovery and sync; raise `InvalidCredentialsError` on HTTP 401/403
  * Bump kafka-python 2.3.2 → 3.0.9
  * Bump black 20.8b1 → 4.0.6
  * Bump pylint 3.3.4 → 9.1.1
  * Bump tox 3.20.1 → 7.1.0
  * Pin mypy 2.3.0, pytest 9.1.1, pytest-cov 7.1.0, pytest-xdist 3.8.0, vcrpy 8.3.0
  * [#31](https://github.com/singer-io/tap-ordway/pull/31)


## 0.5.0
  * Update metadata generation to include parent-tap-stream-id for substreams
  * Add forced-replication-method support [#27](https://github.com/singer-io/tap-ordway/pull/27)


# 0.4.6
  * Bump requests to 2.33.0 for security updates [#28](https://github.com/singer-io/tap-ordway/pull/28)


## 0.4.5
  * Updated missing fields from below Modules 
  * Charges, Invoices, Orders, Payments, Plans, Refunds, Subscriptions, Billing runs, Billing Schedules, Chart of accounts, Credits, Customers, Payment runs, Products, Revenue Schedules, Webhooks

  * https://github.com/singer-io/tap-ordway/pull/21


## 0.4.4
  * Add replication-keys and primary-keys in the schema of journal_entries. [#26](https://github.com/singer-io/tap-ordway/pull/26)

## 0.4.3
  * Update Product to have transaction posting entries
  * Update Subscription to have transaction posting entries
  * Add Journal Entries Schema [#18](https://github.com/singer-io/tap-ordway/pull/18)


## 0.4.2
  * Revert v0.4.1

## 0.4.1
  * Updated Product to have transaction posting entries
  * Updated Subscription to have transaction posting entries
  * Added Journal Entries Schema [#18](https://github.com/singer-io/tap-ordway/pull/18)

## 0.4.0
  * Library version upgrades [#19](https://github.com/singer-io/tap-ordway/pull/19)

## 0.3.0
  * Add support for new Stream DebitMemo [#17](https://github.com/singer-io/tap-ordway/pull/17)

## 0.2.0
  * Fix JSON Schema validator in CircleCI [#7](https://github.com/singer-io/tap-ordway/pull/7)
  * Remove versions from Incremental Streams [#9](https://github.com/singer-io/tap-ordway/pull/9)
  * Change when Full Table Streams send an `ACTIVATE_VERSION` message [#9](https://github.com/singer-io/tap-ordway/pull/9)

## 0.1.2
  * Fixed the schema transform issues

## 0.1.1
  * Increased API timeout to 30 seconds from 4 seconds

## 0.1.0
  * Initial commit
