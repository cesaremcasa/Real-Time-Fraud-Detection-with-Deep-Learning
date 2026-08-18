# Synthetic characterization fixtures

Every file in this directory is generated solely for the PR1 contract tests.
It contains no customer, taxi-provider, or production Kafka data. The values
are intentionally small, deterministic, and safe to publish. They are
dedicated to the public domain under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).

These fixtures exercise serialization, feature engineering, and fake broker
boundaries only. They do not claim a live Redpanda, API, worker, or end-to-end
transaction flow; those checks remain PR2/PR3 scope.
