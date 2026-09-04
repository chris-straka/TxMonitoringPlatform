check "topic_names_are_distinct" {
  assert {
    condition = length(toset([
      var.events_topic,
      var.alerts_topic,
      var.dead_letter_topic,
    ])) == 3
    error_message = "events_topic, alerts_topic, and dead_letter_topic must be distinct."
  }
}

# Production-shaped durability: replication factor 3 with min.insync.replicas=2, producers on
# Acks.All with idempotence enabled (application code). var.min_insync_replicas is validated
# against replication_factor so single-broker dev clusters (replication_factor=1) still work.
resource "kafka_topic" "transaction_events" {
  name               = var.events_topic
  partitions         = var.partitions
  replication_factor = var.replication_factor

  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "604800000" # 7 days
    "min.insync.replicas" = tostring(var.min_insync_replicas)
  }
}

# Postgres is the decision record. Delete retention keeps a bounded alert history; never use
# log compaction here, which would silently collapse an account's alert history.
resource "kafka_topic" "risk_alerts" {
  name               = var.alerts_topic
  partitions         = var.partitions
  replication_factor = var.replication_factor

  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "604800000" # 7 days
    "min.insync.replicas" = tostring(var.min_insync_replicas)
  }
}

# Poison Kafka records must be published durably before detection commits their source offset.
# A delete policy keeps forensic evidence for a bounded period instead of growing without limit.
resource "kafka_topic" "transaction_events_dead_letter" {
  name               = var.dead_letter_topic
  partitions         = var.partitions
  replication_factor = var.replication_factor

  config = {
    "cleanup.policy"      = "delete"
    "retention.ms"        = "2592000000" # 30 days, longer than the source topic
    "min.insync.replicas" = tostring(var.min_insync_replicas)
  }
}
