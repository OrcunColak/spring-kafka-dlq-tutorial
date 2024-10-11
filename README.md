# Read Me First

The original idea is from  
https://www.jimdoverse.com/the-power-of-handling-errors-using-a-dead-letter-topic-and-spring-2-3-6e2e719112f2

When Kafka listener throws an exception the message is routed to DLQ

# Reprocessing Messages from DLQ

To reprocess messages from the DLQ, we need to define another Kafka listener that consumes messages from the DLQ topic (
my-dlq-topic). In this listener, you can implement logic to retry processing the messages, apply any necessary fixes, or
perform different actions based on the failure reason.