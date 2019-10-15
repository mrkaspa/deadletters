# Conclusions

Message when is rejected by using Nack on the consumer

```
{Acknowledger:0xc0000f8480 Headers:map[x-death:[map[count:1 exchange:logs queue:logs-queue reason:rejected routing-keys:[] time:2019-08-02 16:52:02 -0500 -05]] x-first-death-exchange:logs x-first-death-queue:logs-queue x-first-death-reason:rejected] ContentType:text/plain ContentEncoding: DeliveryMode:0 Priority:0 CorrelationId: ReplyTo: Expiration: MessageId: Timestamp:0001-01-01 00:00:00 +0000 UTC Type: UserId: AppId: ConsumerTag:ctag-/tmp/go-build127540106/b001/exe/main-2 MessageCount:0 DeliveryTag:5 Redelivered:false Exchange:my-dlx RoutingKey: Body:[104 101 108 108 111]}
```

Message when is retried and expires by ttl

```
{Acknowledger:0xc000106000 Headers:map[x-death:[map[count:1 exchange:logs queue:logs-listener reason:expired routing-keys:[] time:2019-08-02
 15:58:01 -0500 -05]] x-first-death-exchange:logs x-first-death-queue:logs-listener x-first-death-reason:expired] ContentType:text/plain Con
tentEncoding: DeliveryMode:0 Priority:0 CorrelationId: ReplyTo: Expiration: MessageId: Timestamp:0001-01-01 00:00:00 +0000 UTC Type: UserId:
 AppId: ConsumerTag:ctag-/tmp/go-build918149678/b001/exe/main-1 MessageCount:0 DeliveryTag:1 Redelivered:false Exchange:my-dlx RoutingKey: B
ody:[104 101 108 108 111]}
```
