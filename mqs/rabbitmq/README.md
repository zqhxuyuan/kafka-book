
# QuickStart

## Server

```
➜  rabbitmq_server-3.6.11 sbin/rabbitmq-server
              RabbitMQ 3.6.11. Copyright (C) 2007-2017 Pivotal Software, Inc.
  ##  ##      Licensed under the MPL.  See http://www.rabbitmq.com/
  ##  ##
  ##########  Logs: /Users/zhengqh/Soft/rabbitmq_server-3.6.11/var/log/rabbitmq/rabbit@zqhmac.log
  ######  ##        /Users/zhengqh/Soft/rabbitmq_server-3.6.11/var/log/rabbitmq/rabbit@zqhmac-sasl.log
  ##########
              Starting broker...
 completed with 6 plugins.

➜  rabbitmq_server-3.6.11 sbin/rabbitmq-plugins enable rabbitmq_management
```

use guest/guest browse http://localhost:15672/#/

## Client

http://www.rabbitmq.com/tutorials/tutorial-one-java.html