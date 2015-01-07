FIXSIM
======

FIXSIM is an example project of FIX client and server implementations.
It can be used in production for testing your client or server and you might
find a need to add new feature or change it`s behavior. And FIXSIM can be used
just as extended example for implementing acceptors and initiators

Configuration
-------------

FIXSIM consists of two scripts fixsim-client.py and fixsim-server.py. Each
script use two configuration files, one for FIX Session settings, in ini format
and one for business logic, in YAML. You can find example configurations with
comments in project tree. For example fixsim-server may be started by command

```
python fixsim-server --acceptor_config fixsim-server.conf.ini --config
fixsim-server.conf.yaml
```

And fixsim-client may be started 

```
python fixsim-client --initiator_config fixsim-client.conf.ini --config
fixsim-client.conf.yaml
```
FIXSIM depends on twisted but you can easily purge it from poject by replacing reactor infinite loop by other infinite loop and implementing something like twisted.internet.task.LoopingCall which used for sending snapshots and subscribing to instruments

FIXSIM supports only FIX44 now  

Runtime
-------

FIXSIM business logic is pretty simple. Server receives client session and stores it. Client subscribes for one or more instruments (like EUR/USD, USD/CAD etc) and server begin sending market data snapshots to client. For each snapshot client can create new order(see skip_snapshot_chance attr in client yaml config) and send it to acceptor. For each such order acceptor with some chance(see reject rate in server yaml config) can create filled exection report.


