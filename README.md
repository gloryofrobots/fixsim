FIXSIM
======

FIXSIM is an example project of FIX client and server implementations.
It can be used in production for testing your client or server and you might
find a need to add new feature or change it`s behavior. And FIXSIM can be used
just as example for implementing acceptors and initiators

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
`python fixsim-client --initiator_config fixsim-client.conf.ini --config
fixsim-client.conf.yaml
```

