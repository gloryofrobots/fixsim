import sys
import yaml
from twisted.internet import reactor

import quickfix

from fixsim.server import create_acceptor


def load_simulator_config(path):
    with open(path, 'r') as stream:
        cfg = yaml.load(stream)

    return cfg


def print_usage():
    print "usage: python fixsim-server.py path_to_fix_config path_to_server_config"


def main(args):
    print args
    if len(args) != 2:
        return print_usage()
    try:
        acceptor = create_acceptor(args[0], args[1])
        acceptor.start()

        reactor.run()
    except (quickfix.ConfigError, quickfix.RuntimeError) as e:
        print(e)


if __name__ == "__main__":
    main(sys.argv[1:])
