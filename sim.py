import sys
import yaml
import quickfix

from twisted.internet import reactor
from server import create_acceptor

def load_simulator_config(path):
    with open(path,'r') as stream:
        cfg = yaml.load(stream)

    return cfg

def main():
    try:
        acceptor = create_acceptor(sys.argv[1], sys.argv[2])
        acceptor.start()

        reactor.run()
    except (quickfix.ConfigError, quickfix.RuntimeError) as e:
        print(e)

if __name__ == "__main__":
    main()