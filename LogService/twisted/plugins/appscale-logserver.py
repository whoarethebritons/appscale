from zope.interface import implementer

from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker
from twisted.application import internet

from logserver import LogServerFactory


class Options(usage.Options):
    optParameters = [["port", "p", 7422, "The port number to listen on."],
                     ["path", "a", "/var/log/appscale/", "Path where logs are stored."],
                     ["size", "s", 25, "Size in GiB of retention of logs."]]


@implementer(IServiceMaker, IPlugin)
class MyServiceMaker(object):
    tapname = "appscale-logserver"
    description = "Holds track of appserver logs."

    options = Options

    def makeService(self, options):
        """
        Construct a TCPServer from a factory defined in myproject.
        """
        return internet.TCPServer(int(options["port"]), LogServerFactory(options["path"], int(options["size"])))


# Now construct an object which *provides* the relevant interfaces
# The name of this variable is irrelevant, as long as there is *some*
# name bound to a provider of IPlugin and IServiceMaker.

serviceMaker = MyServiceMaker()
