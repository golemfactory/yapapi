import factory

from yapapi.props import NodeInfo


class NodeInfoFactory(factory.Factory):
    class Meta:
        model = NodeInfo

    name = factory.Faker("pystr")
    subnet_tag = "devnet-beta.1"
