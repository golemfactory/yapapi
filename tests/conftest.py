def pytest_addoption(parser):

    parser.addoption("--ya-api-key", type=str, help="instance api key", dest="yaApiKey")
