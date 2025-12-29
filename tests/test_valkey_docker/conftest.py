def pytest_addoption(parser):
    parser.addoption("--tag", action="store", default="tag name")
