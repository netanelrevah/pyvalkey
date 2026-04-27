def pytest_addoption(parser):
    parser.addoption("--tag", action="store", default="tag name")
    parser.addoption("--only", action="store", default="")
