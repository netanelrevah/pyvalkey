def pytest_addoption(parser):
    parser.addoption("--external", action="store_true")


def pytest_generate_tests(metafunc):
    option_value = metafunc.config.option.external
    if 'external' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("external", [option_value])