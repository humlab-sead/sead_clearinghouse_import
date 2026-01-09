from importer.configuration import ConfigStore


def test_config_store():
    store: ConfigStore = ConfigStore.configure_context(source="tests/test_data/config.yml")
    assert store
    assert store.context == "default"
    assert 'test' in store.data.keys()
