import os, sys
import tempfile

sys.path.append('src/')

import edit_config

# run with:
# python -m pytest tests/test_edit_config.py -v


def test_clear_collections_from_config():
    content = "some: value\nresources:\n  alpo is the best:\n    bar: 1\n"
    with tempfile.NamedTemporaryFile("w+", delete=False) as in_file, \
         tempfile.NamedTemporaryFile("r+", delete=False) as out_file:
        in_file.write(content)
        in_file.flush()
        edit_config.clear_collections_from_config(in_file.name, out_file.name)
        out_file.seek(0)
        result = out_file.read()
    assert "resources:" in result
    assert "alpo is the best:" not in result
    os.remove(in_file.name)
    os.remove(out_file.name)

def test_add_to_pygeoapi_config():
    template = "name: {NAME}\nage: {AGE}\n"
    params = {"{NAME}": "test", "{AGE}": "100"}
    with tempfile.NamedTemporaryFile("w+", delete=False) as template_file, \
         tempfile.NamedTemporaryFile("r+", delete=False) as out_file:
        template_file.write(template)
        template_file.flush()
        edit_config.add_to_pygeoapi_config(template_file.name, params, out_file.name)
        out_file.seek(0)
        result = out_file.read()
    assert "name: test" in result
    assert "age: 100" in result
    assert "NAME" not in result
    assert "AGE" not in result
    os.remove(template_file.name)
    os.remove(out_file.name)

def test_add_resources_to_config():
    with tempfile.NamedTemporaryFile("r+", delete=False) as out_file:
        db_path = "/tmp/db.json"
        edit_config.add_resources_to_config(out_file.name, db_path)
        out_file.seek(0)
        result = out_file.read()
    assert "occurrence-metadata:" in result
    assert "lajiapi-connection" in result
    assert db_path in result
    os.remove(out_file.name)