import importlib.util, pathlib
p=pathlib.Path(r'c:\Project p\SQRS\webapp\app.py')
spec=importlib.util.spec_from_file_location('temp_app', p)
mod=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
print(mod.app.url_map)
