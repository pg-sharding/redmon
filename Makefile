.PHONY: test build

build:
	python3 -m py_compile spqr_monitor.py
	python3 -m py_compile test_spqr_monitor.py
	python3 -m py_compile test_integration.py

test:
	python3 -m unittest discover -s . -p "test_*.py" -v
