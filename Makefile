# Fabric Utilities Makefile

.PHONY: test

test:
	@echo Running doctests...
	@uv sync --quiet
	@uv pip install -e . --quiet

	@echo Testing write.py...
	@uv run python -m doctest src/fabric_utilities/write.py -v

	@echo [PASS] All unit tests passed!

	@echo Cleaning up test artifacts...
	@uv run python -c "import shutil, os; [shutil.rmtree(os.path.join(root, d), ignore_errors=True) for root, dirs, files in os.walk('.') for d in dirs if d.endswith('.egg-info')]"
	@uv run python -c "import shutil, os; [shutil.rmtree(os.path.join(root, d), ignore_errors=True) for root, dirs, files in os.walk('.') for d in dirs if d == '__pycache__']"
	@echo [DONE] Tests completed and cleaned up!
