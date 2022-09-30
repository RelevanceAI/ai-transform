## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type f -name "*.log" -delete
	find . -type f -name "*.logs" -delete
	find . -type f -name "*.coverage*" -delete
	find . -type f -name "*.temp" -delete
	find . -type d -name "*.coverage" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name "*.coverage" -exec rm -rf {} +
	find . -type d -name "*.eggs" -exec rm -rf {} +
	find . -type d -name "*.pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.mypy_cache" -exec rm -rf {} +
	find . -type d -empty -delete
