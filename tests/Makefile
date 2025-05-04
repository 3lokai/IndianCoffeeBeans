# Makefile for Indian Coffee Beans Scraper

.PHONY: test test-real test-mock clean lint

# Default Python command
PYTHON := python3

# Test directories
TEST_DIR := tests
RESULTS_DIR := test_results
LOGS_DIR := logs

# Test commands
TEST_RUNNER := $(TEST_DIR)/run_tests.py
TEST_REAL := $(TEST_DIR)/test_multiple_roasters.py
TEST_BLUE_TOKAI := $(TEST_DIR)/test_blue_tokai.py
TEST_MOCK := $(TEST_DIR)/test_helpers.py

# Default target
all: test

# Create necessary directories
$(RESULTS_DIR):
	mkdir -p $(RESULTS_DIR)

$(LOGS_DIR):
	mkdir -p $(LOGS_DIR)

# Run all tests (both real and mock)
test: $(RESULTS_DIR) $(LOGS_DIR)
	$(PYTHON) $(TEST_RUNNER) --mode both

# Run only real-world tests
test-real: $(RESULTS_DIR) $(LOGS_DIR)
	$(PYTHON) $(TEST_RUNNER) --mode real

# Run only mock tests
test-mock: $(RESULTS_DIR) $(LOGS_DIR)
	$(PYTHON) $(TEST_RUNNER) --mode mock

# Test with a single roaster (Blue Tokai)
test-blue-tokai: $(RESULTS_DIR) $(LOGS_DIR)
	$(PYTHON) $(TEST_BLUE_TOKAI)

# Clean up test results and logs
clean:
	rm -rf $(RESULTS_DIR)/*
	rm -rf $(LOGS_DIR)/*

# Lint the code with flake8
lint:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

# Install dependencies
install-deps:
	pip install -r requirements.txt

# Help target
help:
	@echo "Available targets:"
	@echo "  make test           - Run all tests (real and mock)"
	@echo "  make test-real      - Run only real-world tests"
	@echo "  make test-mock      - Run only mock tests"
	@echo "  make test-blue-tokai - Test with a single roaster (Blue Tokai)"
	@echo "  make clean          - Clean up test results and logs"
	@echo "  make lint           - Lint the code"
	@echo "  make install-deps   - Install dependencies"