@echo off
REM Windows Batch script for running Indian Coffee Beans Scraper tests

REM Default Python command (using virtual environment if activated)
SET PYTHON=python

REM Test directories
SET TEST_DIR=tests
SET RESULTS_DIR=test_results
SET LOGS_DIR=logs

REM Create directories if they don't exist
if not exist %RESULTS_DIR% mkdir %RESULTS_DIR%
if not exist %LOGS_DIR% mkdir %LOGS_DIR%

REM Parse command line arguments
SET MODE=both
SET ARG=%1

if "%ARG%"=="test" (
    echo Running all tests...
    %PYTHON% %TEST_DIR%\run_tests.py --mode both
) else if "%ARG%"=="test-real" (
    echo Running real-world tests...
    %PYTHON% %TEST_DIR%\run_tests.py --mode real
) else if "%ARG%"=="test-mock" (
    echo Running mock tests...
    %PYTHON% %TEST_DIR%\run_tests.py --mode mock
) else if "%ARG%"=="test-blue-tokai" (
    echo Testing with Blue Tokai...
    %PYTHON% %TEST_DIR%\test_blue_tokai.py
) else if "%ARG%"=="clean" (
    echo Cleaning test results and logs...
    del /Q %RESULTS_DIR%\*
    del /Q %LOGS_DIR%\*
) else if "%ARG%"=="help" (
    echo Available commands:
    echo   run_tests.bat test           - Run all tests (real and mock)
    echo   run_tests.bat test-real      - Run only real-world tests
    echo   run_tests.bat test-mock      - Run only mock tests
    echo   run_tests.bat test-blue-tokai - Test with a single roaster (Blue Tokai)
    echo   run_tests.bat clean          - Clean up test results and logs
) else (
    echo Running all tests (default)...
    %PYTHON% %TEST_DIR%\run_tests.py --mode both
)

echo Test run completed.