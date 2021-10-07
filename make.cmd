@echo off
cd /d "%~dp0"

if not "%JDK17_PATH%"=="" (
	set "JAVA_HOME=%JDK17_PATH%"
)

if not exist "%JAVA_HOME%\bin\java.exe" (
	echo File "%JAVA_HOME%\bin\java.exe" not found. Please check your JAVA_HOME and try again!
	pause
	exit /b 1
)

if not exist "%ANT_HOME%\bin\ant.bat" (
	echo File "%ANT_HOME%\bin\ant.bat" not found. Please check your ANT_HOME and try again!
	pause
	exit /b 1
)

set "PATH=%ANT_HOME%\bin;%JAVA_HOME%\bin;%PATH%"

call "%ANT_HOME%\bin\ant.bat" %*

echo.

if not %ERRORLEVEL%==0 (
	echo Error: Something went wrong !!!
	pause
	exit /b 1
)

echo Completed.
pause
