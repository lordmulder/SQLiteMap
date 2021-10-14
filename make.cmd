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

if not exist "%GIT_PATH%\bin\git.exe" (
	echo File "%GIT_PATH%\bin\git.exe" not found. Please check your GIT_PATH and try again!
	pause
	exit /b 1
)

set "PATH=%ANT_HOME%\bin;%JAVA_HOME%\bin;%GIT_PATH%\bin"

call "%ANT_HOME%\bin\ant.bat" %*
if not %ERRORLEVEL%==0 (
	echo Error: Something went wrong !!!
	pause
	exit /b 1
)

echo Completed.
