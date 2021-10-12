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

set "JUNIT_STANDALONE=%CD%\lib\junit-platform-console-standalone.jar"
for %%f in ("%CD%\lib\junit-platform-console-standalone-*.jar") do set "JUNIT_STANDALONE=%%~ff"
if not exist "%JUNIT_STANDALONE%" (
	echo File "%JUNIT_STANDALONE%" not found. Please check your JUNIT_STANDALONE and try again!
	pause
	exit /b 1
)

set "SQLITE_JDBC_LIBRARY=%CD%\lib\sqlite-jdbc-3.jar"
for %%f in ("%CD%\lib\sqlite-jdbc-3*.jar") do set "SQLITE_JDBC_LIBRARY=%%~ff"
if not exist "%SQLITE_JDBC_LIBRARY%" (
	echo File "%SQLITE_JDBC_LIBRARY%" not found. Please check your SQLITE_JDBC_LIBRARY and try again!
	pause
	exit /b 1
)

set "PATH=%ANT_HOME%\bin;%JAVA_HOME%\bin"

call "%ANT_HOME%\bin\ant.bat" clean compile-test
if not %ERRORLEVEL%==0 (
	echo Error: Something went wrong !!!
	pause
	exit /b 1
)

"%JAVA_HOME%\bin\java.exe" -jar "%JUNIT_STANDALONE%" -cp "%SQLITE_JDBC_LIBRARY%" -cp "%CD%\bin" --disable-ansi-colors --include-classname="(.*)SQLite(.+)Test$" --scan-classpath | "%CD%\etc\win32\buffer-x64.exe"

echo Completed.
pause
