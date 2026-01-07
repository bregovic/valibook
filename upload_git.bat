@echo off
echo ========================================================
echo  Automaticke nahravani projektu na GitHub (valibook)
echo ========================================================

:: Ziskani data a casu pro commit zpravu
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set datetime=%%I
set commit_msg=Auto-update %datetime:~0,4%-%datetime:~4,2%-%datetime:~6,2% %datetime:~8,2%:%datetime:~10,2%

echo 1. Pridavam zmenene subory (git add)...
git add .

echo 2. Vytvarim commit (git commit)...
git commit -m "%commit_msg%"

echo 3. Odesilam na GitHub (git push)...
git push -u origin main

echo.
echo Hotovo!
timeout /t 5
