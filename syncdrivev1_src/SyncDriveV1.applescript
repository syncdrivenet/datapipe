-- SyncDrive V1 AppleScript Application
--
-- Expected folder structure:
--   any_folder/
--   ├── sessions/          <- put source session folders here
--   ├── processed/         <- processed output goes here
--   └── syncdrivev1_src/   <- this folder (export app here)
--       ├── SyncDriveV1.app
--       ├── syncdrivev1.py
--       └── ...

on run
	set appPath to (POSIX path of (path to me))
	set appDir to do shell script "dirname " & quoted form of appPath

	-- Handle both cases: app inside syncdrivev1_src or alongside it
	set srcDir to appDir
	if appDir does not end with "syncdrivev1_src" then
		set srcDir to appDir & "/syncdrivev1_src"
	end if
	set venvDir to srcDir & "/.venv"

	-- Check if first time setup needed
	set needsSetup to false
	try
		do shell script "test -d " & quoted form of venvDir
	on error
		set needsSetup to true
	end try

	if needsSetup then
		-- Run setup in Terminal so user can see progress
		tell application "Terminal"
			activate
			set setupScript to "cd " & quoted form of srcDir & " && echo '=== SyncDrive V1 Setup ===' && echo '' && echo 'Step 1/2: Creating Python environment...' && python3 -m venv .venv && echo 'Done.' && echo '' && echo 'Step 2/2: Installing dependencies...' && .venv/bin/pip install -r requirements.txt && echo '' && echo '=== Setup Complete! ===' && echo '' && echo 'Starting server...' && .venv/bin/python syncdrivev1.py &" & " && sleep 3 && open http://localhost:5050"
			do script setupScript
		end tell

		delay 2
		display dialog "SyncDrive V1 is setting up in Terminal." & return & return & "Safari will open when ready." & return & return & "After use, come back here to stop the server." buttons {"Stop Server"} default button "Stop Server"

	else
		-- Already set up - just start server
		try
			do shell script "curl -s http://localhost:5050 > /dev/null 2>&1"
			-- Already running
			tell application "Safari"
				activate
				open location "http://localhost:5050"
			end tell
			return
		end try

		-- Start server in background
		do shell script "cd " & quoted form of srcDir & " && .venv/bin/python syncdrivev1.py > /dev/null 2>&1 &"

		-- Wait for server
		repeat 15 times
			delay 0.5
			try
				do shell script "curl -s http://localhost:5050 > /dev/null 2>&1"
				exit repeat
			end try
		end repeat

		tell application "Safari"
			activate
			open location "http://localhost:5050"
		end tell

		display dialog "SyncDrive V1 is running." & return & return & "Click Stop when done." buttons {"Stop Server"} default button "Stop Server"
	end if

	-- Stop server
	try
		do shell script "pkill -f 'syncdrivev1.py'"
	end try
end run
