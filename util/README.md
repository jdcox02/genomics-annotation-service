# GAS Utilities
This directory contains the following utility-related files:
* `helpers.py` - Miscellaneous helper functions
* `util_config.ini` - Common configuration options for all utility scripts


/notify
* `notify.py` - Sends notification email on completion of annotation job
* `notify_config.ini` - Configuration options for notification utility
* `run_notify.sh` - Runs the notifications utility script

/archive
* `archive_scipt.py` - Archives free user result files to Glacier using a script
* `archive_script_config.ini` - Configuration options for archive utility script
* `run_archive_scipt.sh` - Runs the archive script

/thaw
* `thaw_script.py` - Thaws an archived Glacier object using a script
* `thaw_script_config.ini` - Configuration options for thaw utility script
* `run_thaw_scipt.sh` - Runs the thaw script


/restore
* `restore.py` - The code for AWS Lambda function that restores thawed objects to S3
