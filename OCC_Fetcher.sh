#!/bin/bash

###########################
# @file RegSHO_Tracker.sh #
###########################

# Navigate to the project directory + start the Virtual Environment
cd $HOME/Development/Projects/OCC_Fetcher/OCC_Fetcher || exit 1
#source ./venv/bin/activate

# Run the script + send Webhook Object to Discord
python3 OCC_Fetcher.py --discord




# *** Run the launchctl job manually ***
###launchctl kickstart -k gui/$(id -u)/com.you.OCC_Fetcher
