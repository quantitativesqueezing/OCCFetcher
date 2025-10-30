#!/bin/bash

###########################
# @file RegSHO_Tracker.sh #
###########################

# Navigate to the project directory + start the Virtual Environment
cd $HOME/Development/Projects/RegSHO_Tracker/RegSHO_Tracker || exit 1
source ./venv/bin/activate

# Run the script + send Webhook Object to Discord
python3 RegSHO_Tracker.py --post-combined




# *** Run the launchctl job manually ***
###launchctl kickstart -k gui/$(id -u)/com.you.RegSHO_Tracker

