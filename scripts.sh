#!/bin/sh
ACTION=run
FLASK_APP=play.py
FLASK_ENV=development

# export flask variables
export FLASK_APP=$FLASK_APP
export FLASK_ENV=$FLASK_ENV


# perform action
if [ "$ACTION" = "run" ]
then
  echo "starting flask app"
  flask run
fi

