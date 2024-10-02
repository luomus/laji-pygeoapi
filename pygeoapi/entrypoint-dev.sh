#!/bin/bash

. setup.sh

# start the development server
flask --app src.app --debug run --host=0.0.0.0 --port=80
