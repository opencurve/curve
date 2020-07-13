#
#     Copyright (c) 2020 NetEase Inc.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

#!/bin/bash

# Updates local dashboard configurations by retrieving
# the new version from a Grafana instance.
#
# The script assumes that basic authentication is configured
# (change the login credentials with `LOGIN`).
#
# DASHBOARD_DIRECTORY represents the path to the directory
# where the JSON files corresponding to the dashboards exist.
# The default location is relative to the execution of the
# script.
#
# URL specifies the URL of the Grafana instance.
#

set -o errexit

readonly URL=${URL:-"http://127.0.0.1:3000"}
readonly LOGIN=${LOGIN:-"admin:curve"}
readonly DASHBOARDS_DIRECTORY=${DASHBOARDS_DIRECTORY:-"./grafana/provisioning/dashboards"}


main() {
  local dashboards=$(list_dashboards)
  local dashboard_json

  show_config

  for dashboard in $dashboards; do
    dashboard_json=$(get_dashboard "$dashboard")

    if [[ -z "$dashboard_json" ]]; then
      echo "ERROR:
  Couldn't retrieve dashboard $dashboard.
      "
      exit 1
    fi

    echo "$dashboard_json" >$DASHBOARDS_DIRECTORY/$dashboard.json
  done
}


# Shows the global environment variables that have been configured
# for this run.
show_config() {
  echo "INFO:
  Starting dashboard extraction.

  URL:                  $URL
  LOGIN:                $LOGIN
  DASHBOARDS_DIRECTORY: $DASHBOARDS_DIRECTORY
  "
}


# Retrieves a dashboard ($1) from the database of dashboards.
#
# As we're getting it right from the database, it'll contain an `id`.
#
# Given that the ID is potentially different when we import it
# later, to be make this dashboard importable we make the `id`
# field NULL.
get_dashboard() {
  local dashboard=$1

  if [[ -z "$dashboard" ]]; then
    echo "ERROR:
  A dashboard must be specified.
  "
    exit 1
  fi

  curl \
    --silent \
    --user "$LOGIN" \
    $URL/api/dashboards/db/$dashboard |
    jq '.dashboard | .id = null'
}


# lists all the dashboards available.
#
# `/api/search` lists all the dashboards and folders
# that exist under our organization.
#
# Here we filter the response (that also contain folders)
# to gather only the name of the dashboards.
list_dashboards() {
  curl \
    --silent \
    --user "$LOGIN" \
    $URL/api/search |
    jq -r '.[] | select(.type == "dash-db") | .uri' |
    cut -d '/' -f2
}

main "$@"
