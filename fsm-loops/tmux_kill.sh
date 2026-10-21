#!/bin/bash

while getopts "c:" opt; do
  case $opt in
    c) CALIBRATION="$OPTARG" ;;
    *) echo "Usage: $0 -c EcalPedestals|SiStripBad|BeamSpot"; exit 1 ;;
  esac
done

if [[ "$CALIBRATION" != "EcalPedestals" && "$CALIBRATION" != "SiStripBad" && "$CALIBRATION" != "BeamSpot" ]]; then
  echo "Error: calibration -c must be EcalPedestals, SiStripBad, or BeamSpot"
  exit 1
fi

for step in 2 3 4; do
  SESSION="CalibrationLoop${step}_${CALIBRATION}"

  if tmux has-session -t "$SESSION" 2>/dev/null; then
    tmux kill-session -t "$SESSION"
    echo "Killed session: $SESSION"
  else
    echo "Session not found: $SESSION"
  fi
done
