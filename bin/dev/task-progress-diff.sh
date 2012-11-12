#!/bin/sh

grep "Starting"  $1 | awk '{ print $10 }' | sort -n > starting.log
grep "Finished TID" $1 | awk '{ print $7 }' | sort -n > finished.log
diff starting.log finished.log

