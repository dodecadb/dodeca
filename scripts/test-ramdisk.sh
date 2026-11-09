#!/bin/sh
# Creates a RAM disk for test data on macOS: Neo4j and Raft force every commit to disk, which on
# an SSD takes most of a test's time. The build keeps test data on /Volumes/DozenDbTests while it
# is mounted. Its size is 8 GB, or DOZENDB_RAMDISK_GB.
set -eu
volume=/Volumes/DozenDbTests
if [ "$(uname)" != Darwin ]; then
  echo "Not macOS: pass -Dtest.tmpdir=/dev/shm, or another RAM disk, to the build" >&2
  exit 1
fi
if [ -d "$volume" ]; then
  echo "$volume is mounted already"
  exit 0
fi
device=$(hdiutil attach -nomount "ram://$((${DOZENDB_RAMDISK_GB:-8} * 2097152))" | awk '{print $1}')
# APFS, as the system disk: HFS+ keeps file times in whole seconds, which tests order files by.
diskutil erasevolume APFS DozenDbTests "$device" > /dev/null
echo "Test data goes to $volume until it is ejected: hdiutil detach $device"
