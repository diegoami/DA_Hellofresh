#!/bin/bash

MAIN_PYTHON=$(python -c 'import sys; print(sys.version_info[0])')
SEC_PYTHON=$(python -c 'import sys; print(sys.version_info[1])')

if [ $MAIN_PYTHON -eq 3 ];  then
   if [ $SEC_PYTHON -gt 5 ]; then
	echo "Apache Spark 1.6.1 will not work with Python 3.6 or later"
	exit 1
  fi
fi
exit 0
