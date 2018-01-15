#!/bin/bash

MAIN_PYTHON=$(python -c 'import sys; print(sys.version_info[0])')
SEC_PYTHON=$(python -c 'import sys; print(sys.version_info[1])')

if [$MAIN_PYTHON = 3];  then
   if [$SEC_PYTHON > 5]; then
	echo "Apache Spark 1.6.1 will not work with Python 3.6 or later"
	return 1
   fi
fi

return 0
