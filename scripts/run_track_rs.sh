#!/bin/bash

# Run Track_RS.jar program
# X.Bonnin, 10-APR-2014
#
# Usage: 
#     bash run_track_rs.sh tablespace host [startdate enddate zonex zoney output_dir]
#
# Example:
#     bash run_track_rs.sh hfc1test voparis-mysql5-paris.obspm.fr '2001-01-01 00:00:00' '2001-01-31 23:59:59' 

if [ $# -lt 2 ]
then 
    echo 'Usage: bash run_track_rs.sh tablespace host [startdate enddate zonex zoney output_dir]'
    exit 1
fi

TABLESPACE=$1
HOST=$2
STARTDATE="`date -v-1m '+%Y-%m-%d %H:%M:%S'`"
ENDDATE="`date -v-1d '+%Y-%m-%d %H:%M:%S'`"
ZONEX=4
ZONEY=4
OUTPUT_DIR=../products
if [ $# -gt 2 ]
then 
    STARTDATE="$3"
fi
if [ $# -gt 3 ]
then 
    ENDDATE="$4"
fi
if [ $# -gt 4 ]
then 
    ZONEX=$5
fi
if [ $# -gt 5 ]
then 
    ZONEY=$6
fi
if [ $# -gt 6 ]
then 
    OUTPUT_DIR=$7
fi

java -jar Track_RS.jar $TABLESPACE $HOST "$STARTDATE" "$ENDDATE" $ZONEX $ZONEY $OUTPUT_DIR

exit