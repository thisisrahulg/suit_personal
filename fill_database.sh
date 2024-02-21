#!/bin/bash

database_queue=database.queue

echo "fill_database.sh: $USER $(date "+%Y-%m-%d %H:%M:%S"):: fill_database.sh INITIATED" 
while true;

	do 
		curfile=$(head -n 1 $database_queue)
		if [ ! -z $curfile ]; then

            ./fillDatabase.py $curfile


            escaped_curfile=$(sed 's/[^^]/[&]/g; s/\^/\\^/g' <<<"$curfile")
            sed -i "\|${escaped_curfile}|d" "$database_queue"
		
            echo "fill_database.sh: $USER $(date "+%Y-%m-%d %H:%M:%S"):: Execution completed"
	        
                	
		fi
	done

