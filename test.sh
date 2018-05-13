#!/bin/bash
function generate(){
    n=1
    RANDOM=$1
    while [ $n -le $3 ];do
            if [ $? -eq 0 ]
                then
                    let n++
                    project1="https://frontend-dot-cloud-202303.appspot.com/get_information"
                    time s=$(curl -F 'jobtype=Anything' -F 'joblocation=Bay Area' -X POST $project1)
            fi
    done
}

function fork(){
        count=1;
        echo $1
        while [ $count -le $1 ]
        do
                generate $(($base+$count)) $2 $3&

                count=$(( count+1 ))
        done
   }
   base=1213
if [ !${1} ]
then
echo ./test.sh [IP_address] [concurrent_requests_number] [total_requests_number]
fi
s=$((${3}/${2}))
fork ${2} ${1} $s
exit 0
