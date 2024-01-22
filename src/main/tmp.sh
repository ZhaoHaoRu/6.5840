#!/usr/bin/env bash

cd mr-tmp
NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
echo $NT