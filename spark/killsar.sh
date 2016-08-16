#!/bin/bash


ps -ef | grep sar | awk '{print $2}' | xargs kill
