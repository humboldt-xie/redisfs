#!/bin/bash
#===============================================================================
#
#          FILE:  run.sh
# 
#         USAGE:  ./run.sh 
# 
#   DESCRIPTION:  
# 
#       OPTIONS:  ---
#  REQUIREMENTS:  ---
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  xiehongbao (), @.com
#       COMPANY:  
#       VERSION:  1.0
#       CREATED:  2018/06/29 00时01分08秒 CST
#      REVISION:  ---
#===============================================================================
set -e
set -x
go build ./
umount -f /tmp/a
./redisfs /tmp/a
