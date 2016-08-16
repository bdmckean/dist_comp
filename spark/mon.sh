#!/bin/bash

I=10
#D=10
D=


echo "Run at interval of $I second(s) and length of $D second(s)"
sar $I $D > /root/mon/stat_c.log&
sar -u $I $D > /root/mon/stat_u.log&
sar -b $I $D > /root/mon/stat_b.log&
sar -r $I $D > /root/mon/stat_m.log&
sar -n DEV $I $D > /root/mon/stat_ns.log&
#if/root/mon/stat -bat $I $D > /root/mon/stat_ni.log&
echo "Done!"

