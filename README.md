# zfileinfo
Gather information on a file in a ZFS filesystem<br/>
:warning: _VERY Unfinished project!_

# Overview
Take one (or more) filenames, and look into them using zfs tools (most likely zdb) to report detailed information about them.
This tool is very unfinished (Sep 2025), and may or may not do any number of things.

# TODO
1. Finish the current only functionality, using "zdb -ddddd" on an object in a ZFS filesystem to find out where all of its blocks are, and report on that.  In some way.
2. Contemplate output format of the above
3. Think about anything else that we might want to know/present about files in ZFS?
4. Implement options to give different types of information about the file.


