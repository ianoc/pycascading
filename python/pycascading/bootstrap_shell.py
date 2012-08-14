import sys, imp
running_mode = 'local'
python_dir = sys.argv[1] # Path to the Pycascading Python sources
sys.argv = sys.argv[2:]

from com.twitter.pycascading import Util

# This is a list of jars that Cascading comes in
cascading_jar = Util.getCascadingJar()
# This is the folder where Hadoop extracted the jar file for execution
tmp_dir = Util.getJarFolder()

Util.setPycascadingRoot(python_dir)

# The initial value of sys.path is JYTHONPATH plus whatever Jython appends
# to it (normally the Python standard libraries the come with Jython)
print cascading_jar
print python_dir
print tmp_dir
sys.path.extend(cascading_jar)
sys.path.extend((tmp_dir, python_dir,'.'))

# Allow the importing of user-installed Jython packages
import site
site.addsitedir(python_dir + '/python/Lib/site-packages')

#print 'PATH:', sys.path
import os
import encodings
import pycascading.pipe, getopt

# This holds some global configuration parameters
pycascading.pipe.config = dict()

# pycascading.pipe.config is a dict with configuration parameters
pycascading.pipe.config['pycascading.running_mode'] = running_mode
pycascading.pipe.config['pycascading.main_file'] = 'stdin'

