source "$home_dir/java/dependencies.properties"

classpath="$home_dir/build/classes"

function add2classpath
{
	for lib in $1; do
		for file in $(ls $2/$lib); do
			classpath="$classpath:$file"
		done
	done
}

# Jython jars
jython_libs='jython.jar'
add2classpath "$jython_libs" "$jython"

# Cascading jars
# We have to exclude SLF4J that comes with Cascading, otherwise there's a
# conflict with the SLF4J from the Hadoop distribution
cascading_libs='cascading-core-*.jar cascading-hadoop-*.jar lib/cascading-core/[jr]*.jar'
add2classpath "$cascading_libs" "$cascading"

# Hadoop jars
hadoop_libs='hadoop-*core*.jar lib/*.jar'
add2classpath "$hadoop_libs" "$hadoop"

export JYTHONPATH=$JYTHONPATH:"$home_dir/python"
