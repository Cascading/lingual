if [ -z "$TEZ_JARS" ]; then
  echo "please set the TEZ_JARS environment variable"
  exit 1
fi
if [ -z "$TEZ_CONF_DIR" ]; then
  echo "please set the TEZ_CONF_DIR environment variable"
  exit 1
fi

export YARN_CLASSPATH="$YARN_CLASSPATH:${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*"
