build:
	mvn clean package
	rm -rf /usr/local/share/kafka/plugins/*
	cp -rf target/kafka-connector-*-fat.jar /usr/local/share/kafka/plugins

start-source:
	connect-standalone /usr/local/etc/kafka/connect-standalone.properties /Users/bzhezlo/Projects/Lohika/kafka-connector/config/MySourceConnector.properties

start-sink:
	connect-standalone /usr/local/etc/kafka/connect-standalone.properties /Users/bzhezlo/Projects/Lohika/kafka-connector/config/MySinkConnector.properties
