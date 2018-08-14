package(default_visibility = ["//visibility:public"])

java_binary(
    name='api-examples-unshaded',
    srcs = glob(["org/apache/heron/examples/api/**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/simulator/src/java:simulator-java",
        "@org_apache_curator_curator_client//jar",
        "@org_apache_curator_curator_framework//jar",
        "@org_apache_kafka_kafka_211//jar",
        "@org_apache_kafka_kafka_clients//jar",
        "@org_json//jar",
        "@org_apache_zookeeper_zookeeper//jar",
        "@org_slf4j_slf4j_api//jar",
        "@log4j_log4j//jar",
        "@io_netty_netty//jar",
        "@jline_jline//jar",
        "@com_google_guava_guava//jar",
    ],
    create_executable = 0,
)

genrule(
    name = 'heron-api-examples',
    srcs = [":api-examples-unshaded_deploy.jar"],
    outs = ["heron-api-examples.jar"],
    cmd  = "cp $< $@",
)

java_binary(
    name='streamlet-examples-unshaded',
    srcs = glob(["org/apache/heron/examples/streamlet/**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:basics-java",
        "//heron/simulator/src/java:simulator-java",
        "//third_party/java:kryo",
        "@apache_pulsar_client//jar",
        "@org_apache_commons_commons_math3//jar"
    ],
    create_executable = 0,
)

genrule(
    name = 'heron-streamlet-examples',
    srcs = [":streamlet-examples-unshaded_deploy.jar"],
    outs = ["heron-streamlet-examples.jar"],
    cmd  = "cp $< $@",
)
