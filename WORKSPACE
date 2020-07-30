load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

RULES_JVM_EXTERNAL_TAG = "3.3"

RULES_JVM_EXTERNAL_SHA = "d85951a92c0908c80bd8551002d66cb23c3434409c814179c0ff026b53544dab"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "org.junit.jupiter:junit-jupiter-api:5.3.1",
        "org.junit.jupiter:junit-jupiter-engine:5.3.1",
        "com.google.cloud:google-cloud-spanner:1.55.1",
        "us.bpsm:edn-java:0.7.1",
        "com.google.code.gson:gson:2.8.6",
        "com.beust:jcommander:1.78",
        "knossos:knossos:0.3.5",
        "org.clojure:clojure:1.10.1",
    ],
    repositories = [
        "https://jcenter.bintray.com/",
        "https://repo1.maven.org/maven2",
        "https://maven.google.com",
        "http://clojars.org/repo",
    ],
)
