java_binary(
    name = "Jepsen-on-spanner",
    srcs = glob(["src/main/java/**/*.java"]),
    main_class = "com.google.jepsenonspanner.JepsenOnSpanner",
    resources = glob(["src/main/resources/**"]),
    deps = [
        "@maven//:com_beust_jcommander",
        "@maven//:com_google_cloud_google_cloud_spanner",
        "@maven//:com_google_code_gson_gson",
        "@maven//:knossos_knossos",
        "@maven//:org_clojure_clojure",
        "@maven//:org_junit_jupiter_junit_jupiter_api",
        "@maven//:org_junit_jupiter_junit_jupiter_engine",
        "@maven//:us_bpsm_edn_java",
    ],
)
