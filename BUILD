# Copyright 2026 The Android Open Source Project
# SPDX-License-Identifier: Apache-2.0

load("@rules_rust//rust:defs.bzl", "rust_binary")

package(default_visibility = ["//visibility:public"])

genrule(
    name = "casimir_nci_packets_rust_gen",
    srcs = ["src/nci_packets.pdl"],
    outs = ["nci_packets.rs"],
    cmd = "$(location @pdl-compiler//:pdlc) --output-format rust $(SRCS) > $(OUTS)",
    tools = ["@pdl-compiler//:pdlc"],
)

genrule(
    name = "casimir_rf_packets_rust_gen",
    srcs = ["src/rf_packets.pdl"],
    outs = ["rf_packets.rs"],
    cmd = "$(location @pdl-compiler//:pdlc) --output-format rust $(SRCS) > $(OUTS)",
    tools = ["@pdl-compiler//:pdlc"],
)

rust_binary(
    name = "casimir",
    srcs = glob(["src/**/*.rs"]) + [
        ":casimir_nci_packets_rust_gen",
        ":casimir_rf_packets_rust_gen",
    ],
    edition = "2021",
    rustc_env = {
        "OUT_DIR": "..",
    },
    deps = [
        "@anyhow",
        "@argh",
        "@bytes",
        "@command-fds",
        "@env_logger",
        "@futures",
        "@log",
        "@pdl-runtime",
        "@tokio",
    ],
)
