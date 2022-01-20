"""Driver scripts for running Spark-Terasort.

This script assumes we run on a Hadoop YARN cluster with HDFS setup.
"""
import argparse
import logging
import os
import subprocess

import numpy as np
import requests
import wandb
import re

HDFS_DIR = "terasort"

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

JAR_NAME = "spark-terasort-1.2-SNAPSHOT-jar-with-dependencies.jar"
JAR_PATH = os.path.join(REPO_ROOT, "target", JAR_NAME)

STEPS = [
    "generate_input",
    "sort",
    "validate_output",
]

TERASORT_PKG = "com.github.ehiggs.spark.terasort."
TERAGEN_CLASS = TERASORT_PKG + "TeraGen"
TERASORT_CLASS = TERASORT_PKG + "TeraSort"
TERAVALIDATE_CLASS = TERASORT_PKG + "TeraValidate"

HIST_SERVER = "http://localhost:18080/api/v1/applications"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%Z"


def get_args(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--total_tb",
        default=1,
        type=float,
        help="total data size in TiB",
    )
    parser.add_argument(
        "--input_part_size",
        default=2500 * 1000 * 1000,
        type=int,
        help="size in bytes of each map partition",
    )
    parser.add_argument(
        "--num_workers",
        default=8,
        type=int,
        help="number of workers in the YARN cluster",
    )
    parser.add_argument(
        "--map_parallelism",
        default=8,
        type=int,
        help="number of executors to run per YARN node",
    )
    parser.add_argument("--external_shuffle", action="store_true")
    parser.add_argument("--push_based_shuffle", action="store_true")
    # Which steps to run?
    steps_grp = parser.add_argument_group(
        "steps to run", "if none is specified, will run all steps"
    )
    for step in STEPS:
        steps_grp.add_argument(f"--{step}", action="store_true")
    return parser.parse_args(*args, **kwargs)


def _get_app_args(args):
    # If no steps are specified, run all steps.
    args_dict = vars(args)
    if not any(args_dict[step] for step in STEPS):
        for step in STEPS:
            args_dict[step] = True
    args.total_data_size = int(args.total_tb * 10 ** 12)
    args.num_mappers = int(np.ceil(args.total_data_size / args.input_part_size))


def get_spark_args(args):
    ret = []
    # Universal setup
    # TODO: this didn't work
    # ret.append(f"-c spark.files.maxPartitionBytes={args.input_part_size}")
    # Parallelism: number of executors
    num_executors = args.num_workers * args.map_parallelism
    ret.append(f"--num-executors={num_executors}")
    ret.append("--executor-cores=2")
    # External shuffle service
    if args.external_shuffle or args.push_based_shuffle:
        ret.append("-c spark.shuffle.service.enabled=true")
    if args.push_based_shuffle:
        ret.append("-c spark.shuffle.push.enabled=true")
    return ret


def run(cmd, **kwargs):
    logging.info("$ " + cmd)
    return subprocess.run(
        cmd, capture_output=True, shell=True, check=True, encoding="utf-8", **kwargs
    )


def run_output(cmd, **kwargs):
    proc = run(cmd, stdout=subprocess.PIPE, **kwargs)
    return proc.stdout.decode("ascii")


def generate_input(args):
    spark_args = get_spark_args(args)
    parts = (
        ["spark-submit"]
        + spark_args
        + [
            f"-c spark.default.parallelism={args.num_mappers}",
            f"--class {TERAGEN_CLASS}",
            f"--master yarn",
            JAR_PATH,
            f"{args.total_data_size}",
            "/terasort/input",
        ]
    )
    cmd = " ".join(parts)
    # TODO: pipe logs to teragen.log
    return run(cmd)


def sort_main(args):
    spark_args = get_spark_args(args)
    parts = (
        ["spark-submit"]
        + spark_args
        + [
            f"--class {TERASORT_CLASS}",
            f"--master yarn",
            JAR_PATH,
            "/terasort/input",
            "/terasort/output",
        ]
    )
    cmd = " ".join(parts)
    # TODO: pipe logs to terasort.log
    return run(cmd)


def validate_output(args):
    spark_args = get_spark_args(args)
    parts = [
        ["spark-submit"]
        + spark_args
        + [
            f"--class {TERAVALIDATE_CLASS}",
            f"--master yarn",
            JAR_PATH,
            "/terasort/output",
            "/terasort/validate",
        ]
    ]
    cmd = " ".join(parts)
    # TODO: pipe logs to teravalidate.log
    return run(cmd)


def log_metrics(result):
    wandb.init(project="spark", entity="raysort")

    app_id = re.search("application_\d+_\d+", str(result.stderr)).group()

    metrics = {
        "duration": 0,
        "memoryBytesSpilled": 0,
        "diskBytesSpilled": 0,
        "shuffleReadBytes": 0,
        "shuffleWriteBytes": 0,
    }

    endpoint = f"{HIST_SERVER}/{app_id}"

    app_metrics = requests.get(endpoint).json()
    metrics["duration"] = app_metrics["attempts"][0]["duration"] / 1000

    stage_metrics = requests.get(f"{endpoint}/stages").json()
    for sm in stage_metrics:
        metrics["memoryBytesSpilled"] += sm["memoryBytesSpilled"]
        metrics["diskBytesSpilled"] += sm["diskBytesSpilled"]
        metrics["shuffleReadBytes"] += sm["shuffleReadBytes"]
        metrics["shuffleWriteBytes"] += sm["shuffleWriteBytes"]

    wandb.log(metrics)


def main(args):
    _get_app_args(args)
    print(args)
    # TODO: wandb setup and logging of time

    if args.generate_input:
        result = generate_input(args)

    if args.sort:
        result = sort_main(args)
        log_metrics(result)

    if args.validate_output:
        validate_output(args)


if __name__ == "__main__":
    main(get_args())
