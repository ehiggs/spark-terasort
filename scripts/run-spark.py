"""Driver scripts for running Spark-Terasort.

This script assumes we run on a Hadoop YARN cluster with HDFS setup.
"""
import argparse
import logging
import os
import subprocess


HDFS_DIR = "terasort"

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

JAR_NAME = "spark-terasort-1.2-SNAPSHOT-jar-with-dependencies.jar"
JAR_PATH = os.path.join(REPO_ROOT, "target", JAR_NAME)

STEPS = ["generate_input", "sort", "validate_output"]

TERASORT_PKG = "com.github.ehiggs.spark.terasort."
TERAGEN_CLASS = TERASORT_PKG + "TeraGen"
TERASORT_CLASS = TERASORT_PKG + "TeraSort"
TERAVALIDATE_CLASS = TERASORT_PKG + "TeraValidate"


def get_args(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--total_tb",
        default=1,
        type=float,
        help="total data size in TiB",
    )
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


def run(cmd, **kwargs):
    logging.info("$ " + cmd)
    return subprocess.run(cmd, shell=True, check=True, **kwargs)


def run_output(cmd, **kwargs):
    proc = run(cmd, stdout=subprocess.PIPE, **kwargs)
    return proc.stdout.decode("ascii")


def start_yarn():
    # Set core-site.xml hadoop.tmp.dir
    # Run $HADOOP_HOME/bin/hdfs namenode -format
    # Run $HADOOP_HOME/sbin/start-all.sh
    pass


def generate_input(args):
    parts = [
        "spark-submit",
        f"--class {TERAGEN_CLASS}",
        f"--master yarn",
        JAR_PATH,
        f"{args.total_data_size}",
        "/terasort/input",
    ]
    cmd = " ".join(parts)
    # TODO: pipe logs to teragen.log
    run(cmd)


def sort_main(args):
    parts = [
        "spark-submit",
        f"--class {TERASORT_CLASS}",
        f"--master yarn",
        JAR_PATH,
        "/terasort/input",
        "/terasort/output",
    ]
    cmd = " ".join(parts)
    # TODO: pipe logs to terasort.log
    run(cmd)


def validate_output(args):
    parts = [
        "spark-submit",
        f"--class {TERAVALIDATE_CLASS}",
        f"--master yarn",
        JAR_PATH,
        "/terasort/output",
        "/terasort/validate",
    ]
    cmd = " ".join(parts)
    # TODO: pipe logs to teravalidate.log
    run(cmd)


def main(args):
    _get_app_args(args)
    # TODO: wandb setup and logging of time

    if args.generate_input:
        generate_input(args)

    if args.sort:
        sort_main(args)

    if args.validate_output:
        validate_output(args)


if __name__ == "__main__":
    main(get_args())
