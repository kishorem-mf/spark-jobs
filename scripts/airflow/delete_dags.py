"""
CLI for deleting dags.
Example usage: python delete_dags.py dagname
"""

import argparse
import sys

from airflow import models, settings


def main():
    """Main function."""
    args = parse_args()
    delete_dags(dag_ids=args.dag_ids, quiet=args.quiet, force=args.force)


def parse_args():
    """Parses commandline arguments."""

    parser = argparse.ArgumentParser()
    parser.add_argument("dag_ids", nargs="+", help="DAGs to delete.")
    parser.add_argument(
        "--force",
        default=False,
        action="store_true",
        help="Always try to delete DAGs, even if they aren't found or are still in the DagBag.",
    )
    parser.add_argument(
        "--quiet",
        default=False,
        action="store_true",
        help="Run without asking for confirmation.",
    )

    return parser.parse_args()


def delete_dags(dag_ids, quiet=False, force=False):
    """Deletes DAGs with given IDs."""

    if dag_ids:
        session = settings.Session()
        dag_bag = models.DagBag()

        for dag_id in dag_ids:
            delete_dag(
                dag_id, session=session, dag_bag=dag_bag, force=force, quiet=quiet
            )


        session.commit()


def delete_dag(dag_id, session=None, dag_bag=None, force=False, quiet=False):
    """Deletes DAG with given ID."""

    if not (quiet or query_yes_no(f"Remove dag {dag_id}?")):
        return

    if session is None:
        session = settings.Session()
        commit = True
    else:
        commit = False

    if not force:
        # Check if DAG exists.
        dag = (
            session.query(models.DagModel)
            .filter(models.DagModel.dag_id == dag_id)
            .first()
        )

        if dag is None:
            raise ValueError("DAG {!r} does not exist".format(dag_id))

        # Check if DAG file has been deleted.
        if dag_bag is None:
            dag_bag = models.DagBag()

        if dag_id in dag_bag.dags:
            raise ValueError(
                "DAG {} is still in the DagBag. "
                "Remove the DAG file first.".format(dag_id)
            )

    # Remove DAG
    for current in [
        models.XCom,
        models.SlaMiss,
        models.TaskInstance,
        models.Log,
        models.DagStat,
        models.DagRun,
        models.DagModel,
    ]:
        session.query(current).filter(current.dag_id == dag_id).delete()

    if commit:
        session.commit()


def query_yes_no(question, default="no"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' " "(or 'y' or 'n').\n")


if __name__ == "__main__":
    main()
