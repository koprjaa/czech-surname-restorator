#
# Project: czech-surname-restorator
# File:    main.py
#
# Description:
# Command line entry point: loads the reference list, spreads the work over Ray workers, and writes the corrected CSV.
#
# Author:
# Jan Alexandr Kopřiva
# jan.alexandr.kopriva@gmail.com
#
# License: MIT
#

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

from restorator import build_index, correct_batch

logger = logging.getLogger(__name__)


def setup_logging(log_file: Path | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def load_reference(path: Path, column: str) -> list[str] | None:
    """Read the reference surname list, or None when the column is not there."""
    frame = pd.read_csv(path, dtype=str)
    if column not in frame.columns:
        logger.error("Reference %s has no column %r. Found: %s", path, column, list(frame.columns))
        return None
    return sorted(frame[column].dropna().tolist())


def split_names(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    """Split a full name into its first token and its last token."""
    frame["given_name"] = frame[column].str.extract(r"^(\S+)")
    frame["surname"] = frame[column].str.extract(r"(\S+)$")
    return frame


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore diacritics in Czech surnames using a reference list."
    )
    parser.add_argument("input", type=Path, help="CSV file with the names to correct")
    parser.add_argument(
        "-o", "--output", type=Path, default=Path("output_corrected.csv"),
        help="where to write the result (default: output_corrected.csv)",
    )
    parser.add_argument(
        "-r", "--reference", type=Path, default=Path("surnames_all.csv"),
        help="reference surname list (default: surnames_all.csv)",
    )
    parser.add_argument(
        "--name-column", default="Person_Name", help="column holding the full name"
    )
    parser.add_argument(
        "--reference-column", default="column1", help="column holding the reference surnames"
    )
    parser.add_argument("--chunk-size", type=int, default=10_000, help="rows read at a time")
    parser.add_argument("--batch-size", type=int, default=1_000, help="names per Ray task")
    parser.add_argument("--num-cpus", type=int, default=None, help="CPUs for Ray (default: all)")
    parser.add_argument("--log-file", type=Path, default=None, help="also write logs here")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    setup_logging(args.log_file)

    if not args.input.exists():
        logger.error("Input file does not exist: %s", args.input)
        return 1
    if not args.reference.exists():
        logger.error("Reference file does not exist: %s", args.reference)
        return 1

    # Imported here so that the library stays usable without a Ray cluster.
    import ray

    started = time.time()

    reference_names = load_reference(args.reference, args.reference_column)
    if reference_names is None:
        return 1
    logger.info("Loaded %d reference surnames", len(reference_names))

    index = build_index(reference_names)
    logger.info("Built an index with %d keys", len(index))

    flat_reference = [name for names in index.values() for name in names]

    ray.init(ignore_reinit_error=True, num_cpus=args.num_cpus, log_to_driver=True)
    try:
        worker = ray.remote(correct_batch)
        index_ref = ray.put(index)
        reference_ref = ray.put(flat_reference)

        rows_written = 0
        first_chunk = True
        for chunk in pd.read_csv(
            args.input, dtype=str, low_memory=False, chunksize=args.chunk_size
        ):
            if args.name_column not in chunk.columns:
                logger.error("Input has no column %r", args.name_column)
                return 1

            chunk = split_names(chunk, args.name_column)
            surnames = chunk["surname"].tolist()

            tasks = [
                worker.remote(surnames[i: i + args.batch_size], index_ref, reference_ref)
                for i in range(0, len(surnames), args.batch_size)
            ]
            corrected = [name for batch in ray.get(tasks) for name in batch]

            chunk["corrected_surname"] = corrected
            chunk["corrected_name"] = chunk["given_name"] + " " + chunk["corrected_surname"]

            chunk.to_csv(
                args.output, mode="w" if first_chunk else "a",
                header=first_chunk, index=False, encoding="utf-8",
            )
            first_chunk = False
            rows_written += len(chunk)
            logger.info("Wrote %d rows so far", rows_written)
    finally:
        ray.shutdown()

    logger.info("Wrote %d rows to %s in %.2f s", rows_written, args.output, time.time() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
