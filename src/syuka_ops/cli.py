from __future__ import annotations

from .collector import build_parser, options_from_args, run_collect


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    options = options_from_args(args)
    run_collect(options)


if __name__ == "__main__":
    main()
