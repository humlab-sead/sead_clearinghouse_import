import os
import sys
from typing import Any, Optional

import click
from loguru import logger

from .. import utility

CLI_LOG_PATH = './logs'


def update_arguments_from_options_file(
    *,
    arguments: dict,
    filename_key: str,
    log_args: bool = True,
    ctx: click.Context = None,
    skip_keys: str = 'ctx,config_filename',
    suffix: str = None,
) -> dict:
    """Updates `arguments` based on values found in file specified by `filename_key`.
    Values specified at the command line overrides values from options file."""

    options_filename: Optional[str] = arguments.get(filename_key)
    del arguments[filename_key]

    if options_filename:
        arguments = utility.update_dict_from_yaml(options_filename, arguments)
        arguments.update(passed_cli_arguments(ctx, arguments))

    for k in skip_keys.split(','):
        if k in arguments:
            del arguments[k]

    if log_args:
        log_arguments(arguments, suffix=suffix)

    # setup_log_to_file()

    return arguments


# def setup_log_to_file(
#     level: str = 'WARNING',
#     subdir: bool = False,
#     suffix: str = None,
# ) -> None:
#     """Setup loguru logger to write to file"""
#     cli_command: str = utility.strip_path_and_extension(sys.argv[0])
#     folder: str = os.path.join(CLI_LOG_PATH, cli_command) if subdir else CLI_LOG_PATH
#     filename: str = utility.ts_data_path(folder, f"{cli_command}_{level}.log")
#     if suffix:
#         filename = utility.path_add_suffix(filename, suffix=f"_{suffix.strip('_')}")
#     logger.add(filename, filter=lambda record: record["level"].name == level, level=level)


def log_arguments(
    args: dict, subdir: bool = False, skip_keys: str = 'ctx,options_filename', suffix: str = None
) -> None:
    """Log run time arguments to file"""

    def fix_value(v: Any):
        if isinstance(v, tuple):
            v = list(v)
        return v

    cli_command: str = utility.strip_path_and_extension(sys.argv[0])

    log_dir: str = os.path.join(CLI_LOG_PATH, cli_command) if subdir else CLI_LOG_PATH

    os.makedirs(log_dir, exist_ok=True)

    log_name: str = utility.ts_data_path(log_dir, f"{cli_command}.yml")

    if suffix:
        log_name = utility.path_add_suffix(log_name, suffix=f"_{suffix.strip('_')}")

    log_args: dict = {k: fix_value(v) for k, v in args.items() if k not in skip_keys.split(',')}
    utility.write_yaml(log_args, log_name)


def passed_cli_arguments(ctx: click.Context, args: dict) -> dict:
    """Returns a dictionary of arguments passed at the command line"""
    ctx = ctx or click.get_current_context()
    cli_args = {
        name: args[name] for name in args if ctx.get_parameter_source(name) == click.core.ParameterSource.COMMANDLINE
    }

    return cli_args


def remove_none(d: dict) -> dict:
    """Removes keys with None values from a dictionary"""
    return {k: v for k, v in d.items() if v is not None}
