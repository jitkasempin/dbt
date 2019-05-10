from abc import ABCMeta, abstractmethod
import os

import six

from dbt.config import RuntimeConfig, Project
from dbt.config.profile import read_profile, PROFILES_DIR
from dbt import tracking
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions


class NoneConfig(object):
    @classmethod
    def from_args(cls, args):
        return None


def read_profiles(profiles_dir=None):
    """This is only used for some error handling"""
    if profiles_dir is None:
        profiles_dir = PROFILES_DIR

    raw_profiles = read_profile(profiles_dir)

    if raw_profiles is None:
        profiles = {}
    else:
        profiles = {k: v for (k, v) in raw_profiles.items() if k != 'config'}

    return profiles


PROFILES_HELP_MESSAGE = """
For more information on configuring profiles, please consult the dbt docs:

https://docs.getdbt.com/docs/configure-your-profile
"""


@six.add_metaclass(ABCMeta)
class BaseTask(object):
    ConfigType = NoneConfig

    def __init__(self, args, config):
        self.args = args
        self.config = config

    @classmethod
    def pre_init_hook(cls):
        """A hook called before the task is initialized."""

    @classmethod
    def from_args(cls, args):
        try:
            config = cls.ConfigType.from_args(args)
        except dbt.exceptions.DbtProjectError as exc:
            logger.error("Encountered an error while reading the project:")
            logger.error("  ERROR: {}".format(str(exc)))

            tracking.track_invalid_invocation(
                args=args,
                result_type=exc.result_type)
            raise dbt.exceptions.RuntimeException('Could not run dbt')
        except dbt.exceptions.DbtProfileError as exc:
            logger.error("Encountered an error while reading profiles:")
            logger.error("  ERROR {}".format(str(exc)))

            all_profiles = read_profiles(args.profiles_dir).keys()

            if len(all_profiles) > 0:
                logger.info("Defined profiles:")
                for profile in all_profiles:
                    logger.info(" - {}".format(profile))
            else:
                logger.info("There are no profiles defined in your "
                            "profiles.yml file")

            logger.info(PROFILES_HELP_MESSAGE)

            tracking.track_invalid_invocation(
                args=args,
                result_type=exc.result_type)
            raise dbt.exceptions.RuntimeException('Could not run dbt')
        return cls(args, config)

    @abstractmethod
    def run(self):
        raise dbt.exceptions.NotImplementedException('Not Implemented')

    def interpret_results(self, results):
        return True


def get_nearest_project_dir():
    root_path = os.path.abspath(os.sep)
    cwd = os.getcwd()

    while cwd != root_path:
        project_file = os.path.join(cwd, "dbt_project.yml")
        if os.path.exists(project_file):
            return cwd
        cwd = os.path.dirname(cwd)

    return None


def move_to_nearest_project_dir():
    nearest_project_dir = get_nearest_project_dir()
    if nearest_project_dir is None:
        raise dbt.exceptions.RuntimeException(
            "fatal: Not a dbt project (or any of the parent directories). "
            "Missing dbt_project.yml file"
        )

    os.chdir(nearest_project_dir)


class RequiresProjectTask(BaseTask):
    @classmethod
    def from_args(cls, args):
        move_to_nearest_project_dir()
        return super(RequiresProjectTask, cls).from_args(args)


class ConfiguredTask(RequiresProjectTask):
    ConfigType = RuntimeConfig


class ProjectOnlyTask(RequiresProjectTask):
    ConfigType = Project
