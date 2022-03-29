from hpcflow.config import Config


def get_config_file_contents():
    return Config.config_file_contents


def show_config_file_contents():
    print(get_config_file_contents())


def show_config():
    print(Config.to_string(exclude=["config_file_contents"]))


def check():
    Config.check_data_files()
