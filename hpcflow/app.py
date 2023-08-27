from hpcflow import __version__, _app_name
from hpcflow.sdk import app as sdk_app
from hpcflow.sdk.config import ConfigOptions


# provide access to app attributes:
__getattr__ = sdk_app.get_app_attribute

# ensure docs/help can see dynamically loaded attributes:
__all__ = sdk_app.get_app_module_all()
__dir__ = sdk_app.get_app_module_dir()

# set app-level config options:
config_options = ConfigOptions(
    directory_env_var="HPCFLOW_CONFIG_DIR",
    default_directory="~/.hpcflow-new",
    sentry_DSN="https://2463b288fd1a40f4bada9f5ff53f6811@o1180430.ingest.sentry.io/6293231",
    sentry_traces_sample_rate=1.0,
    sentry_env="main" if "a" in __version__ else "develop",
)

# load built in template components (in this case, for demonstration purposes):
template_components = sdk_app.BaseApp.load_builtin_template_component_data(
    "hpcflow.sdk.data.template_components"
)

# initialise the App object:
app: sdk_app.BaseApp = sdk_app.BaseApp(
    name=_app_name,
    version=__version__,
    module=__name__,
    docs_import_conv="hf",
    description="Computational workflow management",
    config_options=config_options,
    template_components=template_components,
    scripts_dir="sdk.demo.scripts",  # relative to root package
    pytest_args=[
        "--verbose",
        "--exitfirst",
    ],
)  #: |app|
