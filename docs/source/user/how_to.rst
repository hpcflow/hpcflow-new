How-to guides
#############

Configuration
-------------

Load an existing workflow
~~~~~~~~~~~~~~~~~~~~~~~~~

We support paths like:

* `/path/to/workflow` for local zarr or json
* `/path/to/workflow.zip` for local zarr-zip
* `/path/to/workflow.json` for local json-single # TODO
* `ssh://user@host/path/to/workflow` for remote zarr
* `ssh://user@host/path/to/workflow.zip` for remote zarr-zip
* `ssh://user@host/path/to/workflow.json` for remote json
* `https://sandbox.zenodo.org/record/1210144/files/workflow.zip` for zenodo zarr-zip

You can convert a zarr store to a zarr-zip store using `Workflow.to_zip()`.

Get and set config items
~~~~~~~~~~~~~~~~~~~~~~~~

Using the config sub-command in the |app_name| CLI, we can get configuration items like this:

.. jinja:: first_ctx
    
    .. code-block:: console

        {{ app_package_name }} config get machine

    Items can be set like this:

    .. code-block:: console

        {{ app_package_name }} config set machine my-machine-name

    ------------

    In the Python API, we can interact with the |app_name| configuration as below. Note that we must call :meth:`config.save <hpcflow.sdk.config.config.Config.save>` to make the config changes persistent, otherwise any changes made will only be temporary.

    .. code-block:: python

        import {{ app_module }} as {{ app_docs_import_conv }}

        # print the value of the `machine` item:
        print({{ app_docs_import_conv }}.config.machine)

        # set the value of the `machine` item:
        {{ app_docs_import_conv }}.config.machine = "my-machine-name"

        # save the changes to the config file:
        {{ app_docs_import_conv }}.config.save()

        # "see" the changes in the current session:
        {{ app_docs_import_conv }}.reload_config()

    If you want to change a configuration item temporarily (just for the current session), you can provide configuration item values to `load_config` and `reload_config`, like this:

    .. code-block:: python

        import {{ app_module }} as {{ app_docs_import_conv }}

        # modify the log console level just for this session:
        {{ app_docs_import_conv }}.load_config(log_console_level="debug")

    See the configuration :ref:`reference documentation <reference/config_file:Configuration file>` for a listing of configurable items.


Template components
-------------------

How to name parameters and task schemas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parameter type names (i.e. the `typ` attribute) must be valid Python identifiers. This means 
that they cannot start with a number. They must also be fully alphanumeric, but may include 
underscores (but not at the start). These rules also apply to task schema methods,
implementations, and objective names. See :func:`hpcflow.sdk.core.utils.check_valid_py_identifier`
for more details. By convention, lower case is preferred, except for acronyms and
initialisms.
