These GitHub actions workflows are synchronised with those in https://github.com/hpcflow-new/python-release-workflow.

The following modifications are made:

In `test.yml`, we set these environment variables:

```yaml
env:
  PYTEST_ARGS: --verbose --exitfirst -k "not task_schema and not task_template and not resolve_elements and not input_value and not action"
```
