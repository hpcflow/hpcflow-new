"""Core programmatic models for hpcflow.

EAR abort exit code is set to 64 [1].

References
----------
https://tldp.org/LDP/abs/html/exitcodes.html

"""

#: Formats supported for templates.
ALL_TEMPLATE_FORMATS = ("yaml", "json")
#: The exit code used by an EAR when it aborts.
ABORT_EXIT_CODE = 64
