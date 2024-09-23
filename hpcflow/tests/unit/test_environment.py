import pytest
from hpcflow.sdk.core.environment import SemanticVersionSpec
from hpcflow.sdk.core.errors import SemanticVersionSpecError


def test_precedence_maj_min_patch():
    assert (
        SemanticVersionSpec("1.0.0")
        < SemanticVersionSpec("2.0.0")
        < SemanticVersionSpec("2.1.0")
        < SemanticVersionSpec("2.1.1")
    )


def test_precedence_patch():
    assert SemanticVersionSpec("0.0.1") < SemanticVersionSpec("0.0.10")


def test_precedence_prerelease_simple():
    assert SemanticVersionSpec("1.0.0-alpha") < SemanticVersionSpec("1.0.0")


def test_precedence_prerelease_complex():
    assert (
        SemanticVersionSpec("1.0.0-alpha")
        < SemanticVersionSpec("1.0.0-alpha.1")
        < SemanticVersionSpec("1.0.0-alpha.beta")
        < SemanticVersionSpec("1.0.0-beta")
        < SemanticVersionSpec("1.0.0-beta.2")
        < SemanticVersionSpec("1.0.0-beta.11")
        < SemanticVersionSpec("1.0.0-rc.1")
        < SemanticVersionSpec("1.0.0")
    )


def test_equality():
    assert SemanticVersionSpec("1.0.0") == SemanticVersionSpec("1.0.0")


def test_equality_with_build_metadata():
    assert SemanticVersionSpec("1.0.0") == SemanticVersionSpec("1.0.0+xyz")


def test_equality_with_prerelease_and_build_metadata():
    assert SemanticVersionSpec("1.0.0-beta.11") == SemanticVersionSpec(
        "1.0.0-beta.11+xyz"
    )


def test_equality_str():
    assert SemanticVersionSpec("1.0.0") == "1.0.0"


def test_lt_str():
    assert SemanticVersionSpec("1.0.0") < "1.1.0"


def test_gt_str():
    assert SemanticVersionSpec("1.1.0") > "1.0.0"


def test_ge_str():
    assert SemanticVersionSpec("1.1.0") >= "1.0.0"


def test_le_str():
    assert SemanticVersionSpec("1.0.0") <= "1.1.0"


def test_raise_semver_error_missing_buildmetadata():
    with pytest.raises(SemanticVersionSpecError):
        SemanticVersionSpec("1.0.0+")


def test_raise_semver_error_missing_min_or_patch():
    with pytest.raises(SemanticVersionSpecError):
        SemanticVersionSpec("1.0")
