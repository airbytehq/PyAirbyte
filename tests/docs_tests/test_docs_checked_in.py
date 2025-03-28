# Copyright (c) 2023 Airbyte, Inc., all rights reserved.

import os

import docs.generate as generate
import pytest


@pytest.mark.filterwarnings("ignore")
def test_docs_generation():
    """
    Docs need to be able to be generated via `poetry run poe docs-generate`.

    This test runs the docs generation and ensures that it can complete successfully.

    Generation often produces warnings that are not relevant, so we suppress warnings in this test.
    """

    generate.run()

    # compare the generated docs with the checked in docs
    diff = os.system("git diff --exit-code docs/generated")

    # if there is a diff, fail the test
    assert diff == 0, (
        "Docs are out of date. Please run `poetry run poe docs-generate` and commit the changes."
    )
