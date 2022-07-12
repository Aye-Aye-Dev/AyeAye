"""
Common patterns/helpers for populating the resolver context.

@see ayeaye.connector_resolver

The resolver context is there to provide runtime variables to dataset connections (i.e. 
ayeaye.Connect(..)) within ayeaye.Models.
"""
import json
import os
import sys

import ayeaye


def manifest_build_context():
    """
    A manifest is a dataset document with variables needed to repeat a build. i.e. exact versions
    of files used, paths etc.

    This method makes all the variables in the manifest available within the resolver context. It
    also makes details of the manifest file available-
    * manifest_path - Filesystem path. e.g. /data/
    * manifest_file - absolute path to manifest file. e.g. /data/manifest_20220511.json

    For example of this working, see ayeaye/examples/manifest_build_context/

    TLDR;
    At the command line -
    $ python sensor_model.py manifest_file.json

    Warning: don't let the manifest file get too big as it will be loaded by every model. If you need big
    listings of files create secondary manifest files and refer to them in the primary manifest file.

    @return: ayeaye.connector_resolver context object
    """

    if len(sys.argv) != 2:
        msg = (
            "Manifest build context requires a single command line argument.\n"
            f"usage: python {sys.argv[0]} <manifest_file>\n"
            "e.g.\n"
            f"python {sys.argv[0]} /data/manifest_20220511.json"
        )
        sys.stderr.write(msg)
        sys.exit(1)

    manifest_file = os.path.abspath(sys.argv[1])
    manifest_path = os.path.dirname(manifest_file)

    with open(manifest_file, encoding="utf-8-sig") as f:
        manifest = json.load(f)

    context_values = {
        "manifest_path": manifest_path,
        "manifest_file": manifest_file,
    }

    # values in the manifest fil eoverwrite the manifest_* vars
    if not isinstance(manifest, dict):
        raise ValueError("The manifest file must contain a dictionary so key values can be used as context variables")
    context_values.update(manifest)

    context = ayeaye.connector_resolver.context(**context_values)

    return context
