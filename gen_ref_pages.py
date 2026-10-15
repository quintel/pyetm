"""Generate the code reference pages and navigation."""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

src_root = Path(__file__).parent.parent / "src"
reference_root = Path("api")

# Iterate through all Python files in src/pyetm
for path in sorted(src_root.rglob("*.py")):
    # Get the module path relative to src
    module_path = path.relative_to(src_root).with_suffix("")

    # Get the parts for navigation
    parts = tuple(module_path.parts)

    # Skip if not in pyetm package
    if parts[0] != "pyetm":
        continue

    # Skip __pycache__ and similar
    if any(part.startswith("_") and part != "__init__" and part != "__main__" for part in parts):
        continue

    # Skip __init__.py and __main__.py files
    if parts[-1] in ("__init__", "__main__"):
        continue

    # Create document path without 'pyetm' prefix to avoid duplication
    nav_parts = parts[1:]  # Remove 'pyetm' prefix
    doc_path = Path(*nav_parts).with_suffix(".md")
    full_doc_path = reference_root / doc_path

    # Add to navigation (use nav_parts for structure, doc_path for link)
    # Since SUMMARY.md is in api/, paths should be relative to api/
    if nav_parts:
        nav[nav_parts] = doc_path.as_posix()

    # Write the API reference page
    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        # Create the module identifier
        ident = ".".join(parts)

        # Write the mkdocstrings directive
        print(f"::: {ident}", file=fd)

    # Set the edit path
    mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(src_root.parent))

# Write the navigation file
with mkdocs_gen_files.open(reference_root / "SUMMARY.md", "w") as nav_file:
    # Add the index page first
    nav_file.write("* [Overview](index.md)\n")
    # Then add the generated navigation
    nav_file.writelines(nav.build_literate_nav())
