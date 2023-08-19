git --no-pager diff -U0 --diff-filter=ACMRT $1 HEAD | clang-format-diff.py -p1 -i -style file
