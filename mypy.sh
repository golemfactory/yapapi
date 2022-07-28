#   We include most of the --strict flags, but not all
mypy \
    --namespace-packages \
    --install-types \
    example.py      \
    t/test_1.py     \
    run.py          \
    yapapi/mid      \
    --warn-unused-configs           \
    --disallow-subclassing-any      \
    --disallow-untyped-decorators   \
    --no-implicit-optional          \
    --warn-redundant-casts          \
    --warn-unused-ignores           \
    --warn-return-any               \
    --strict-equality               \
    --strict-concatenate            \

#   TODO
    # --disallow-untyped-calls        \
    # --disallow-incomplete-defs      \
    # --check-untyped-defs            \
#   DISABLED
    # --disallow-untyped-defs         \
    # --disallow-any-generics         \
    # --no-implicit-reexport          \
