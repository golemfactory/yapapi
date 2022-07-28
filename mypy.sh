#   Most of the --strict flags are included
mypy \
    --namespace-packages            \
    --install-types                 \
    example.py                      \
    run.py                          \
    yapapi/mid                      \
    --warn-unused-configs           \
    --disallow-incomplete-defs      \
    --disallow-subclassing-any      \
    --disallow-untyped-decorators   \
    --no-implicit-optional          \
    --warn-redundant-casts          \
    --warn-unused-ignores           \
    --warn-return-any               \
    --strict-equality               \
    --strict-concatenate            \
    --check-untyped-defs            \
    --disallow-untyped-defs         \

    #   Disabled parts of --strict
    # --disallow-any-generics         \
    # --no-implicit-reexport          \
    # --disallow-untyped-calls        \
