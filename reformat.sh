# reformat patch immediately before commit
# if you want to print a diff instead of applying the changes, please use --diff
# git-clang-format --style=file --force --verbose

# reformat source code with clang-format
# if you want to print a diff instead of applying the changes, please remove -i
find src test -name "*.h" -o -name "*.c" -o -name "*.cpp" | xargs clang-format --style=file -i