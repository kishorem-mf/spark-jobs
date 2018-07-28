#!/usr/bin/env bash

##############################################################################
# Description:                                                               #
# Helper script for running pylint and checking for certain error conditions #
# in pylints exit code.                                                      #
#                                                                            #
# Output types:                                                              #
#   * (C) convention, for programming standard violation                     #
#   * (R) refactor, for bad code smell                                       #
#   * (W) warning, for python specific problems                              #
#   * (E) error, for probable bugs in the code                               #
#   * (F) fatal, if an error occurred which prevented pylint from doing      #
#                further processing.                                         #
##############################################################################

# Run pylint.
pylint --output-format=colorized $@

# Check exit code with bitmask, so that we only return a
# non-zero exit code for fatal & error messages and usage errors.
#
# Pylint exit codes:
# 0 no error
# 1 fatal message issued
# 2 error message issued
# 4 warning message issued
# 8 refactor message issued
# 16 convention message issued
# 32 usage error
rc=$?; ((($rc&(1+2+32))>0)) && exit 1 || exit 0
