#!/bin/sh

aclocal &&
automake --add-missing --copy --force &&
${AUTOCONF:-autoconf}
