#!/bin/sh

libtoolize &&
aclocal &&
automake --add-missing --copy --force &&
${AUTOCONF:-autoconf}
