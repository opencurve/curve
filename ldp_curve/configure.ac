AC_INIT(ldpcurve, 0.0.1, dxiang@corp.netease.com)
AM_INIT_AUTOMAKE([subdir-objects])

AC_CONFIG_MACRO_DIRS([m4])

AC_PROG_CC
AC_PROG_CXX
LT_INIT
#AC_PROG_LIBTOOL
AC_PROG_MAKE_SET


have_lib_dir=yes;
AC_ARG_WITH(lib-dir,
  AC_HELP_STRING([--with-lib-dir=<sysio lib build directory>],
  [directory for sysio library]),
  [     case "${withval}" in
         "yes"|"no"|"") have_lib_dir=no ;;
         *) LIBBUILD_DIR=${withval};
            test -d ${LIBBUILD_DIR} || mkdir ${LIBBUILD_DIR} ||
                have_lib_dir=no;;
        esac;],
  [ LIBBUILD_DIR=`pwd`/lib;
    test -d ${LIBBUILD_DIR} || mkdir ${LIBBUILD_DIR} || have_lib_dir=no;])
if test x${have_lib_dir} = xyes; then
  echo "Using ldp_curve library directory ${LIBBUILD_DIR}"
else
  AC_MSG_ERROR(Need writeable path to sysio library directory ${LIBBUILD_DIR})
fi
AC_SUBST(LIBBUILD_DIR)

AC_CONFIG_FILES([Makefile
                 server/Makefile
                 test/Makefile
                ])
AC_OUTPUT
