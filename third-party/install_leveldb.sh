mkdir -p $2/include
mkdir -p $2/lib
cp -Rf $1/include/* $2/include
cp -Rf -P $1/out-shared/libleveldb.so* $2/lib
