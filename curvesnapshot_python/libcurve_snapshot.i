%module curvesnapshot

%include "std_vector.i"

%{
    #include <stdint.h>
    #include "libcurveSnapshot.h"
%}

%include <stdint.i>
%include "libcurveSnapshot.h"

namespace std {
  %template(CCIDinfoVector) vector<CChunkIDInfo_t>;
  %template(IntVector) vector<int>;
}