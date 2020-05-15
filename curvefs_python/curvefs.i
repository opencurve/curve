%module curvefs
%{
#include <stdint.h>
#include "curve_type.h"
#include "libcurvefs.h"
#include "cbd_client.h"
%}

%include <stdint.i>
%include <std_string.i>
%include "curve_type.h"
%include "libcurvefs.h"
%include "cbd_client.h"
