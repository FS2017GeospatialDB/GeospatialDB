// Copyright 2006 Google Inc. All Rights Reserved.
// Author: Andrew Eland (andrewe@google.com)

%module s2
%include base.i

%{
#include <sstream>
#include <vector>
#include <cstdlib>
#include <algorithm>

#include "s2.h"
#include "s2cellid.h"
#include "s2region.h"
#include "s2cap.h"
#include "s2latlng.h"
#include "s2latlngrect.h"
#include "s2regioncoverer.h"
#include "s2cell.h"
#include "s2cellunion.h"
#include "s2loop.h"
#include "s2polygon.h"
#include "vector2.h"
%}

// The PACKED macro makes SWIG think that we're declaring a variable of type
// S2CellId named PACKED.  We don't need it so we clobber it with an empty body.
#define PACKED

// The DECLARE_POD macro makes SWIG think the specified type's defined twice.
// We don't need it, so we can just remove it.
#define DECLARE_POD(TypeName)

// Warning 510 is "friend function 'operator +' ignored.". We can't do anything
// about that.
#pragma SWIG nowarn=510

// If we don't ignore this, the wrapper ends up assigning to None
%ignore S2CellId::None;

#ifdef SWIGPYTHON

%inline %{
  static PyObject *FromS2CellId(const S2CellId &cell_id) {
    return SWIG_NewPointerObj(new S2CellId(cell_id), SWIGTYPE_p_S2CellId, 1);
  }
%}

%typemap(in, numinputs=0)
vector<S2CellId> *OUTPUT(vector<S2CellId> temp) {
  $1 = &temp;
}

%typemap(argout, fragment="t_output_helper")
vector<S2CellId> *OUTPUT {
  $result = t_output_helper($result, vector_output_helper($1, &FromS2CellId));
}

%apply vector<S2CellId> *OUTPUT {vector<S2CellId> *covering};
%apply vector<S2CellId> *OUTPUT {vector<S2CellId> *output};

%include "std_vector.i"
%template(S2PointVector) std::vector<S2Point>;
%template(VectorDouble) std::vector<double>;

template<class T1>
struct Vector2 {
  T1 x();
  T1 y();
  int Size();
};
%template(Vector2_double) Vector2<double>;
%template(Vector2_int) Vector2<int>;

template<class T1>
struct Vector3 {
  T1 x();
  T1 y();
  T1 z();
  int Size();
};
%template(Vector3_double) Vector3<double>;
%template(Vector3_int) Vector3<int>;

%typemap(in, numinputs=0)
S2CellId *OUTPUT(S2CellId temp[4]) {
  $1 = temp;
}

%typemap(argout, fragment="t_output_helper") 
S2CellId *OUTPUT {
  vector<S2CellId> holder($1, $1 + 4);
  $result = t_output_helper($result, vector_output_helper(&holder, &FromS2CellId));
}

%apply S2CellId *OUTPUT {S2CellId neighbors[4]}

#endif

%include "r1interval.h"
%include "s1angle.h"
%include "s1interval.h"
%include "s2cellid.h"
%include "s2region.h"
%include "s2cap.h"
%include "s2latlng.h"
%include "s2latlngrect.h"
%include "s2regioncoverer.h"
%include "s2cell.h"
%include "s2cellunion.h"
%include "s2loop.h"
%include "s2polygon.h"
%include "vector2.h"

%define USE_STREAM_INSERTOR_FOR_STR(type)
  %extend type {
    string __str__() {
      std::ostringstream output;
      output << *self << std::ends;
      return output.str();
    }
  }
%enddef

%define USE_EQUALS_FOR_EQ_AND_NE(type)
  %extend type {
    bool __eq__(const type& other) {
      return *self == other;
    }

    bool __ne__(const type& other) {
      return *self != other;
    }
  }
%enddef

%define USE_COMPARISON_FOR_LT_AND_GT(type)
  %extend type {
    bool __lt__(const type& other) {
      return *self < other;
    }

    bool __gt__(const type& other) {
      return *self > other;
    }
  }
%enddef

%define USE_STD_HASH_FOR_HASH(type)
  %extend type {
    size_t __hash__() {
      return HASH_NAMESPACE::hash<type>()(*self);
    }
  }
%enddef

USE_STREAM_INSERTOR_FOR_STR(S1Angle)
USE_STREAM_INSERTOR_FOR_STR(S1Interval)
USE_STREAM_INSERTOR_FOR_STR(S2CellId)
USE_STREAM_INSERTOR_FOR_STR(S2Cap)
USE_STREAM_INSERTOR_FOR_STR(S2LatLng)
USE_STREAM_INSERTOR_FOR_STR(S2LatLngRect)

USE_EQUALS_FOR_EQ_AND_NE(S2CellId)
USE_COMPARISON_FOR_LT_AND_GT(S2CellId)
// FIXME: Throws compiler errors when enabled.
//USE_STD_HASH_FOR_HASH(S2CellId)

