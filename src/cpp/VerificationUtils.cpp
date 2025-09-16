// VerificationUtils.cpp
#include "VerificationUtils.hpp"
#include <cmath>

bool is_missing(double v) { return v < -998.0; }

double get_surface_value(const SurfaceStation& s, const std::string& var) {
    if (var == "NN") return s.nn;
    else if (var == "DD") return s.dd;
    else if (var == "FF") return s.ff;
    else if (var == "TT") return s.tt;
    else if (var == "TTHA") return s.ttha;
    else if (var == "RH") return s.rh;
    else if (var == "PS") return s.ps;
    else if (var == "SPS" || var == "PSS") return s.pss;   // station pressure
    else if (var == "PE") return s.pe;          // total (forecast only)
    else if (var == "PE1") return s.pe1;
    else if (var == "PE3") return s.pe3;
    else if (var == "PE6") return s.pe6;
    else if (var == "PE12") return s.pe12;
    else if (var == "PE24") return s.pe24;
    else if (var == "QQ") return s.qq;
    else if (var == "VI") return s.vi;
    else if (var == "TD") return s.td;
    else if (var == "TX") return s.tx;
    else if (var == "TN") return s.tn;
    else if (var == "GG") return s.gg;
    else if (var == "GX") return s.gx;
    else if (var == "FX") return s.fx;
    else if (var == "CH") return s.ch;
    else if (var == "LC") return s.lc;
    return -999.0;
}

double get_temp_value(const TempLevel& t, const std::string& var) {
    if (var == "TT") return t.temp;
    else if (var == "TD") return t.td;
    else if (var == "RH") return t.rh;
    else if (var == "QQ") return t.qq;
    else if (var == "DD") return t.dd;
    else if (var == "FF") return t.ff;
    else if (var == "FI") return t.fi;
    return -999.0;
}

double directional_diff(double f, double o) {
    if (is_missing(f) || is_missing(o)) return -999.0;
    double d = f - o;
    while (d > 180.0) d -= 360.0;
    while (d < -180.0) d += 360.0;
    return d;
}
